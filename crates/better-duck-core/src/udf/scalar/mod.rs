//! DuckDB scalar functions: row-wise functions used in a `SELECT` list or
//! `WHERE` clause, e.g. `SELECT my_func(x) FROM t`.

mod function;

use std::ffi::CString;

use crate::{
    connection::Connection,
    error::Result,
    ffi::{duckdb_bind_info, duckdb_data_chunk, duckdb_function_info, duckdb_vector},
};

use self::function::{ScalarFunction, ScalarFunctionInfo, ScalarFunctionSet};
use super::{callback::contain_callback, data_chunk::DataChunkHandle, vector::VectorMut};
use crate::types::LogicalType;

pub use self::function::ScalarBindInfo;

/// A DuckDB scalar function: computes one value per row.
///
/// See the callback containment contract in [`crate::udf`].
pub trait VScalar: Sized {
    /// State set at registration time, shared across every invocation and every
    /// worker thread. Persists for the lifetime of the catalog entry, so it must
    /// be `'static`; any interior mutation must be synchronized.
    type State: Send + Sync + 'static;

    /// Per-query data produced by [`VScalar::bind`] and shared, read-only, by every
    /// [`VScalar::invoke`] call for that query. Use `()` if the function needs none.
    type BindData: Send + Sync + 'static;

    /// The possible signatures of this function. Each becomes a DuckDB overload;
    /// [`VScalar::invoke`] must be able to handle every one of them.
    ///
    /// # Errors
    ///
    /// Returns an error if a signature's logical type cannot be built.
    fn signatures() -> Result<Vec<ScalarSignature>>;

    /// Runs once per query that references this function, before any [`invoke`](VScalar::invoke).
    /// Inspects the call's argument expressions (via [`ScalarBindInfo`] — e.g. to
    /// fold a constant argument or reject an unsupported one) and produces the
    /// per-query [`BindData`](VScalar::BindData). Functions needing no bind data
    /// return `Ok(())` (with `type BindData = ()`).
    ///
    /// # Errors
    ///
    /// Returns an error to reject the query at bind time with that message.
    fn bind(bind: &ScalarBindInfo) -> super::UdfResult<Self::BindData>;

    /// Computes `output[row]` for every `row` in `0..input.len()`.
    ///
    /// DuckDB guarantees `input` and `output` stay live for the duration of this
    /// call, and that `output`'s capacity is at least `input.len()`. `bind_data` is
    /// the value [`bind`](VScalar::bind) produced for this query.
    ///
    /// # Errors
    ///
    /// Returns an error to fail the query with that message.
    fn invoke(
        state: &Self::State,
        bind_data: &Self::BindData,
        input: &DataChunkHandle,
        output: &mut VectorMut<'_>,
    ) -> super::UdfResult<()>;

    /// Whether this function is volatile — re-evaluated for every row even with
    /// no parameters, rather than optimized to a constant. Needed for functions
    /// like random-number or UUID generators.
    fn volatile() -> bool {
        false
    }

    /// Whether this function should be invoked for rows containing `NULL`
    /// parameters. By default DuckDB substitutes `NULL` as the result for any
    /// row with a `NULL` argument without calling [`VScalar::invoke`] at all;
    /// returning `true` here disables that shortcut.
    fn special_handling() -> bool {
        false
    }
}

/// The parameter shape of one [`ScalarSignature`].
enum ScalarParams {
    /// A fixed list of parameter types.
    Exact(Vec<LogicalType>),
    /// Any number of arguments of a single type.
    Variadic(LogicalType),
}

/// One overload of a scalar function: a parameter shape and a return type.
pub struct ScalarSignature {
    parameters: Option<ScalarParams>,
    return_type: LogicalType,
}

impl ScalarSignature {
    /// A signature with a fixed list of parameter types.
    pub fn exact(
        parameters: Vec<LogicalType>,
        return_type: LogicalType,
    ) -> Self {
        Self { parameters: Some(ScalarParams::Exact(parameters)), return_type }
    }

    /// A signature accepting any number of arguments of `parameter`'s type.
    pub fn variadic(
        parameter: LogicalType,
        return_type: LogicalType,
    ) -> Self {
        Self { parameters: Some(ScalarParams::Variadic(parameter)), return_type }
    }

    fn apply(
        &self,
        f: &ScalarFunction,
    ) {
        f.set_return_type(&self.return_type);
        match &self.parameters {
            Some(ScalarParams::Exact(params)) => {
                for p in params {
                    f.add_parameter(p);
                }
            },
            Some(ScalarParams::Variadic(p)) => f.set_varargs(p),
            None => {},
        }
    }
}

/// The C trampoline installed via `duckdb_scalar_function_set_function`.
///
/// Nothing above the `contain_callback` call may panic or carry a `Drop` impl:
/// since Rust 1.81 a panic escaping a plain `extern "C"` frame aborts the
/// process regardless of panic strategy, so containment must be the outermost
/// thing here. See [`crate::udf`] for the full `panic = "abort"` caveat.
unsafe extern "C" fn scalar_trampoline<S: VScalar>(
    info: duckdb_function_info,
    input: duckdb_data_chunk,
    output: duckdb_vector,
) {
    let sink = ScalarFunctionInfo::from(info);
    contain_callback(&sink, || {
        // SAFETY: DuckDB owns `input` and guarantees it stays live and unmutated
        // by other code for the duration of this call.
        let chunk = unsafe { DataChunkHandle::borrowed(input) };
        // SAFETY: DuckDB owns `output`, guarantees it stays live for the
        // duration of this call, and that no other code writes through it
        // concurrently.
        let mut out = unsafe { VectorMut::new(output) };
        // SAFETY: `register_scalar_function`/`register_scalar_function_with_state`
        // always call `ScalarFunction::set_extra_info::<S::State>`, so the state
        // stored for this catalog entry always has type `S::State`.
        let state = unsafe { sink.state::<S::State>() };
        // SAFETY: `scalar_bind_trampoline::<S>` runs before any invoke and stores a
        // `Box<S::BindData>` via `set_bind_data`, so the bind data has type
        // `S::BindData` and is live for every invocation of this query.
        let bind_data = unsafe { sink.bind_data::<S::BindData>() };
        S::invoke(state, bind_data, &chunk, &mut out)
    });
}

/// The C trampoline installed via `duckdb_scalar_function_set_bind`. Runs `S::bind`
/// and stores its result as the query's bind data. See [`scalar_trampoline`] for why
/// containment is the outermost thing here.
unsafe extern "C" fn scalar_bind_trampoline<S: VScalar>(info: duckdb_bind_info) {
    let bind = ScalarBindInfo::from(info);
    contain_callback(&bind, || {
        let data = S::bind(&bind)?;
        bind.set_bind_data(data);
        Ok(())
    });
}

impl Connection {
    /// Registers `S` as a scalar function named `name`, using `S::State`'s
    /// default value as the shared state for every overload.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` contains a NUL byte, a signature's logical
    /// type cannot be built, or DuckDB rejects the registration (e.g. a name
    /// conflict).
    pub fn register_scalar_function<S: VScalar>(
        &mut self,
        name: &str,
    ) -> Result<()>
    where
        S::State: Default,
    {
        // A fresh default per overload, rather than one shared value cloned —
        // avoids requiring `S::State: Clone` in addition to `Default`.
        register_scalar_function_impl::<S>(self, name, S::State::default)
    }

    /// Registers `S` as a scalar function named `name`, with explicit shared
    /// state. The state is cloned once per overload.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` contains a NUL byte, a signature's logical
    /// type cannot be built, or DuckDB rejects the registration (e.g. a name
    /// conflict).
    pub fn register_scalar_function_with_state<S: VScalar>(
        &mut self,
        name: &str,
        state: S::State,
    ) -> Result<()>
    where
        S::State: Clone,
    {
        register_scalar_function_impl::<S>(self, name, move || state.clone())
    }
}

fn register_scalar_function_impl<S: VScalar>(
    conn: &mut Connection,
    name: &str,
    mut make_state: impl FnMut() -> S::State,
) -> Result<()> {
    let c_name = CString::new(name)?;
    let set = ScalarFunctionSet::new(&c_name);
    for signature in S::signatures()? {
        let f = ScalarFunction::new(&c_name);
        signature.apply(&f);
        f.set_function(Some(scalar_trampoline::<S>));
        f.set_bind(Some(scalar_bind_trampoline::<S>));
        if S::volatile() {
            f.set_volatile();
        }
        if S::special_handling() {
            f.set_special_handling();
        }
        f.set_extra_info(make_state());
        set.add_function(&f)?;
    }
    set.register(conn.raw_con(), name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Connection;

    /// A hand-written `VScalar`, exercising the trait directly (not through the
    /// `#[duckdb_scalar]` macro, which lands in a later phase).
    struct AddOne;

    impl VScalar for AddOne {
        type State = ();
        type BindData = ();

        fn signatures() -> Result<Vec<ScalarSignature>> {
            Ok(vec![ScalarSignature::exact(
                vec![LogicalType::of::<i32>()?],
                LogicalType::of::<i32>()?,
            )])
        }

        fn bind(_bind: &ScalarBindInfo) -> super::super::UdfResult<()> {
            Ok(())
        }

        fn invoke(
            _state: &(),
            _bind_data: &(),
            input: &DataChunkHandle,
            output: &mut VectorMut<'_>,
        ) -> super::super::UdfResult<()> {
            let col = input.vector(0)?;
            for row in 0..input.len() {
                let v: i32 = col.get(row)?;
                output.set(row, v + 1)?;
            }
            Ok(())
        }
    }

    #[test]
    fn register_and_call_scalar_function() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.register_scalar_function::<AddOne>("add_one").unwrap();
        conn.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1), (2), (41)").unwrap();
        let result = conn.execute("SELECT add_one(v) AS r FROM t ORDER BY v").unwrap();
        let rows: Vec<_> = result.collect::<Result<_>>().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2].get("r"), Some(&crate::types::value::DuckValue::Int(42)));
    }

    /// A `VScalar` whose `invoke` always errors, verifying the error surfaces as
    /// a normal query error and the connection stays usable afterward.
    struct AlwaysFails;

    impl VScalar for AlwaysFails {
        type State = ();
        type BindData = ();

        fn signatures() -> Result<Vec<ScalarSignature>> {
            Ok(vec![ScalarSignature::exact(
                vec![LogicalType::of::<i32>()?],
                LogicalType::of::<i32>()?,
            )])
        }

        fn bind(_bind: &ScalarBindInfo) -> super::super::UdfResult<()> {
            Ok(())
        }

        fn invoke(
            _state: &(),
            _bind_data: &(),
            _input: &DataChunkHandle,
            _output: &mut VectorMut<'_>,
        ) -> super::super::UdfResult<()> {
            Err("deliberate failure".into())
        }
    }

    #[test]
    fn invoke_error_surfaces_as_query_error_and_connection_stays_usable() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.register_scalar_function::<AlwaysFails>("always_fails").unwrap();
        conn.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1)").unwrap();
        let err = match conn.execute("SELECT always_fails(v) FROM t") {
            Ok(_) => panic!("expected an error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("deliberate failure"), "{err}");
        // The connection must still be usable after a UDF error.
        conn.execute_batch("INSERT INTO t VALUES (2)").unwrap();
    }

    /// A panicking `VScalar`, verifying panic containment.
    struct AlwaysPanics;

    impl VScalar for AlwaysPanics {
        type State = ();
        type BindData = ();

        fn signatures() -> Result<Vec<ScalarSignature>> {
            Ok(vec![ScalarSignature::exact(
                vec![LogicalType::of::<i32>()?],
                LogicalType::of::<i32>()?,
            )])
        }

        fn bind(_bind: &ScalarBindInfo) -> super::super::UdfResult<()> {
            Ok(())
        }

        fn invoke(
            _state: &(),
            _bind_data: &(),
            _input: &DataChunkHandle,
            _output: &mut VectorMut<'_>,
        ) -> super::super::UdfResult<()> {
            panic!("deliberate panic")
        }
    }

    #[test]
    #[cfg(panic = "unwind")]
    fn invoke_panic_is_contained_and_connection_stays_usable() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.register_scalar_function::<AlwaysPanics>("always_panics").unwrap();
        conn.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1)").unwrap();
        let err = match conn.execute("SELECT always_panics(v) FROM t") {
            Ok(_) => panic!("expected an error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("deliberate panic"), "{err}");
        conn.execute_batch("INSERT INTO t VALUES (2)").unwrap();
    }

    /// `add_const(x, c)`: the second argument must be a *constant*. `bind` inspects
    /// the argument expressions, folds the constant to an `i64`, and stores it as
    /// bind data; `invoke` adds it to every row. This exercises the whole bind/fold
    /// path: set_bind, argument_count/argument, Expression::is_foldable/fold (via the
    /// bind client context), set_bind_data, and get_bind_data.
    struct AddConst;

    impl VScalar for AddConst {
        type State = ();
        type BindData = i64;

        fn signatures() -> Result<Vec<ScalarSignature>> {
            Ok(vec![ScalarSignature::exact(
                vec![LogicalType::of::<i32>()?, LogicalType::of::<i32>()?],
                LogicalType::of::<i32>()?,
            )])
        }

        fn bind(bind: &ScalarBindInfo) -> super::super::UdfResult<i64> {
            // Registration state is readable at bind time too (here it is `()`).
            // SAFETY: this function is registered with `State = ()`, so the stored
            // extra-info has type `()`.
            let _state: &() = unsafe { bind.extra_info::<()>() };
            assert_eq!(bind.argument_count(), 2, "add_const has two arguments");
            let arg = bind.argument(1).ok_or("add_const needs a second argument")?;
            if !arg.is_foldable() {
                return Err("add_const's second argument must be a constant".into());
            }
            let ctx = bind.client_context().ok_or("no client context for folding")?;
            let folded = arg.fold(&ctx).map_err(|e| format!("fold failed: {e:?}"))?;
            match folded {
                crate::types::value::DuckValue::Int(n) => Ok(i64::from(n)),
                crate::types::value::DuckValue::BigInt(n) => Ok(n),
                other => Err(format!("expected an integer constant, got {other:?}").into()),
            }
        }

        fn invoke(
            _state: &(),
            bind_data: &i64,
            input: &DataChunkHandle,
            output: &mut VectorMut<'_>,
        ) -> super::super::UdfResult<()> {
            let col = input.vector(0)?;
            let c = i32::try_from(*bind_data).map_err(|_| "constant out of range")?;
            for row in 0..input.len() {
                let v: i32 = col.get(row)?;
                output.set(row, v + c)?;
            }
            Ok(())
        }
    }

    #[test]
    fn bind_folds_a_constant_argument_and_invoke_uses_it() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.register_scalar_function::<AddConst>("add_const").unwrap();
        conn.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1), (2), (40)").unwrap();
        // The second argument (10) is folded at bind time and added to every row.
        let rows: Vec<_> = conn
            .execute("SELECT add_const(v, 2 + 8) AS r FROM t ORDER BY v")
            .unwrap()
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("r"), Some(&crate::types::value::DuckValue::Int(11)));
        assert_eq!(rows[2].get("r"), Some(&crate::types::value::DuckValue::Int(50)));
    }

    #[test]
    fn bind_rejects_a_non_constant_argument() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.register_scalar_function::<AddConst>("add_const").unwrap();
        conn.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1)").unwrap();
        // The second argument references a column, so it is not foldable — bind
        // rejects the query with our error message, and the connection stays usable.
        let err = match conn.execute("SELECT add_const(v, v) FROM t") {
            Ok(_) => panic!("expected a bind error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("must be a constant"), "{err}");
        conn.execute_batch("INSERT INTO t VALUES (2)").unwrap();
    }
}
