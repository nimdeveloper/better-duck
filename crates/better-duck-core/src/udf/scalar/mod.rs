//! DuckDB scalar functions: row-wise functions used in a `SELECT` list or
//! `WHERE` clause, e.g. `SELECT my_func(x) FROM t`.

mod function;

use std::ffi::CString;

use crate::{
    connection::Connection,
    error::Result,
    ffi::{duckdb_data_chunk, duckdb_function_info, duckdb_vector},
};

use self::function::{ScalarFunction, ScalarFunctionInfo, ScalarFunctionSet};
use super::{
    callback::contain_callback, data_chunk::DataChunkHandle, logical_type::LogicalType,
    vector::VectorMut,
};

/// A DuckDB scalar function: computes one value per row.
///
/// See the callback containment contract in [`crate::udf`].
pub trait VScalar: Sized {
    /// State set at registration time, shared across every invocation and every
    /// worker thread. Persists for the lifetime of the catalog entry, so it must
    /// be `'static`; any interior mutation must be synchronized.
    type State: Send + Sync + 'static;

    /// The possible signatures of this function. Each becomes a DuckDB overload;
    /// [`VScalar::invoke`] must be able to handle every one of them.
    ///
    /// # Errors
    ///
    /// Returns an error if a signature's logical type cannot be built.
    fn signatures() -> Result<Vec<ScalarSignature>>;

    /// Computes `output[row]` for every `row` in `0..input.len()`.
    ///
    /// DuckDB guarantees `input` and `output` stay live for the duration of this
    /// call, and that `output`'s capacity is at least `input.len()`.
    ///
    /// # Errors
    ///
    /// Returns an error to fail the query with that message.
    fn invoke(
        state: &Self::State,
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
        S::invoke(state, &chunk, &mut out)
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

        fn signatures() -> Result<Vec<ScalarSignature>> {
            Ok(vec![ScalarSignature::exact(
                vec![LogicalType::of::<i32>()?],
                LogicalType::of::<i32>()?,
            )])
        }

        fn invoke(
            _state: &(),
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

        fn signatures() -> Result<Vec<ScalarSignature>> {
            Ok(vec![ScalarSignature::exact(
                vec![LogicalType::of::<i32>()?],
                LogicalType::of::<i32>()?,
            )])
        }

        fn invoke(
            _state: &(),
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

        fn signatures() -> Result<Vec<ScalarSignature>> {
            Ok(vec![ScalarSignature::exact(
                vec![LogicalType::of::<i32>()?],
                LogicalType::of::<i32>()?,
            )])
        }

        fn invoke(
            _state: &(),
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
}
