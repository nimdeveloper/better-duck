//! DuckDB table functions: functions used in a `FROM` clause that produce rows
//! and columns, e.g. `SELECT * FROM my_func(1, 2)`.

mod function;
mod row;

use std::ffi::CString;

use crate::{
    connection::Connection,
    error::Result,
    ffi::{duckdb_bind_info, duckdb_data_chunk, duckdb_function_info, duckdb_init_info},
};

use self::function::TableFunction;
pub use self::function::{BindInfo, InitInfo, TableFunctionInfo};
pub use self::row::{run_table_func, TableInitData, TableRow};
use super::{callback::contain_callback, data_chunk::DataChunkHandle, logical_type::LogicalType};

/// A DuckDB table function: produces rows and columns for use in a `FROM`
/// clause, e.g. `SELECT * FROM my_func(1, 2)`.
///
/// See the callback containment contract in [`crate::udf`].
pub trait VTab: Sized {
    /// Data produced once by [`VTab::bind`] and shared, read-only, by every
    /// later call to [`VTab::init`] and [`VTab::func`] for this query.
    type BindData: Send + Sync;

    /// Data produced once by [`VTab::init`], shared across every worker thread
    /// executing this query. Any interior mutation must be synchronized.
    type InitData: Send + Sync;

    /// The positional parameter types this function accepts, in order. Empty by
    /// default (no positional parameters).
    ///
    /// # Errors
    ///
    /// Returns an error if a parameter's logical type cannot be built.
    fn parameters() -> Result<Vec<LogicalType>> {
        Ok(Vec::new())
    }

    /// The named (keyword) parameters this function accepts, as
    /// `(name, type)` pairs. Empty by default (no named parameters). Read back
    /// during [`VTab::bind`] via [`BindInfo::get_named_parameter`].
    ///
    /// # Errors
    ///
    /// Returns an error if a parameter's logical type cannot be built.
    fn named_parameters() -> Result<Vec<(String, LogicalType)>> {
        Ok(Vec::new())
    }

    /// Whether this function can consume a projected column list — i.e. honors
    /// [`InitInfo::column_indices`] in [`VTab::init`]/[`VTab::func`] to skip
    /// producing columns the query doesn't need. `false` by default (every
    /// column is always written).
    fn supports_projection_pushdown() -> bool {
        false
    }

    /// Determines the function's output schema (via
    /// [`BindInfo::add_result_column`]) and produces the data shared by every
    /// later call for this query.
    ///
    /// # Errors
    ///
    /// Returns an error to fail the query with that message.
    fn bind(bind: &BindInfo) -> super::UdfResult<Self::BindData>;

    /// Produces data shared across every worker thread executing this query,
    /// e.g. the initial position of a cursor.
    ///
    /// # Errors
    ///
    /// Returns an error to fail the query with that message.
    fn init(init: &InitInfo<Self>) -> super::UdfResult<Self::InitData>;

    /// Writes up to `output.capacity()` rows into `output`, then calls
    /// `output.set_len(k)` with the number actually written. Called repeatedly
    /// until `set_len(0)` (or `output.len() == 0` on entry, if untouched) signals
    /// the scan is complete.
    ///
    /// # Errors
    ///
    /// Returns an error to fail the query with that message.
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> super::UdfResult<()>;
}

/// A [`VTab`] that also produces per-worker-thread ("local") init data, in
/// addition to the query-wide init data every `VTab` already produces.
///
/// Registered via [`Connection::register_table_function_ext`] instead of
/// [`Connection::register_table_function`]. Useful for state that must not be
/// shared across threads (e.g. a non-`Sync` cursor or scratch buffer) when
/// [`InitInfo::set_max_threads`] allows more than one worker.
pub trait VTabLocalInit: VTab {
    /// Data produced once per worker thread, retrievable on that same thread
    /// via [`TableFunctionInfo::local_init_data`].
    type LocalInitData: Send + Sync;

    /// Produces this thread's local init data.
    ///
    /// # Errors
    ///
    /// Returns an error to fail the query with that message.
    fn local_init(init: &InitInfo<Self>) -> super::UdfResult<Self::LocalInitData>;
}

/// The C trampoline installed via `duckdb_table_function_set_bind`.
///
/// See [`scalar_trampoline`](super::scalar) for why containment must be the
/// outermost thing here.
unsafe extern "C" fn table_bind_trampoline<T: VTab>(info: duckdb_bind_info) {
    let bind = BindInfo::from(info);
    contain_callback(&bind, || {
        let data = T::bind(&bind)?;
        bind.set_bind_data(data);
        Ok(())
    });
}

/// The C trampoline installed via `duckdb_table_function_set_init`.
unsafe extern "C" fn table_init_trampoline<T: VTab>(info: duckdb_init_info) {
    let init = InitInfo::<T>::from(info);
    contain_callback(&init, || {
        let data = T::init(&init)?;
        init.set_init_data(data);
        Ok(())
    });
}

/// The C trampoline installed via `duckdb_table_function_set_local_init`.
///
/// DuckDB calls this once per worker thread that executes the query, using
/// the same `duckdb_init_info` shape as the global init callback above — the
/// data it sets is stored as *this thread's* local init data instead, purely
/// by virtue of which callback DuckDB is currently invoking.
unsafe extern "C" fn table_local_init_trampoline<T: VTabLocalInit>(info: duckdb_init_info) {
    let init = InitInfo::<T>::from(info);
    contain_callback(&init, || {
        let data = T::local_init(&init)?;
        init.set_local_init_data(data);
        Ok(())
    });
}

/// The C trampoline installed via `duckdb_table_function_set_function`.
unsafe extern "C" fn table_func_trampoline<T: VTab>(
    info: duckdb_function_info,
    output: duckdb_data_chunk,
) {
    let func = TableFunctionInfo::<T>::from(info);
    contain_callback(&func, || {
        // SAFETY: DuckDB owns `output` and guarantees it stays live and
        // unmutated by other code for the duration of this call.
        let mut chunk = unsafe { DataChunkHandle::borrowed(output) };
        T::func(&func, &mut chunk)
    });
}

impl Connection {
    /// Builds and configures a [`TableFunction`] for `T`: positional and named
    /// parameters, the projection-pushdown flag, and the bind/init/func
    /// trampolines. Shared by every `register_table_function*` entry point
    /// below; callers add anything extra (local init, extra info) before
    /// calling [`TableFunction::register`].
    fn build_table_function<T: VTab>(name: &str) -> Result<TableFunction> {
        let c_name = CString::new(name)?;
        let f = TableFunction::new(&c_name);
        for param in T::parameters()? {
            f.add_parameter(&param);
        }
        for (param_name, ty) in T::named_parameters()? {
            f.add_named_parameter(&param_name, &ty)?;
        }
        f.set_supports_projection_pushdown(T::supports_projection_pushdown());
        f.set_bind(Some(table_bind_trampoline::<T>));
        f.set_init(Some(table_init_trampoline::<T>));
        f.set_function(Some(table_func_trampoline::<T>));
        Ok(f)
    }

    /// Registers `T` as a table function named `name`.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` contains a NUL byte, a parameter's logical
    /// type cannot be built, or DuckDB rejects the registration (e.g. a name
    /// conflict).
    pub fn register_table_function<T: VTab>(
        &mut self,
        name: &str,
    ) -> Result<()> {
        let f = Self::build_table_function::<T>(name)?;
        f.register(self.raw_con(), name)
    }

    /// Registers `T` as a table function named `name`, additionally wiring its
    /// per-worker-thread [`VTabLocalInit::local_init`] callback.
    ///
    /// # Errors
    ///
    /// Same as [`register_table_function`](Self::register_table_function).
    pub fn register_table_function_ext<T: VTabLocalInit>(
        &mut self,
        name: &str,
    ) -> Result<()> {
        let f = Self::build_table_function::<T>(name)?;
        f.set_local_init(Some(table_local_init_trampoline::<T>));
        f.register(self.raw_con(), name)
    }

    /// Registers `T` as a table function named `name`, sharing `extra_info`
    /// read-only across every `bind`/`init`/`func` call for this function via
    /// [`BindInfo::extra_info`]/[`InitInfo::extra_info`]/
    /// [`TableFunctionInfo::extra_info`].
    ///
    /// # Errors
    ///
    /// Same as [`register_table_function`](Self::register_table_function).
    pub fn register_table_function_with_extra_info<T: VTab, E: Send + Sync + 'static>(
        &mut self,
        name: &str,
        extra_info: E,
    ) -> Result<()> {
        let f = Self::build_table_function::<T>(name)?;
        f.set_extra_info(extra_info);
        f.register(self.raw_con(), name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Connection;
    use crate::types::value::DuckValue;

    /// `series(start, stop)`: the integers in `[start, stop)`, one per row,
    /// exercising `bind` (result schema + cardinality), `init` (cursor state
    /// shared across the scan), and chunked `func` output.
    struct Series;

    struct SeriesBind {
        start: i64,
        stop: i64,
    }

    struct SeriesInit {
        next: std::sync::atomic::AtomicI64,
    }

    impl VTab for Series {
        type BindData = SeriesBind;
        type InitData = SeriesInit;

        fn parameters() -> Result<Vec<LogicalType>> {
            Ok(vec![LogicalType::of::<i64>()?, LogicalType::of::<i64>()?])
        }

        fn bind(bind: &BindInfo) -> super::super::UdfResult<Self::BindData> {
            bind.add_result_column("n", &LogicalType::of::<i64>()?)?;
            let start: i64 = bind.get_parameter(0)?;
            let stop: i64 = bind.get_parameter(1)?;
            bind.set_cardinality((stop - start).max(0) as u64, true);
            Ok(SeriesBind { start, stop })
        }

        fn init(init: &InitInfo<Self>) -> super::super::UdfResult<Self::InitData> {
            Ok(SeriesInit { next: std::sync::atomic::AtomicI64::new(init.bind_data().start) })
        }

        fn func(
            func: &TableFunctionInfo<Self>,
            output: &mut DataChunkHandle,
        ) -> super::super::UdfResult<()> {
            let stop = func.bind_data().stop;
            let init = func.init_data();
            let cap = output.capacity();
            let mut written = 0usize;
            {
                let mut col = output.vector_mut(0)?;
                while written < cap {
                    let n = init.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if n >= stop {
                        init.next.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                        break;
                    }
                    col.set(written, n)?;
                    written += 1;
                }
            }
            output.set_len(written)?;
            Ok(())
        }
    }

    #[test]
    fn register_and_scan_table_function() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.register_table_function::<Series>("series").unwrap();
        let result = conn.execute("SELECT n FROM series(1, 6) ORDER BY n").unwrap();
        let rows: Vec<_> = result.collect::<Result<_>>().unwrap();
        let got: Vec<i64> = rows
            .iter()
            .map(|r| match r.get("n").unwrap() {
                DuckValue::BigInt(n) => *n,
                other => panic!("expected BigInt, got {other:?}"),
            })
            .collect();
        assert_eq!(got, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn aggregate_over_table_function_matches_expected_sum() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.register_table_function::<Series>("series").unwrap();
        let mut result = conn.execute("SELECT sum(n) AS total FROM series(1, 101)").unwrap();
        let row = result.next().unwrap().unwrap();
        assert_eq!(row.get("total"), Some(&DuckValue::HugeInt(5050)));
    }

    /// A large enough range to force multiple chunks through `func`, proving the
    /// cursor state in `InitData` survives across repeated calls.
    #[test]
    fn scan_spanning_multiple_chunks() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.register_table_function::<Series>("series").unwrap();
        let result = conn.execute("SELECT count(*) AS n FROM series(0, 10000)").unwrap();
        let rows: Vec<_> = result.collect::<Result<_>>().unwrap();
        assert_eq!(rows[0].get("n"), Some(&DuckValue::BigInt(10000)));
    }

    /// A `VTab` whose `bind` always errors, verifying it surfaces as a normal
    /// query error and the connection stays usable afterward.
    struct AlwaysFailsBind;
    impl VTab for AlwaysFailsBind {
        type BindData = ();
        type InitData = ();

        fn bind(_bind: &BindInfo) -> super::super::UdfResult<Self::BindData> {
            Err("deliberate bind failure".into())
        }

        fn init(_init: &InitInfo<Self>) -> super::super::UdfResult<Self::InitData> {
            Ok(())
        }

        fn func(
            _func: &TableFunctionInfo<Self>,
            output: &mut DataChunkHandle,
        ) -> super::super::UdfResult<()> {
            output.set_len(0)?;
            Ok(())
        }
    }

    #[test]
    fn bind_error_surfaces_as_query_error_and_connection_stays_usable() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.register_table_function::<AlwaysFailsBind>("always_fails_bind").unwrap();
        let err = match conn.execute("SELECT * FROM always_fails_bind()") {
            Ok(_) => panic!("expected an error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("deliberate bind failure"), "{err}");
        conn.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
    }

    /// `doubler(stop)`: the even numbers `0, 2, 4, …` below `2 * stop`,
    /// exercising `VTabLocalInit` — each worker thread's local init data (a
    /// fixed multiplier here) is read back in `func` via
    /// `TableFunctionInfo::local_init_data`.
    struct Doubler;

    struct DoublerInit {
        next: std::sync::atomic::AtomicI64,
        stop: i64,
    }

    impl VTab for Doubler {
        type BindData = i64;
        type InitData = DoublerInit;

        fn parameters() -> Result<Vec<LogicalType>> {
            Ok(vec![LogicalType::of::<i64>()?])
        }

        fn bind(bind: &BindInfo) -> super::super::UdfResult<Self::BindData> {
            bind.add_result_column("n", &LogicalType::of::<i64>()?)?;
            let stop: i64 = bind.get_parameter(0)?;
            Ok(stop)
        }

        fn init(init: &InitInfo<Self>) -> super::super::UdfResult<Self::InitData> {
            let stop = *init.bind_data();
            Ok(DoublerInit { next: std::sync::atomic::AtomicI64::new(0), stop })
        }

        fn func(
            func: &TableFunctionInfo<Self>,
            output: &mut DataChunkHandle,
        ) -> super::super::UdfResult<()> {
            let init = func.init_data();
            let multiplier = func.local_init_data::<i64>().copied().expect("local init ran");
            let cap = output.capacity();
            let mut written = 0usize;
            {
                let mut col = output.vector_mut(0)?;
                while written < cap {
                    let n = init.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if n >= init.stop {
                        init.next.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                        break;
                    }
                    col.set(written, n * multiplier)?;
                    written += 1;
                }
            }
            output.set_len(written)?;
            Ok(())
        }
    }

    impl VTabLocalInit for Doubler {
        type LocalInitData = i64;

        fn local_init(_init: &InitInfo<Self>) -> super::super::UdfResult<Self::LocalInitData> {
            Ok(2)
        }
    }

    #[test]
    fn local_init_data_is_readable_from_func() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.register_table_function_ext::<Doubler>("doubler").unwrap();
        let result = conn.execute("SELECT n FROM doubler(5) ORDER BY n").unwrap();
        let rows: Vec<_> = result.collect::<Result<_>>().unwrap();
        let got: Vec<i64> = rows
            .iter()
            .map(|r| match r.get("n").unwrap() {
                DuckValue::BigInt(n) => *n,
                other => panic!("expected BigInt, got {other:?}"),
            })
            .collect();
        assert_eq!(got, vec![0, 2, 4, 6, 8]);
    }

    /// `with_context()`: a single row equal to the `i64` extra info shared
    /// across `bind`/`func` via `register_table_function_with_extra_info`.
    struct WithContext;

    impl VTab for WithContext {
        type BindData = ();
        type InitData = std::sync::atomic::AtomicBool;

        fn bind(bind: &BindInfo) -> super::super::UdfResult<Self::BindData> {
            bind.add_result_column("ctx", &LogicalType::of::<i64>()?)?;
            // Prove `extra_info` is reachable during `bind` too, not just `func`.
            assert!(bind.extra_info::<i64>().is_some());
            Ok(())
        }

        fn init(_init: &InitInfo<Self>) -> super::super::UdfResult<Self::InitData> {
            Ok(std::sync::atomic::AtomicBool::new(false))
        }

        fn func(
            func: &TableFunctionInfo<Self>,
            output: &mut DataChunkHandle,
        ) -> super::super::UdfResult<()> {
            let already_emitted = func.init_data().swap(true, std::sync::atomic::Ordering::Relaxed);
            if already_emitted {
                output.set_len(0)?;
                return Ok(());
            }
            let ctx = *func.extra_info::<i64>().expect("extra info was registered");
            output.vector_mut(0)?.set(0, ctx)?;
            output.set_len(1)?;
            Ok(())
        }
    }

    #[test]
    fn extra_info_is_shared_and_readable_from_bind_and_func() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.register_table_function_with_extra_info::<WithContext, i64>("with_context", 777)
            .unwrap();
        let mut result = conn.execute("SELECT ctx FROM with_context()").unwrap();
        let row = result.next().unwrap().unwrap();
        assert_eq!(row.get("ctx"), Some(&DuckValue::BigInt(777)));
    }
}
