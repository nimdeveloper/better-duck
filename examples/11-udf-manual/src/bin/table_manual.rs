//! Table UDFs the low-level way: implement `VTab` (and optionally `VTabLocalInit`)
//! and register with the `register_table_function*` family.
//!
//! `VTab` splits a table function into `bind` (declare the output schema and read
//! parameters), `init` (per-query shared state, e.g. a cursor), and `func`
//! (fill chunks of rows). This is the trait-level counterpart to the
//! `#[duckdb_table_function]` macro in `ex10-udf-macros`.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::types::LogicalType;
use better_duck_core::udf::{
    BindInfo, DataChunkHandle, InitInfo, TableFunctionInfo, UdfResult, VTab, VTabLocalInit,
};
use common::{section, show, Result};

/// `series_m(start, stop)`: the integers in `[start, stop)`.
struct Series;

struct SeriesBind {
    start: i64,
    stop: i64,
}

impl VTab for Series {
    type BindData = SeriesBind;
    type InitData = AtomicI64;

    fn parameters() -> Result<Vec<LogicalType>> {
        Ok(vec![LogicalType::of::<i64>()?, LogicalType::of::<i64>()?])
    }

    fn bind(bind: &BindInfo) -> UdfResult<Self::BindData> {
        bind.add_result_column("n", &LogicalType::of::<i64>()?)?;
        let start: i64 = bind.get_parameter(0)?;
        let stop: i64 = bind.get_parameter(1)?;
        bind.set_cardinality((stop - start).max(0) as u64, true);
        Ok(SeriesBind { start, stop })
    }

    fn init(init: &InitInfo<Self>) -> UdfResult<Self::InitData> {
        Ok(AtomicI64::new(init.bind_data().start))
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> UdfResult<()> {
        let stop = func.bind_data().stop;
        let next = func.init_data();
        let cap = output.capacity();
        let mut written = 0usize;
        {
            let mut col = output.vector_mut(0)?;
            while written < cap {
                let n = next.fetch_add(1, Ordering::Relaxed);
                if n >= stop {
                    next.fetch_sub(1, Ordering::Relaxed);
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

/// `doubler(stop)`: `0, 2, 4, …` below `2 * stop`. Its per-worker-thread
/// multiplier comes from `VTabLocalInit::local_init` and is read back in `func`.
struct Doubler;

struct DoublerInit {
    next: AtomicI64,
    stop: i64,
}

impl VTab for Doubler {
    type BindData = i64;
    type InitData = DoublerInit;

    fn parameters() -> Result<Vec<LogicalType>> {
        Ok(vec![LogicalType::of::<i64>()?])
    }

    fn bind(bind: &BindInfo) -> UdfResult<Self::BindData> {
        bind.add_result_column("n", &LogicalType::of::<i64>()?)?;
        let stop: i64 = bind.get_parameter(0)?;
        Ok(stop)
    }

    fn init(init: &InitInfo<Self>) -> UdfResult<Self::InitData> {
        Ok(DoublerInit { next: AtomicI64::new(0), stop: *init.bind_data() })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> UdfResult<()> {
        let init = func.init_data();
        let multiplier = func.local_init_data::<i64>().copied().expect("local init ran");
        let cap = output.capacity();
        let mut written = 0usize;
        {
            let mut col = output.vector_mut(0)?;
            while written < cap {
                let n = init.next.fetch_add(1, Ordering::Relaxed);
                if n >= init.stop {
                    init.next.fetch_sub(1, Ordering::Relaxed);
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

    fn local_init(_init: &InitInfo<Self>) -> UdfResult<Self::LocalInitData> {
        Ok(2)
    }
}

/// `with_context()`: a single row equal to the `i64` extra info shared across
/// `bind`/`func` via `register_table_function_with_extra_info`.
struct WithContext;

impl VTab for WithContext {
    type BindData = ();
    type InitData = AtomicBool;

    fn bind(bind: &BindInfo) -> UdfResult<Self::BindData> {
        bind.add_result_column("ctx", &LogicalType::of::<i64>()?)?;
        assert!(bind.extra_info::<i64>().is_some());
        Ok(())
    }

    fn init(_init: &InitInfo<Self>) -> UdfResult<Self::InitData> {
        Ok(AtomicBool::new(false))
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> UdfResult<()> {
        if func.init_data().swap(true, Ordering::Relaxed) {
            output.set_len(0)?;
            return Ok(());
        }
        let ctx = *func.extra_info::<i64>().expect("extra info was registered");
        output.vector_mut(0)?.set(0, ctx)?;
        output.set_len(1)?;
        Ok(())
    }
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.register_table_function::<Series>("series_m")?;
    conn.register_table_function_ext::<Doubler>("doubler")?;
    conn.register_table_function_with_extra_info::<WithContext, i64>("with_context", 777)?;

    section("register_table_function: sum(n) over series_m(1, 101)");
    let mut r = conn.execute("SELECT sum(n) AS total FROM series_m(1, 101)")?;
    let row = r.next().expect("one row")?;
    show("total", row.get("total"));
    assert_eq!(row.get("total"), Some(&DuckValue::HugeInt(5050)));

    section("register_table_function_ext: local init supplies a multiplier");
    let result = conn.execute("SELECT n FROM doubler(5) ORDER BY n")?;
    let rows: Vec<_> = result.collect::<Result<_>>()?;
    let got: Vec<&DuckValue> = rows.iter().filter_map(|row| row.get("n")).collect();
    show("doubler(5)", &got);
    assert_eq!(
        got,
        vec![
            &DuckValue::BigInt(0),
            &DuckValue::BigInt(2),
            &DuckValue::BigInt(4),
            &DuckValue::BigInt(6),
            &DuckValue::BigInt(8),
        ]
    );

    section("register_table_function_with_extra_info: shared registration value");
    let mut r = conn.execute("SELECT ctx FROM with_context()")?;
    let row = r.next().expect("one row")?;
    show("with_context() -> ctx", row.get("ctx"));
    assert_eq!(row.get("ctx"), Some(&DuckValue::BigInt(777)));

    Ok(())
}
