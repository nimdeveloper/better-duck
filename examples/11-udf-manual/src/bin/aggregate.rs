//! Aggregate UDFs the low-level way: implement `VAggregate` and register with
//! `conn.register_aggregate_function::<T>(name)`, or group several overloads
//! under one SQL name with `register_aggregate_function_set`.
//!
//! An aggregate folds many rows into one value per group via four callbacks:
//! `init` (fresh accumulator), `update` (fold a row), `combine` (merge partial
//! states for parallel/grouped aggregation), and `finalize` (emit the result).

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::types::LogicalType;
use better_duck_core::udf::{DataChunkHandle, UdfResult, VAggregate, VectorMut};
use common::{section, show, Result};

/// `int_sum(INTEGER) -> BIGINT`: a running sum over INTEGER inputs.
struct IntSum;

impl VAggregate for IntSum {
    type State = i64;
    type Shared = ();

    fn parameters() -> Result<Vec<LogicalType>> {
        Ok(vec![LogicalType::of::<i32>()?])
    }

    fn return_type() -> Result<LogicalType> {
        LogicalType::of::<i64>()
    }

    fn init() -> i64 {
        0
    }

    fn update(
        _shared: &(),
        state: &mut i64,
        input: &DataChunkHandle,
        row: usize,
    ) -> UdfResult<()> {
        let v: i32 = input.vector(0)?.get(row)?;
        *state += i64::from(v);
        Ok(())
    }

    fn combine(
        _shared: &(),
        source: &i64,
        target: &mut i64,
    ) {
        *target += *source;
    }

    fn finalize(
        _shared: &(),
        state: &i64,
        output: &mut VectorMut<'_>,
        row: usize,
    ) -> UdfResult<()> {
        output.set(row, *state)?;
        Ok(())
    }
}

/// A second overload over BIGINT inputs, so a function set can dispatch on the
/// argument type.
struct BigIntSum;

impl VAggregate for BigIntSum {
    type State = i64;
    type Shared = ();

    fn parameters() -> Result<Vec<LogicalType>> {
        Ok(vec![LogicalType::of::<i64>()?])
    }

    fn return_type() -> Result<LogicalType> {
        LogicalType::of::<i64>()
    }

    fn init() -> i64 {
        0
    }

    fn update(
        _shared: &(),
        state: &mut i64,
        input: &DataChunkHandle,
        row: usize,
    ) -> UdfResult<()> {
        let v: i64 = input.vector(0)?.get(row)?;
        *state += v;
        Ok(())
    }

    fn combine(
        _shared: &(),
        source: &i64,
        target: &mut i64,
    ) {
        *target += *source;
    }

    fn finalize(
        _shared: &(),
        state: &i64,
        output: &mut VectorMut<'_>,
        row: usize,
    ) -> UdfResult<()> {
        output.set(row, *state)?;
        Ok(())
    }
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.register_aggregate_function::<IntSum>("int_sum")?;

    conn.execute_batch("CREATE TABLE t (k INTEGER, v INTEGER)")?;
    conn.execute_batch("INSERT INTO t VALUES (1,10),(1,20),(2,5),(2,7),(2,100)")?;

    section("int_sum(v): grand total (also exercises finalize)");
    let mut r = conn.execute("SELECT int_sum(v) AS s FROM t")?;
    let row = r.next().expect("one row")?;
    show("int_sum(v)", row.get("s"));
    assert_eq!(row.get("s"), Some(&DuckValue::BigInt(142)));

    section("int_sum(v) GROUP BY k: exercises combine across partial states");
    let result = conn.execute("SELECT k, int_sum(v) AS s FROM t GROUP BY k ORDER BY k")?;
    let rows: Vec<_> = result.collect::<Result<_>>()?;
    for row in &rows {
        println!("  k={:?} -> {:?}", row.get("k"), row.get("s"));
    }
    assert_eq!(rows[0].get("s"), Some(&DuckValue::BigInt(30)));
    assert_eq!(rows[1].get("s"), Some(&DuckValue::BigInt(112)));

    section("register_aggregate_function_set: one name, two overloads");
    conn.register_aggregate_function_set("my_sum", |b| {
        b.add::<IntSum>()?;
        b.add::<BigIntSum>()?;
        Ok(())
    })?;

    let mut r_int =
        conn.execute("SELECT my_sum(CAST(v AS INTEGER)) AS s FROM (VALUES (1),(2),(3)) t(v)")?;
    let int_row = r_int.next().expect("one row")?;
    show("my_sum(INTEGER)", int_row.get("s"));
    assert_eq!(int_row.get("s"), Some(&DuckValue::BigInt(6)));

    let mut r_big =
        conn.execute("SELECT my_sum(CAST(v AS BIGINT)) AS s FROM (VALUES (10),(20)) t(v)")?;
    let big_row = r_big.next().expect("one row")?;
    show("my_sum(BIGINT)", big_row.get("s"));
    assert_eq!(big_row.get("s"), Some(&DuckValue::BigInt(30)));

    Ok(())
}
