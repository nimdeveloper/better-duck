//! Scalar UDFs the low-level way: implement the `VScalar` trait and register with
//! `conn.register_scalar_function::<T>(name)`.
//!
//! This is the second way to define UDFs (the first is the `#[duckdb_scalar]`
//! macro in `ex10-udf-macros`). Implementing the trait by hand exposes the full
//! surface — explicit signatures, a `bind` step, and column-at-a-time `invoke`
//! over `DataChunkHandle` / `VectorMut` — and supports shapes the macro doesn't,
//! such as variadic arguments.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::types::LogicalType;
use better_duck_core::udf::{
    DataChunkHandle, ScalarBindInfo, ScalarSignature, UdfResult, VScalar, VectorMut,
};
use common::{section, show, Result};

/// `add_one(INTEGER) -> INTEGER`: a fixed one-argument signature.
struct AddOne;

impl VScalar for AddOne {
    type State = ();
    type BindData = ();

    fn signatures() -> Result<Vec<ScalarSignature>> {
        Ok(vec![ScalarSignature::exact(vec![LogicalType::of::<i32>()?], LogicalType::of::<i32>()?)])
    }

    fn bind(_bind: &ScalarBindInfo) -> UdfResult<()> {
        Ok(())
    }

    fn invoke(
        _state: &(),
        _bind_data: &(),
        input: &DataChunkHandle,
        output: &mut VectorMut<'_>,
    ) -> UdfResult<()> {
        let col = input.vector(0)?;
        for row in 0..input.len() {
            let v: i32 = col.get(row)?;
            output.set(row, v + 1)?;
        }
        Ok(())
    }
}

/// `sum_all(INTEGER...) -> INTEGER`: a variadic signature summing every argument.
/// Each SQL argument arrives as its own input column, so `invoke` reads across
/// all columns for each row.
struct SumAll;

impl VScalar for SumAll {
    type State = ();
    type BindData = ();

    fn signatures() -> Result<Vec<ScalarSignature>> {
        Ok(vec![ScalarSignature::variadic(LogicalType::of::<i32>()?, LogicalType::of::<i32>()?)])
    }

    fn bind(_bind: &ScalarBindInfo) -> UdfResult<()> {
        Ok(())
    }

    fn invoke(
        _state: &(),
        _bind_data: &(),
        input: &DataChunkHandle,
        output: &mut VectorMut<'_>,
    ) -> UdfResult<()> {
        let cols = input.num_columns();
        for row in 0..input.len() {
            let mut total: i32 = 0;
            for c in 0..cols {
                total += input.vector(c)?.get::<i32>(row)?;
            }
            output.set(row, total)?;
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.register_scalar_function::<AddOne>("add_one")?;
    conn.register_scalar_function::<SumAll>("sum_all")?;

    section("add_one(INTEGER) over a scan");
    let result = conn.execute("SELECT add_one(v) AS r FROM (VALUES (1), (2), (41)) t(v) ORDER BY v")?;
    let rows: Vec<_> = result.collect::<Result<_>>()?;
    let got: Vec<&DuckValue> = rows.iter().filter_map(|row| row.get("r")).collect();
    show("add_one over 1,2,41", &got);
    assert_eq!(got, vec![&DuckValue::Int(2), &DuckValue::Int(3), &DuckValue::Int(42)]);

    section("sum_all(INTEGER...) with a variadic signature");
    let mut r = conn.execute("SELECT sum_all(1, 2, 3, 4) AS r")?;
    let row = r.next().expect("one row")?;
    show("sum_all(1, 2, 3, 4)", row.get("r"));
    assert_eq!(row.get("r"), Some(&DuckValue::Int(10)));

    let mut r = conn.execute("SELECT sum_all(10, 20) AS r")?;
    let row = r.next().expect("one row")?;
    show("sum_all(10, 20)", row.get("r"));
    assert_eq!(row.get("r"), Some(&DuckValue::Int(30)));

    Ok(())
}
