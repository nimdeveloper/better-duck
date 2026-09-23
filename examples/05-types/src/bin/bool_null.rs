//! `DuckValue::Boolean` and `DuckValue::Null` — the boolean and SQL-NULL variants.
//!
//! `Boolean(bool)` maps to DuckDB BOOLEAN. `Null` is DuckDB's SQL `NULL`: a value
//! that has no type of its own and takes on the type of the column it lands in. A
//! bound `DuckValue::Null` becomes a SQL NULL, and *any* column that reads back as
//! NULL — regardless of its declared type — decodes to `DuckValue::Null`.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use common::{section, show, Result};

/// Binds `dv` as `$1` and reads the single result column back.
fn round_trip(
    conn: &mut Connection,
    mut dv: DuckValue,
) -> Result<DuckValue> {
    let row = conn.execute_with("SELECT $1 AS v", &mut [&mut dv])?.next().expect("one row")?;
    Ok(row.get("v").expect("column v").clone())
}

/// Reads the single column of `SELECT <expr> AS v`.
fn read_scalar(
    conn: &mut Connection,
    expr: &str,
) -> Result<DuckValue> {
    let sql = format!("SELECT {expr} AS v");
    let row = conn.execute(sql)?.next().expect("one row")?;
    Ok(row.get("v").expect("column v").clone())
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    section("Boolean(true/false)");
    let t = round_trip(&mut conn, DuckValue::Boolean(true))?;
    show("Boolean(true)", &t);
    assert_eq!(t, DuckValue::Boolean(true));
    assert_eq!(round_trip(&mut conn, DuckValue::Boolean(false))?, DuckValue::Boolean(false));

    section("Null — binding DuckValue::Null produces a SQL NULL");
    // COALESCE proves the bound $1 really was SQL NULL: it falls through to the default.
    let mut null_bind = DuckValue::Null;
    let row = conn
        .execute_with("SELECT COALESCE($1, 'fallback') AS v", &mut [&mut null_bind])?
        .next()
        .expect("one row")?;
    show("COALESCE(NULL, 'fallback')", row.get("v"));
    assert_eq!(row.get("v"), Some(&DuckValue::text("fallback")));

    section("reading NULL columns decodes to DuckValue::Null for every type");
    // A typed NULL (INTEGER, VARCHAR, BOOLEAN, ...) always reads back as Null: the
    // decoder checks the row's validity bitmap before it looks at the column type.
    for expr in ["NULL::INTEGER", "NULL::VARCHAR", "NULL::BOOLEAN", "NULL::DOUBLE", "NULL::BLOB"] {
        let v = read_scalar(&mut conn, expr)?;
        show(expr, &v);
        assert_eq!(v, DuckValue::Null);
    }

    section("mixed row: NULL, value, NULL");
    let row = conn
        .execute("SELECT NULL::INTEGER AS a, 42 AS b, NULL::VARCHAR AS c")?
        .next()
        .expect("one row")?;
    assert_eq!(row.get("a"), Some(&DuckValue::Null));
    assert_eq!(row.get("b"), Some(&DuckValue::Int(42)));
    assert_eq!(row.get("c"), Some(&DuckValue::Null));

    println!("\nboolean and null handling verified");
    Ok(())
}
