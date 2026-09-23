//! Failing a UDF from its body with `duck_bail!`.
//!
//! In a fallible UDF body, `duck_bail!("msg {}", x)` returns early with a
//! formatted error. The query fails with that message and the connection stays
//! usable — the preferred alternative to panicking.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::{duck_bail, duckdb_scalar};
use common::{section, show, Result};

/// Returns `x` when it is non-negative; otherwise bails with a message.
#[duckdb_scalar(name = "checked_abs")]
fn checked_abs(x: i32) -> std::result::Result<i32, String> {
    if x < 0 {
        duck_bail!("value must be non-negative, got {}", x);
    }
    Ok(x)
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    checked_abs::register(&mut conn)?;

    section("checked_abs(7): the happy path returns a value");
    let mut r = conn.execute("SELECT checked_abs(7)")?;
    let row = r.next().expect("one row")?;
    show("checked_abs(7)", row.get_idx(0));
    assert_eq!(row.get_idx(0), Some(&DuckValue::Int(7)));

    section("checked_abs(-1): duck_bail! surfaces as a query error");
    let err = conn.execute("SELECT checked_abs(-1)").err().expect("an error");
    show("error", err.to_string());
    assert!(err.to_string().contains("must be non-negative"), "{err}");

    section("the connection stays usable after the bailed query");
    let mut ok = conn.execute("SELECT checked_abs(3) AS r")?;
    assert_eq!(ok.next().expect("row")?.get("r"), Some(&DuckValue::Int(3)));
    show("recovered", true);

    Ok(())
}
