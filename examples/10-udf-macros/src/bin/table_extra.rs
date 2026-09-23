//! A table UDF that reads registration-time "extra info" via `duck_extra_info!`.
//!
//! `extra_info(Type, init)` fixes a `Type` value at registration; the body reads
//! a clone of it with `duck_extra_info!(Type)`.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::{duck_extra_info, duckdb_table_function};
use common::{section, show, Result};

/// Emits a single row equal to the extra info fixed at registration (100).
#[duckdb_table_function(columns("n"), extra_info(i64, 100))]
fn with_extra() -> impl Iterator<Item = i64> + Send {
    std::iter::once(duck_extra_info!(i64))
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    with_extra::register(&mut conn)?;

    section("SELECT n FROM with_extra(): reads registration-time extra info");
    let mut r = conn.execute("SELECT n FROM with_extra()")?;
    let row = r.next().expect("one row")?;
    show("with_extra() -> n", row.get("n"));
    assert_eq!(row.get("n"), Some(&DuckValue::BigInt(100)));

    Ok(())
}
