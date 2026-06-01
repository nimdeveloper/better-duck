#![allow(missing_docs)]
use better_duck_core::{connection::Connection, types::value::DuckValue};

#[test]
fn round_trip_integer_and_text() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (id INTEGER, name TEXT)")?;
    conn.execute_batch("INSERT INTO t VALUES (1, 'Alice')")?;

    let mut stmt = conn.db().prepare("SELECT id, name FROM t")?;
    let mut result = stmt.execute()?;

    let row = result.next().expect("expected one row")?;
    assert_eq!(row.get("id"), Some(&DuckValue::Int(1)));
    assert_eq!(row.get("name"), Some(&DuckValue::Text("Alice".to_string())));
    assert!(result.next().is_none());
    Ok(())
}

#[test]
fn statement_reuse_with_clear_bindings() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE nums (v INTEGER)")?;
    conn.execute_batch("INSERT INTO nums VALUES (10), (20)")?;

    let mut stmt = conn.db().prepare("SELECT v FROM nums WHERE v = $1")?;

    stmt.bind(&mut 10i32)?;
    let rows: Vec<_> = stmt.execute()?.collect::<Result<_, _>>()?;
    assert_eq!(rows.len(), 1);

    stmt.clear_bindings()?;
    stmt.bind(&mut 20i32)?;
    let rows: Vec<_> = stmt.execute()?.collect::<Result<_, _>>()?;
    assert_eq!(rows.len(), 1);
    Ok(())
}
