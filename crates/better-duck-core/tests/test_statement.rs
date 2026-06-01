use better_duck_core::{connection::Connection, types::value::DuckValue, CachedStatement};

// execute (DML paths)

#[test]
fn execute_dml_insert_count() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (v INTEGER)")?;
    let changed = conn.execute("INSERT INTO t VALUES (1)")?.changes();
    assert_eq!(changed, 1);
    Ok(())
}

#[test]
fn execute_dml_update_count() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (v INTEGER)")?;
    conn.execute_batch("INSERT INTO t VALUES (1), (2)")?;
    let changed = conn.execute("UPDATE t SET v = 99 WHERE v = 1")?.changes();
    assert_eq!(changed, 1);
    Ok(())
}

#[test]
fn execute_dml_delete_count() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (v INTEGER)")?;
    conn.execute_batch("INSERT INTO t VALUES (1), (2)")?;
    let changed = conn.execute("DELETE FROM t WHERE v = 1")?.changes();
    assert_eq!(changed, 1);
    Ok(())
}

#[test]
fn execute_dml_ddl_returns_zero() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    let changed = conn.execute("CREATE TABLE t (v INTEGER)")?.changes();
    assert_eq!(changed, 0);
    Ok(())
}

#[test]
fn execute_dml_no_match_returns_zero() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (v INTEGER)")?;
    conn.execute_batch("INSERT INTO t VALUES (1)")?;
    // WHERE clause never matches — should return 0, not an error
    let changed = conn.execute("UPDATE t SET v = 99 WHERE 0 = 1")?.changes();
    assert_eq!(changed, 0);
    Ok(())
}

// execute (query / row-returning paths)

#[test]
fn query_rows_returns_values() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (v INTEGER)")?;
    conn.execute_batch("INSERT INTO t VALUES (42)")?;
    let mut rows = conn.execute("SELECT v FROM t")?;
    let row = rows.next().expect("expected one row")?;
    assert_eq!(row.get("v"), Some(&DuckValue::Int(42)));
    assert!(rows.next().is_none());
    Ok(())
}

#[test]
fn query_rows_with_bound_parameter() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (v INTEGER)")?;
    conn.execute_batch("INSERT INTO t VALUES (10), (20)")?;
    let mut target: i32 = 10;
    let mut rows = conn.execute_with("SELECT v FROM t WHERE v = $1", &mut [&mut target])?;
    let row = rows.next().expect("expected one row")?;
    assert_eq!(row.get("v"), Some(&DuckValue::Int(10)));
    assert!(rows.next().is_none());
    Ok(())
}

// CachedStatement

#[test]
fn cached_statement_reuse() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (v INTEGER)")?;

    // Prepare once, execute twice with different values.
    let mut stmt = CachedStatement::prepare(conn.db(), "INSERT INTO t VALUES ($1)")?;

    let mut val_a: i32 = 100;
    stmt.bind(1, &mut val_a)?;
    let n = stmt.execute()?.changes();
    assert_eq!(n, 1);

    stmt.reset_bindings()?;
    let mut val_b: i32 = 200;
    stmt.bind(1, &mut val_b)?;
    let n = stmt.execute()?.changes();
    assert_eq!(n, 1);

    // Verify both rows landed.
    let rows: Vec<_> = conn.execute("SELECT v FROM t ORDER BY v")?.collect::<Result<_, _>>()?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("v"), Some(&DuckValue::Int(100)));
    assert_eq!(rows[1].get("v"), Some(&DuckValue::Int(200)));
    Ok(())
}

#[test]
fn returning_clause_works() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (id INTEGER)")?;

    let mut stmt = CachedStatement::prepare(conn.db(), "INSERT INTO t VALUES (7) RETURNING id")?;
    let mut result = stmt.execute()?;
    let row = result.next().expect("expected one row")?;
    assert_eq!(row.get("id"), Some(&DuckValue::Int(7)));
    assert!(result.next().is_none());
    Ok(())
}
