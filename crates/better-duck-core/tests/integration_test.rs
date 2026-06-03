#![allow(missing_docs)]
use std::collections::HashMap;

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

#[test]
fn read_map_string_keys() -> better_duck_core::error::Result<()> {
    let conn = Connection::open_in_memory()?;
    let mut stmt = conn.db().prepare("SELECT MAP {'k1': 10, 'k2': 20} AS m")?;
    let mut result = stmt.execute()?;
    let row = result.next().expect("expected one row")?;

    let val = row.get("m").expect("column 'm' missing");
    if let DuckValue::Map(ref m) = val {
        assert_eq!(m.len(), 2, "expected 2 map entries");
        assert_eq!(m.get(&DuckValue::Text("k1".to_string())), Some(&DuckValue::Int(10)));
        assert_eq!(m.get(&DuckValue::Text("k2".to_string())), Some(&DuckValue::Int(20)));
    } else {
        panic!("expected DuckValue::Map, got {:?}", val);
    }
    assert!(result.next().is_none());
    Ok(())
}

#[test]
fn read_map_integer_keys() -> better_duck_core::error::Result<()> {
    let conn = Connection::open_in_memory()?;
    // Integer-keyed MAP: keys are stored as DuckValue::Int (not stringified).
    let mut stmt = conn.db().prepare("SELECT MAP {1: 'one', 2: 'two'} AS m")?;
    let mut result = stmt.execute()?;
    let row = result.next().expect("expected one row")?;

    let val = row.get("m").expect("column 'm' missing");
    if let DuckValue::Map(ref m) = val {
        assert_eq!(m.len(), 2);
    } else {
        panic!("expected DuckValue::Map, got {:?}", val);
    }
    // Use ergonomic helpers: `val.get(key)` accepts any Into<DuckValue>.
    assert_eq!(val.get(1i32), Some(&DuckValue::text("one")));
    assert_eq!(val.get(2i32), Some(&DuckValue::text("two")));
    assert_eq!(val.get(99i32), None);
    Ok(())
}

#[test]
fn read_struct_integer_and_text() -> better_duck_core::error::Result<()> {
    let conn = Connection::open_in_memory()?;
    // Explicit STRUCT cast gives known field order (a, b).
    let mut stmt =
        conn.db().prepare("SELECT {'a': 1, 'b': 'hello'}::STRUCT(a INTEGER, b TEXT) AS s")?;
    let mut result = stmt.execute()?;
    let row = result.next().expect("expected one row")?;

    let expected = DuckValue::Struct(HashMap::from([
        ("a".to_string(), DuckValue::Int(1)),
        ("b".to_string(), DuckValue::Text("hello".to_string())),
    ]));
    assert_eq!(row.get("s"), Some(&expected));
    assert!(result.next().is_none());
    Ok(())
}

#[test]
fn read_struct_with_null_field() -> better_duck_core::error::Result<()> {
    let conn = Connection::open_in_memory()?;
    let mut stmt = conn.db().prepare(
        "SELECT {'yes': 'duck', 'huh': NULL, 'no': 'heron'}::STRUCT(yes TEXT, huh TEXT, no TEXT) AS s",
    )?;
    let mut result = stmt.execute()?;
    let row = result.next().expect("expected one row")?;

    let expected = DuckValue::Struct(HashMap::from([
        ("yes".to_string(), DuckValue::Text("duck".to_string())),
        ("huh".to_string(), DuckValue::Null),
        ("no".to_string(), DuckValue::Text("heron".to_string())),
    ]));
    assert_eq!(row.get("s"), Some(&expected));
    Ok(())
}

#[test]
fn read_struct_mixed_types() -> better_duck_core::error::Result<()> {
    let conn = Connection::open_in_memory()?;
    let mut stmt = conn.db().prepare(
        "SELECT {'key1': 'string', 'key2': 1, 'key3': 12.345, 'key4': NULL, 'key5': False}::STRUCT(key1 TEXT, key2 INTEGER, key3 DOUBLE, key4 INTEGER, key5 BOOLEAN) AS s",
    )?;
    let mut result = stmt.execute()?;
    let row = result.next().expect("expected one row")?;

    let val = row.get("s").expect("column 's' missing");
    if let DuckValue::Struct(ref m) = val {
        assert_eq!(m.len(), 5, "expected 5 struct fields");
        // key1 = Text("string")
        assert_eq!(m.get("key1"), Some(&DuckValue::Text("string".to_string())));
        // key2 = Int(1)
        assert_eq!(m.get("key2"), Some(&DuckValue::Int(1)));
        // key4 = Null
        assert_eq!(m.get("key4"), Some(&DuckValue::Null));
        // key5 = Boolean(false)
        assert_eq!(m.get("key5"), Some(&DuckValue::Boolean(false)));
    } else {
        panic!("expected DuckValue::Struct, got {:?}", val);
    }
    Ok(())
}

#[test]
fn read_union_integer_member() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE tbl1 (u UNION(num INTEGER, str VARCHAR))")?;
    conn.execute_batch("INSERT INTO tbl1 VALUES (1)")?;

    let mut stmt = conn.db().prepare("SELECT u FROM tbl1")?;
    let mut result = stmt.execute()?;

    let row = result.next().expect("expected row")?;
    // Integer member: active variant holds Int(1)
    assert_eq!(row.get("u"), Some(&DuckValue::Union(Box::new(DuckValue::Int(1)))));
    assert!(result.next().is_none());
    Ok(())
}

#[test]
fn read_union_text_member() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE tbl2 (u UNION(num INTEGER, str VARCHAR))")?;
    conn.execute_batch("INSERT INTO tbl2 VALUES ('two')")?;

    let mut stmt = conn.db().prepare("SELECT u FROM tbl2")?;
    let mut result = stmt.execute()?;

    let row = result.next().expect("expected row")?;
    // Text member: active variant holds Text("two")
    assert_eq!(row.get("u"), Some(&DuckValue::Union(Box::new(DuckValue::Text("two".to_string())))));
    assert!(result.next().is_none());
    Ok(())
}

#[test]
fn read_union_multiple_rows() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE tbl3 (u UNION(num INTEGER, str VARCHAR))")?;
    conn.execute_batch("INSERT INTO tbl3 VALUES (1), ('two'), (union_value(str := 'three'))")?;

    let mut stmt = conn.db().prepare("SELECT u FROM tbl3")?;
    let rows: Vec<_> = stmt.execute()?.collect::<Result<_, _>>()?;
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].get("u"), Some(&DuckValue::Union(Box::new(DuckValue::Int(1)))));
    assert_eq!(
        rows[1].get("u"),
        Some(&DuckValue::Union(Box::new(DuckValue::Text("two".to_string()))))
    );
    assert_eq!(
        rows[2].get("u"),
        Some(&DuckValue::Union(Box::new(DuckValue::Text("three".to_string()))))
    );
    Ok(())
}
