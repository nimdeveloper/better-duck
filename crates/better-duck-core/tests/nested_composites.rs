#![allow(missing_docs)]
use std::collections::HashMap;

use better_duck_core::{connection::Connection, types::value::DuckValue};

fn open() -> Connection {
    Connection::open_in_memory().unwrap()
}

// LIST

#[test]
fn list_of_integers() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT [1, 2, 3] AS v")?;
    let row = result.next().unwrap()?;
    let expected = DuckValue::List(vec![DuckValue::Int(1), DuckValue::Int(2), DuckValue::Int(3)]);
    assert_eq!(row.get("v"), Some(&expected));
    Ok(())
}

#[test]
fn list_of_text() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT ['a', 'b', 'c'] AS v")?;
    let row = result.next().unwrap()?;
    let expected =
        DuckValue::List(vec![DuckValue::text("a"), DuckValue::text("b"), DuckValue::text("c")]);
    assert_eq!(row.get("v"), Some(&expected));
    Ok(())
}

#[test]
fn list_of_booleans() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT [true, false, true] AS v")?;
    let row = result.next().unwrap()?;
    let expected = DuckValue::List(vec![
        DuckValue::Boolean(true),
        DuckValue::Boolean(false),
        DuckValue::Boolean(true),
    ]);
    assert_eq!(row.get("v"), Some(&expected));
    Ok(())
}

#[test]
fn list_with_null_element() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT [1, NULL, 3]::INTEGER[] AS v")?;
    let row = result.next().unwrap()?;
    let expected = DuckValue::List(vec![DuckValue::Int(1), DuckValue::Null, DuckValue::Int(3)]);
    assert_eq!(row.get("v"), Some(&expected));
    Ok(())
}

#[test]
fn empty_list() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT []::INTEGER[] AS v")?;
    let row = result.next().unwrap()?;
    assert_eq!(row.get("v"), Some(&DuckValue::List(vec![])));
    Ok(())
}

#[test]
fn list_of_list() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT [[1, 2], [3, 4]] AS v")?;
    let row = result.next().unwrap()?;
    let expected = DuckValue::List(vec![
        DuckValue::List(vec![DuckValue::Int(1), DuckValue::Int(2)]),
        DuckValue::List(vec![DuckValue::Int(3), DuckValue::Int(4)]),
    ]);
    assert_eq!(row.get("v"), Some(&expected));
    Ok(())
}

#[test]
fn list_of_list_with_null() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT [[1, NULL], [3]] AS v")?;
    let row = result.next().unwrap()?;
    let expected = DuckValue::List(vec![
        DuckValue::List(vec![DuckValue::Int(1), DuckValue::Null]),
        DuckValue::List(vec![DuckValue::Int(3)]),
    ]);
    assert_eq!(row.get("v"), Some(&expected));
    Ok(())
}

// ARRAY (fixed-size)

#[test]
fn fixed_array_integers() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    conn.execute_batch("CREATE TABLE arr_int (v INTEGER[3])")?;
    conn.execute_batch("INSERT INTO arr_int VALUES ([10, 20, 30])")?;
    let mut result = conn.execute("SELECT v FROM arr_int")?;
    let row = result.next().unwrap()?;
    let expected = DuckValue::Array(
        vec![DuckValue::Int(10), DuckValue::Int(20), DuckValue::Int(30)].into_boxed_slice(),
    );
    assert_eq!(row.get("v"), Some(&expected));
    Ok(())
}

#[test]
fn fixed_array_text() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    conn.execute_batch("CREATE TABLE arr_txt (v VARCHAR[2])")?;
    conn.execute_batch("INSERT INTO arr_txt VALUES (['hello', 'world'])")?;
    let mut result = conn.execute("SELECT v FROM arr_txt")?;
    let row = result.next().unwrap()?;
    let expected = DuckValue::Array(
        vec![DuckValue::text("hello"), DuckValue::text("world")].into_boxed_slice(),
    );
    assert_eq!(row.get("v"), Some(&expected));
    Ok(())
}

// STRUCT (nested)

#[test]
fn struct_nested() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute(
        "SELECT {'outer': {'inner': 42}}::STRUCT(\"outer\" STRUCT(\"inner\" INTEGER)) AS s",
    )?;
    let row = result.next().unwrap()?;
    let inner = DuckValue::Struct(HashMap::from([("inner".to_string(), DuckValue::Int(42))]));
    let expected = DuckValue::Struct(HashMap::from([("outer".to_string(), inner)]));
    assert_eq!(row.get("s"), Some(&expected));
    Ok(())
}

#[test]
fn struct_with_list_field() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute(
        "SELECT {'items': [1, 2, 3], 'count': 3}\
         ::STRUCT(items INTEGER[], count INTEGER) AS s",
    )?;
    let row = result.next().unwrap()?;
    let expected = DuckValue::Struct(HashMap::from([
        (
            "items".to_string(),
            DuckValue::List(vec![DuckValue::Int(1), DuckValue::Int(2), DuckValue::Int(3)]),
        ),
        ("count".to_string(), DuckValue::Int(3)),
    ]));
    assert_eq!(row.get("s"), Some(&expected));
    Ok(())
}

#[test]
fn struct_with_map_field() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result =
        conn.execute("SELECT {'meta': MAP {'k': 1}}::STRUCT(meta MAP(VARCHAR, INTEGER)) AS s")?;
    let row = result.next().unwrap()?;
    let inner_map = DuckValue::Map(HashMap::from([(DuckValue::text("k"), DuckValue::Int(1))]));
    let expected = DuckValue::Struct(HashMap::from([("meta".to_string(), inner_map)]));
    assert_eq!(row.get("s"), Some(&expected));
    Ok(())
}

// MAP (complex values)

#[test]
fn map_varchar_to_list() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT MAP {'evens': [2, 4, 6], 'odds': [1, 3, 5]} AS m")?;
    let row = result.next().unwrap()?;
    let val = row.get("m").expect("column m missing");
    if let DuckValue::Map(ref m) = val {
        assert_eq!(m.len(), 2);
        let evens = m.get(&DuckValue::text("evens")).expect("evens key missing");
        let odds = m.get(&DuckValue::text("odds")).expect("odds key missing");
        assert_eq!(
            evens,
            &DuckValue::List(vec![DuckValue::Int(2), DuckValue::Int(4), DuckValue::Int(6)])
        );
        assert_eq!(
            odds,
            &DuckValue::List(vec![DuckValue::Int(1), DuckValue::Int(3), DuckValue::Int(5)])
        );
    } else {
        panic!("expected DuckValue::Map, got {:?}", val);
    }
    Ok(())
}

#[test]
fn map_varchar_to_struct() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result =
        conn.execute("SELECT MAP {'p': {'x': 1, 'y': 2}::STRUCT(x INTEGER, y INTEGER)} AS m")?;
    let row = result.next().unwrap()?;
    let val = row.get("m").expect("column m missing");
    if let DuckValue::Map(ref m) = val {
        assert_eq!(m.len(), 1);
        let p = m.get(&DuckValue::text("p")).expect("p key missing");
        let expected_struct = DuckValue::Struct(HashMap::from([
            ("x".to_string(), DuckValue::Int(1)),
            ("y".to_string(), DuckValue::Int(2)),
        ]));
        assert_eq!(p, &expected_struct);
    } else {
        panic!("expected DuckValue::Map, got {:?}", val);
    }
    Ok(())
}

#[test]
fn map_integer_to_list() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT MAP {1: ['a', 'b'], 2: ['c']} AS m")?;
    let row = result.next().unwrap()?;
    let val = row.get("m").expect("column m missing");
    if let DuckValue::Map(ref m) = val {
        assert_eq!(m.len(), 2);
        let v1 = m.get(&DuckValue::Int(1)).expect("key 1 missing");
        let v2 = m.get(&DuckValue::Int(2)).expect("key 2 missing");
        assert_eq!(v1, &DuckValue::List(vec![DuckValue::text("a"), DuckValue::text("b")]));
        assert_eq!(v2, &DuckValue::List(vec![DuckValue::text("c")]));
    } else {
        panic!("expected DuckValue::Map, got {:?}", val);
    }
    Ok(())
}

// LIST of composites

#[test]
fn list_of_struct() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT [{'x': 1, 'y': 2}, {'x': 3, 'y': 4}] AS v")?;
    let row = result.next().unwrap()?;
    let s1 = DuckValue::Struct(HashMap::from([
        ("x".to_string(), DuckValue::Int(1)),
        ("y".to_string(), DuckValue::Int(2)),
    ]));
    let s2 = DuckValue::Struct(HashMap::from([
        ("x".to_string(), DuckValue::Int(3)),
        ("y".to_string(), DuckValue::Int(4)),
    ]));
    let expected = DuckValue::List(vec![s1, s2]);
    assert_eq!(row.get("v"), Some(&expected));
    Ok(())
}

#[test]
fn list_of_map() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT [MAP {'k': 1}, MAP {'k': 2}] AS v")?;
    let row = result.next().unwrap()?;
    let m1 = DuckValue::Map(HashMap::from([(DuckValue::text("k"), DuckValue::Int(1))]));
    let m2 = DuckValue::Map(HashMap::from([(DuckValue::text("k"), DuckValue::Int(2))]));
    let expected = DuckValue::List(vec![m1, m2]);
    assert_eq!(row.get("v"), Some(&expected));
    Ok(())
}

// Deeply nested (≥ 3 levels)

#[test]
fn deep_struct_list_struct() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute(
        "SELECT {'rows': [{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}]}\
         ::STRUCT(rows STRUCT(id INTEGER, name VARCHAR)[]) AS v",
    )?;
    let row = result.next().unwrap()?;
    let s1 = DuckValue::Struct(HashMap::from([
        ("id".to_string(), DuckValue::Int(1)),
        ("name".to_string(), DuckValue::text("a")),
    ]));
    let s2 = DuckValue::Struct(HashMap::from([
        ("id".to_string(), DuckValue::Int(2)),
        ("name".to_string(), DuckValue::text("b")),
    ]));
    let expected =
        DuckValue::Struct(HashMap::from([("rows".to_string(), DuckValue::List(vec![s1, s2]))]));
    assert_eq!(row.get("v"), Some(&expected));
    Ok(())
}

#[test]
fn deep_map_list_struct() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result =
        conn.execute("SELECT MAP {'pts': [{'x': 1, 'y': 2}, {'x': 3, 'y': 4}]} AS m")?;
    let row = result.next().unwrap()?;
    let val = row.get("m").expect("column m missing");
    if let DuckValue::Map(ref m) = val {
        assert_eq!(m.len(), 1);
        let pts = m.get(&DuckValue::text("pts")).expect("pts key missing");
        if let DuckValue::List(ref list) = pts {
            assert_eq!(list.len(), 2);
            let s1 = DuckValue::Struct(HashMap::from([
                ("x".to_string(), DuckValue::Int(1)),
                ("y".to_string(), DuckValue::Int(2)),
            ]));
            assert_eq!(&list[0], &s1);
        } else {
            panic!("expected List for pts, got {:?}", pts);
        }
    } else {
        panic!("expected DuckValue::Map, got {:?}", val);
    }
    Ok(())
}

// UNION extended

#[test]
fn union_struct_member() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    conn.execute_batch(
        "CREATE TABLE u_struct (u UNION(n INTEGER, p STRUCT(x INTEGER, y INTEGER)))",
    )?;
    conn.execute_batch("INSERT INTO u_struct VALUES (union_value(p := {'x': 10, 'y': 20}))")?;
    let mut result = conn.execute("SELECT u FROM u_struct")?;
    let row = result.next().unwrap()?;
    let expected_inner = DuckValue::Struct(HashMap::from([
        ("x".to_string(), DuckValue::Int(10)),
        ("y".to_string(), DuckValue::Int(20)),
    ]));
    assert_eq!(row.get("u"), Some(&DuckValue::Union(Box::new(expected_inner))));
    Ok(())
}

#[test]
fn union_null_row() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    conn.execute_batch("CREATE TABLE u_null (u UNION(n INTEGER, s VARCHAR))")?;
    conn.execute_batch("INSERT INTO u_null VALUES (NULL)")?;
    let mut result = conn.execute("SELECT u FROM u_null")?;
    let row = result.next().unwrap()?;
    assert_eq!(row.get("u"), Some(&DuckValue::Null));
    Ok(())
}

// ENUM

#[test]
fn enum_basic() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    conn.execute_batch("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')")?;
    conn.execute_batch("CREATE TABLE moods (m mood)")?;
    conn.execute_batch("INSERT INTO moods VALUES ('happy'), (NULL), ('sad')")?;
    let rows: Vec<_> = conn.execute("SELECT m FROM moods")?.collect::<Result<_, _>>()?;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("m"), Some(&DuckValue::Enum("happy".to_string())));
    assert_eq!(rows[1].get("m"), Some(&DuckValue::Null));
    assert_eq!(rows[2].get("m"), Some(&DuckValue::Enum("sad".to_string())));
    Ok(())
}
