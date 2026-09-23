//! Composite `DuckValue` variants: `List`, `Array`, `Struct`, `Map`, `Union`.
//!
//!   * `List(Vec<DuckValue>)`   — DuckDB `LIST`, a variable-length homogeneous list.
//!   * `Array(Box<[DuckValue]>)` — DuckDB `ARRAY`, a *fixed*-size list (size >= 1).
//!   * `Struct(HashMap<String, DuckValue>)` — `STRUCT`, named fields, fixed schema.
//!   * `Map(HashMap<DuckValue, DuckValue>)` — `MAP`, arbitrary key -> value pairs.
//!   * `Union(DuckUnion)` — `UNION`, a tagged sum type keeping its full member schema.
//!
//! Composite values are easiest to read via SQL literals, so this example builds each
//! shape in SQL and asserts the decoded Rust structure.

use std::collections::HashMap;

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use common::{section, show, Result};

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

    section("List — Vec<DuckValue> (variable length)");
    let list = read_scalar(&mut conn, "[1, 2, 3]")?;
    show("[1, 2, 3]", &list);
    assert_eq!(
        list,
        DuckValue::List(vec![DuckValue::Int(1), DuckValue::Int(2), DuckValue::Int(3)])
    );
    // A LIST may be empty and may contain NULL elements.
    assert_eq!(read_scalar(&mut conn, "[]::INTEGER[]")?, DuckValue::List(vec![]));
    assert_eq!(
        read_scalar(&mut conn, "[1, NULL, 3]::INTEGER[]")?,
        DuckValue::List(vec![DuckValue::Int(1), DuckValue::Null, DuckValue::Int(3)])
    );

    section("Array — Box<[DuckValue]> (fixed size, at least 1 element)");
    // A fixed-size ARRAY column. Unlike LIST, an ARRAY must have size >= 1.
    conn.execute_batch("CREATE TABLE arr (v INTEGER[3])")?;
    conn.execute_batch("INSERT INTO arr VALUES ([10, 20, 30])")?;
    let row = conn.execute("SELECT v FROM arr")?.next().expect("one row")?;
    show("INTEGER[3]", row.get("v"));
    let expected_arr = DuckValue::Array(
        vec![DuckValue::Int(10), DuckValue::Int(20), DuckValue::Int(30)].into_boxed_slice(),
    );
    assert_eq!(row.get("v"), Some(&expected_arr));

    section("Struct — HashMap<String, DuckValue> (named fields)");
    // An explicit STRUCT cast fixes the field names and order.
    let s = read_scalar(&mut conn, "{'a': 1, 'b': 'x'}::STRUCT(a INTEGER, b TEXT)")?;
    show("STRUCT(a, b)", &s);
    let expected_struct = DuckValue::Struct(HashMap::from([
        ("a".to_string(), DuckValue::Int(1)),
        ("b".to_string(), DuckValue::text("x")),
    ]));
    assert_eq!(s, expected_struct);

    section("Map — HashMap<DuckValue, DuckValue> (dynamic key -> value)");
    let m = read_scalar(&mut conn, "MAP {'k1': 10, 'k2': 20}")?;
    show("MAP {'k1': 10, 'k2': 20}", &m);
    if let DuckValue::Map(ref map) = m {
        assert_eq!(map.len(), 2);
        // MAP keys are real DuckValues; look one up by key.
        assert_eq!(map.get(&DuckValue::text("k1")), Some(&DuckValue::Int(10)));
    } else {
        panic!("expected Map, got {m:?}");
    }
    // DuckValue::get is a convenience for Map lookups by any Into<DuckValue> key.
    assert_eq!(m.get("k2"), Some(&DuckValue::Int(20)));

    section("Union — DuckUnion keeps the full member schema and the active tag");
    conn.execute_batch("CREATE TABLE u (id INTEGER, v UNION(num INTEGER, str VARCHAR))")?;
    conn.execute_batch("INSERT INTO u VALUES (1, 1), (2, 'two')")?;
    let rows: Vec<_> =
        conn.execute("SELECT v FROM u ORDER BY id")?.collect::<std::result::Result<_, _>>()?;

    // First row: the active member is `num` = 1; both members stay in the schema.
    match rows[0].get("v") {
        Some(DuckValue::Union(u)) => {
            show("active_name", u.active_name());
            show("value", u.value());
            show("members", u.members().iter().map(|(n, _)| n.clone()).collect::<Vec<_>>());
            assert_eq!(u.active_name(), "num");
            assert_eq!(u.value(), &DuckValue::Int(1));
            assert_eq!(u.members().len(), 2);
        },
        other => panic!("expected Union, got {other:?}"),
    }
    // Second row: the active member is `str` = "two".
    match rows[1].get("v") {
        Some(DuckValue::Union(u)) => {
            assert_eq!(u.active_name(), "str");
            assert_eq!(u.value(), &DuckValue::text("two"));
        },
        other => panic!("expected Union, got {other:?}"),
    }

    println!("\nlist, array, struct, map, and union verified");
    Ok(())
}
