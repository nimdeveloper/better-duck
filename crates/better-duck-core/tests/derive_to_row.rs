#![allow(missing_docs)]
#![cfg(feature = "derive")]

//! `#[derive(ToRow)]` appends a struct's fields, in declaration order, as one
//! appender row.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::ToRow;

#[derive(ToRow)]
struct NewItem {
    id: i32,
    name: String,
}

#[test]
fn to_row_appends_struct_as_a_row() {
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE items (id INTEGER, name VARCHAR)").unwrap();
    {
        let mut appender = conn.appender("items", "main").unwrap();
        appender.append(&mut NewItem { id: 1, name: "a".to_owned() }).unwrap();
        appender.append(&mut NewItem { id: 2, name: "b".to_owned() }).unwrap();
        appender.finish().unwrap();
    }

    let rs = conn
        .execute("SELECT id, name FROM items ORDER BY id")
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(rs.rows().len(), 2);
    assert_eq!(rs.rows()[0].get("id"), Some(&DuckValue::Int(1)));
    assert_eq!(rs.rows()[0].get("name"), Some(&DuckValue::Text("a".to_owned())));
    assert_eq!(rs.rows()[1].get("id"), Some(&DuckValue::Int(2)));
    assert_eq!(rs.rows()[1].get("name"), Some(&DuckValue::Text("b".to_owned())));
}
