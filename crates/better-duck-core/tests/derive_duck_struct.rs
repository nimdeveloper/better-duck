#![allow(missing_docs)]
#![cfg(feature = "derive")]

//! End-to-end tests for `#[derive(DuckStruct)]`: `Into<DuckValue>` builds a
//! `STRUCT`, and `FromDuckValue` reads a `STRUCT` column back into the Rust struct.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::{DuckStruct, FromDuckValue};

#[derive(DuckStruct, Debug, PartialEq)]
struct Point {
    x: i32,
    y: i32,
}

#[derive(DuckStruct, Debug, PartialEq)]
struct Labeled {
    #[duck(rename = "the_label")]
    label: String,
    value: i32,
}

#[test]
fn struct_into_duckvalue_builds_struct() {
    let dv: DuckValue = Point { x: 3, y: 4 }.into();
    match dv {
        DuckValue::Struct(m) => {
            assert_eq!(m.get("x"), Some(&DuckValue::Int(3)));
            assert_eq!(m.get("y"), Some(&DuckValue::Int(4)));
        },
        other => panic!("expected DuckValue::Struct, got {other:?}"),
    }
}

#[test]
fn reads_struct_column_into_rust_struct() {
    let mut conn = Connection::open_in_memory().unwrap();
    let rs = conn
        .execute("SELECT {'x': CAST(3 AS INTEGER), 'y': CAST(4 AS INTEGER)} AS s")
        .unwrap()
        .materialize()
        .unwrap();
    let got = Point::from_duck_value(rs.rows()[0].get("s").unwrap()).unwrap();
    assert_eq!(got, Point { x: 3, y: 4 });
}

#[test]
fn field_rename_applies_to_struct_value() {
    let dv: DuckValue = Labeled { label: "hi".to_owned(), value: 7 }.into();
    match dv {
        DuckValue::Struct(m) => {
            assert!(m.contains_key("the_label"));
            assert_eq!(m.get("value"), Some(&DuckValue::Int(7)));
        },
        other => panic!("expected DuckValue::Struct, got {other:?}"),
    }
}
