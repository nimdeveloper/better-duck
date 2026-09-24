#![allow(missing_docs)]
#![cfg(feature = "derive")]

//! End-to-end tests for `#[derive(DuckEnum)]`: binding a Rust enum as a parameter
//! (by label, cast to the target `ENUM`), reading an `ENUM` column back into the
//! Rust enum, `From<T> for DuckValue`, and `rename_all` label mapping.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::{params, DuckEnum, FromDuckValue};

#[derive(DuckEnum, Debug, PartialEq, Clone, Copy)]
enum Suit {
    Hearts,
    Diamonds,
    Clubs,
    Spades,
}

#[derive(DuckEnum, Debug, PartialEq)]
#[duck_enum(rename_all = "snake_case")]
enum Status {
    Active,
    InProgress,
    Done,
}

#[test]
fn enum_binds_and_reads_back() {
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TYPE suit AS ENUM ('Hearts','Diamonds','Clubs','Spades')").unwrap();
    conn.execute_batch("CREATE TABLE cards (s suit)").unwrap();

    // Bound by label; DuckDB casts the text to the ENUM column.
    conn.execute_with("INSERT INTO cards VALUES ($1)", &mut params![Suit::Clubs]).unwrap();

    let rs = conn.execute("SELECT s FROM cards").unwrap().materialize().unwrap();
    let got = Suit::from_duck_value(rs.rows()[0].get("s").unwrap()).unwrap();
    assert_eq!(got, Suit::Clubs);
}

#[test]
fn enum_into_duckvalue_uses_variant_label() {
    let dv: DuckValue = Suit::Spades.into();
    match dv {
        DuckValue::Enum(e) => assert_eq!(e.label(), "Spades"),
        other => panic!("expected DuckValue::Enum, got {other:?}"),
    }
}

#[test]
fn rename_all_maps_variant_labels() {
    let dv: DuckValue = Status::InProgress.into();
    match &dv {
        DuckValue::Enum(e) => assert_eq!(e.label(), "in_progress"),
        other => panic!("expected DuckValue::Enum, got {other:?}"),
    }
    // And it reads back through the same label mapping.
    let back = Status::from_duck_value(&dv).unwrap();
    assert_eq!(back, Status::InProgress);
}
