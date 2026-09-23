//! `DuckValue::Enum` — DuckDB `ENUM`, a dictionary type.
//!
//! An ENUM column's logical type carries an ordered list of string labels, and each
//! row stores a small integer *index* into that dictionary. [`DuckEnum`] preserves
//! both the dictionary (shared via `Arc<[String]>`) and the selected index, so a
//! value read from an ENUM column round-trips as a real ENUM rather than degrading
//! to VARCHAR.
//!
//! Public items shown: [`DuckEnum::new`] (dict + index), [`DuckEnum::from_label`]
//! (dict + label lookup), [`DuckEnum::label`], [`DuckEnum::index`],
//! [`DuckEnum::dictionary`].

use std::sync::Arc;

use better_duck_core::connection::Connection;
use better_duck_core::error::Error;
use better_duck_core::types::duck_enum::DuckEnum;
use better_duck_core::types::value::DuckValue;
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    section("read an ENUM column");
    // A user-defined ENUM type; the dictionary order is the declaration order.
    conn.execute_batch("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')")?;
    conn.execute_batch("CREATE TABLE moods (m mood)")?;
    conn.execute_batch("INSERT INTO moods VALUES ('happy'), (NULL), ('sad')")?;

    let rows: Vec<_> =
        conn.execute("SELECT m FROM moods")?.collect::<std::result::Result<_, _>>()?;
    assert_eq!(rows.len(), 3);

    // A non-NULL ENUM row decodes to DuckValue::Enum, preserving the dictionary + index.
    match rows[0].get("m") {
        Some(DuckValue::Enum(e)) => {
            show("label()", e.label());
            show("index()", e.index());
            show("dictionary()", e.dictionary().to_vec());
            assert_eq!(e.label(), "happy");
            assert_eq!(e.index(), 0);
            assert_eq!(e.dictionary(), &["happy", "sad", "neutral"]);
        },
        other => panic!("expected Enum, got {other:?}"),
    }
    // A NULL ENUM row is still DuckValue::Null.
    assert_eq!(rows[1].get("m"), Some(&DuckValue::Null));

    section("build DuckEnum values in Rust");
    // The dictionary is shared via Arc<[String]>; clone it cheaply for many values.
    let dict: Arc<[String]> =
        Arc::from(["happy".to_owned(), "sad".to_owned(), "neutral".to_owned()]);

    // from_label resolves the label to its dictionary index.
    let sad = DuckEnum::from_label(dict.clone(), "sad").map_err(Error::ConversionError)?;
    show("from_label(\"sad\").index()", sad.index());
    assert_eq!(sad.index(), 1);
    assert_eq!(sad.label(), "sad");

    // new selects a member directly by index.
    let neutral = DuckEnum::new(dict.clone(), 2).map_err(Error::ConversionError)?;
    show("new(dict, 2).label()", neutral.label());
    assert_eq!(neutral.label(), "neutral");

    // Equality is by (dictionary, index).
    assert_eq!(
        DuckEnum::from_label(dict.clone(), "neutral").map_err(Error::ConversionError)?,
        neutral
    );

    section("bind a DuckEnum back into an ENUM column");
    // Writing a DuckEnum reproduces a real ENUM value (round-trips through the appender).
    conn.execute_batch("CREATE TABLE moods2 (m mood)")?;
    {
        let mut app = conn.appender("moods2", "main")?;
        app.append(&mut DuckValue::Enum(neutral.clone()))?;
        app.save()?;
    }
    let row = conn.execute("SELECT m FROM moods2")?.next().expect("one row")?;
    match row.get("m") {
        Some(DuckValue::Enum(e)) => assert_eq!(e.label(), "neutral"),
        other => panic!("expected Enum, got {other:?}"),
    }

    println!("\nenum read, construction, and write-back verified");
    Ok(())
}
