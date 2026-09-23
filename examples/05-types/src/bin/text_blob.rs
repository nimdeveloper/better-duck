//! Text and binary payloads: `DuckValue::Text` (VARCHAR) and `DuckValue::Blob` (BLOB).
//!
//! `Text` holds an owned `String`; the [`DuckValue::text`] constructor accepts any
//! `Into<String>` (`&str`, `String`, ...). `Blob` wraps a [`Blob`] newtype over
//! `Vec<u8>` — a distinct type from a bare `Vec<u8>` so that a byte vector is not
//! forced to mean BLOB. Both round-trip byte-for-byte, including empty values,
//! Unicode text, and binary data with embedded NUL bytes.

use better_duck_core::connection::Connection;
use better_duck_core::types::blob::Blob;
use better_duck_core::types::value::DuckValue;
use common::{section, show, Result};

/// Binds `dv` as `$1` and reads the single result column back.
fn round_trip(
    conn: &mut Connection,
    mut dv: DuckValue,
) -> Result<DuckValue> {
    let row = conn.execute_with("SELECT $1 AS v", &mut [&mut dv])?.next().expect("one row")?;
    Ok(row.get("v").expect("column v").clone())
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    section("Text — DuckValue::text() builds a VARCHAR value");
    // `text` accepts &str or String; the value is `DuckValue::Text(String)`.
    let hello = round_trip(&mut conn, DuckValue::text("hello world"))?;
    show("text(\"hello world\")", &hello);
    assert_eq!(hello, DuckValue::text("hello world"));
    // Empty and Unicode strings survive unchanged (bytes are copied verbatim).
    assert_eq!(round_trip(&mut conn, DuckValue::text(""))?, DuckValue::text(""));
    let unicode = "Héllo Wörld 🦆";
    let v = round_trip(&mut conn, DuckValue::text(unicode))?;
    show("unicode", &v);
    assert_eq!(v, DuckValue::text(unicode));

    section("Blob — Blob::new(Vec<u8>) builds a BLOB value");
    // `Blob::new` wraps owned bytes; `DuckValue::Blob(Blob)` is the column value.
    let bytes = round_trip(&mut conn, DuckValue::Blob(Blob::new(vec![0xDE, 0xAD, 0xBE, 0xEF])))?;
    show("blob [DE AD BE EF]", &bytes);
    assert_eq!(bytes, DuckValue::Blob(Blob::new(vec![0xDE, 0xAD, 0xBE, 0xEF])));

    // An empty blob is valid and distinct from NULL.
    assert_eq!(
        round_trip(&mut conn, DuckValue::Blob(Blob::new(vec![])))?,
        DuckValue::Blob(Blob::new(vec![]))
    );

    // Binary data with embedded NUL bytes is preserved (BLOB is not NUL-terminated).
    let binary = vec![0x00, 0xFF, 0x00, 0x80, 0x41];
    let v = round_trip(&mut conn, DuckValue::Blob(Blob::new(binary.clone())))?;
    show("blob with NUL bytes", &v);
    assert_eq!(v, DuckValue::Blob(Blob::new(binary)));
    // The `Blob(pub Vec<u8>)` tuple constructor is equivalent to `Blob::new`.
    assert_eq!(Blob(vec![1, 2, 3]), Blob::new(vec![1, 2, 3]));

    println!("\ntext and blob values round-tripped");
    Ok(())
}
