//! The `json` extension: parse, extract from, and measure JSON values.
//!
//! With the `json` feature the extension is statically linked, so the `JSON` type
//! and the `json_*` functions are available with no runtime install. Extracted
//! scalars are cast to a concrete SQL type so they decode into ordinary
//! `DuckValue` variants.

use better_duck_core::connection::Connection;
use better_duck_core::error::Result;
use better_duck_core::types::value::DuckValue;

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    // Parse a JSON document, then render it back to text.
    let row =
        conn.execute("SELECT '{\"a\":1,\"b\":[2,3]}'::JSON::VARCHAR AS j")?.next().expect("row")?;
    let j = row.get("j").expect("column j").clone();
    println!("JSON value -> {j:?}");
    assert!(matches!(j, DuckValue::Text(_)));

    // Extract a scalar field and cast it to INTEGER.
    let row = conn
        .execute("SELECT json_extract('{\"a\":1,\"b\":[2,3]}', '$.a')::INTEGER AS a")?
        .next()
        .expect("row")?;
    let a = row.get("a").expect("column a").clone();
    println!("json_extract $.a -> {a:?}");
    assert_eq!(a, DuckValue::Int(1));

    // Measure the length of a nested array.
    let row = conn
        .execute("SELECT json_array_length('{\"a\":1,\"b\":[2,3]}', '$.b')::INTEGER AS n")?
        .next()
        .expect("row")?;
    let n = row.get("n").expect("column n").clone();
    println!("json_array_length $.b -> {n:?}");
    assert_eq!(n, DuckValue::Int(2));

    println!("\nJSON extension parsed, extracted, and measured values");
    Ok(())
}
