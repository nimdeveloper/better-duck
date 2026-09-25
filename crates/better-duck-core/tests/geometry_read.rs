#![allow(missing_docs)]

//! Reading a DuckDB `GEOMETRY` column (type-id 40, from the `spatial` extension).
//!
//! A GEOMETRY value physically stores its WKB bytes in the `string_t` layout, so
//! it surfaces as a `DuckValue::Blob` of those WKB bytes — previously this errored
//! with "reading DuckDB column type 40 is not yet supported".
//!
//! The test needs the `spatial` extension; it skips cleanly when it can't load
//! (e.g. offline), so it verifies where spatial is available without failing CI.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;

#[test]
fn geometry_column_reads_as_wkb_blob() {
    let mut conn = Connection::open_in_memory().unwrap();
    if conn.ensure_extension("spatial").is_err() {
        eprintln!("skipping geometry_column_reads_as_wkb_blob: `spatial` extension unavailable");
        return;
    }

    // A raw GEOMETRY value (no ST_AsText/ST_AsWKB wrapper) used to error on read.
    let mut result = conn.execute("SELECT ST_Point(1.0, 2.0) AS g").unwrap();
    let row = result.next().unwrap().unwrap();
    match row.get("g") {
        Some(DuckValue::Blob(wkb)) => {
            // WKB for POINT(1 2): a non-empty byte string (endianness flag + type + coords).
            assert!(!wkb.0.is_empty(), "expected non-empty WKB bytes");
        },
        other => panic!("expected GEOMETRY to read as Blob(WKB), got {other:?}"),
    }

    // And the spatial functions themselves work through the normal query path.
    let mut txt = conn.execute("SELECT ST_AsText(ST_Point(1.0, 2.0)) AS t").unwrap();
    assert_eq!(
        txt.next().unwrap().unwrap().get("t"),
        Some(&DuckValue::Text("POINT (1 2)".to_owned()))
    );
}
