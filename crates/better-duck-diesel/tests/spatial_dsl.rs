#![allow(missing_docs)]
#![cfg(feature = "spatial")]

//! DSL tests for the DuckDB `spatial` extension. They load the extension first
//! and skip cleanly when it is unavailable (e.g. offline), so they verify where
//! spatial is present without failing CI.

use better_duck_diesel::spatial::{ensure_loaded, st_as_text, st_point};
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;

#[test]
fn st_point_and_as_text_via_dsl() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    if ensure_loaded(&mut conn).is_err() {
        eprintln!("skipping st_point_and_as_text_via_dsl: `spatial` extension unavailable");
        return;
    }

    // ST_AsText(ST_Point(1, 2)) — geometries flow as WKB (Binary) between the calls.
    let text: String =
        diesel::select(st_as_text(st_point(1.0, 2.0))).get_result(&mut conn).unwrap();
    assert_eq!(text, "POINT (1 2)");
}
