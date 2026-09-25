#![allow(missing_docs)]
#![cfg(feature = "spatial")]

//! DSL tests for the DuckDB `spatial` extension. They load the extension first
//! and skip cleanly when it is unavailable (e.g. offline), so they verify where
//! spatial is present without failing CI.

use better_duck_diesel::spatial::{
    ensure_loaded, st_area, st_as_text, st_distance, st_geom_from_text, st_point,
};
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

#[test]
fn geom_from_text_distance_and_area_via_dsl() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    if ensure_loaded(&mut conn).is_err() {
        eprintln!("skipping geom_from_text_distance_and_area_via_dsl: `spatial` unavailable");
        return;
    }

    // Distance between two points is 5 (3-4-5 triangle).
    let d: f64 = diesel::select(st_distance(st_point(0.0, 0.0), st_point(3.0, 4.0)))
        .get_result(&mut conn)
        .unwrap();
    assert!((d - 5.0).abs() < 1e-9, "distance was {d}");

    // ST_GeomFromText needs a constant WKT argument, so inline it as a SQL literal.
    // A unit square polygon has area 1.
    let wkt = diesel::dsl::sql::<diesel::sql_types::Text>("'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'");
    let a: f64 = diesel::select(st_area(st_geom_from_text(wkt))).get_result(&mut conn).unwrap();
    assert!((a - 1.0).abs() < 1e-9, "area was {a}");
}
