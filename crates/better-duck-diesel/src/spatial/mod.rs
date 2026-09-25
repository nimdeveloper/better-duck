//! DuckDB `spatial` extension support for Diesel: a load-check helper plus a
//! selection of `ST_*` functions exposed as typed DSL calls.
//!
//! GEOMETRY values travel as WKB, so geometry arguments/results are typed as
//! [`Binary`](diesel::sql_types::Binary) here; the spatial functions themselves
//! only exist once the extension is loaded, so call [`ensure_loaded`] first.
//!
//! Docs: <https://duckdb.org/docs/current/core_extensions/spatial/functions>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::{Binary, Double, Text};

use crate::result::DuckDbError;
use crate::DuckDbConnection;

/// Ensures the DuckDB `spatial` extension is installed and loaded on `conn`.
///
/// Call this before using any `ST_*` DSL function; a spatial call against a
/// connection without the extension would otherwise fail with a catalog error.
///
/// # Errors
/// Returns a diesel error if the extension cannot be made available (e.g. it is
/// not installed and cannot be downloaded offline).
pub fn ensure_loaded(conn: &mut DuckDbConnection) -> diesel::result::QueryResult<()> {
    conn.inner_mut().ensure_extension("spatial").map_err(|e| DuckDbError::new(e).into())
}

define_sql_function! {
    /// `ST_Point(x, y)` — construct a point geometry (returned as WKB).
    #[sql_name = "ST_Point"]
    fn st_point(x: Double, y: Double) -> Binary;
}
define_sql_function! {
    /// `ST_GeomFromText(wkt)` — parse a WKT string into a geometry (WKB).
    #[sql_name = "ST_GeomFromText"]
    fn st_geom_from_text(wkt: Text) -> Binary;
}
define_sql_function! {
    /// `ST_AsText(geom)` — the WKT representation of a geometry.
    #[sql_name = "ST_AsText"]
    fn st_as_text(geom: Binary) -> Text;
}
define_sql_function! {
    /// `ST_Distance(a, b)` — the distance between two geometries.
    #[sql_name = "ST_Distance"]
    fn st_distance(a: Binary, b: Binary) -> Double;
}
define_sql_function! {
    /// `ST_Area(geom)` — the area of a geometry.
    #[sql_name = "ST_Area"]
    fn st_area(geom: Binary) -> Double;
}
