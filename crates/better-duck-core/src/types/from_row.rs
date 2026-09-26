//! Deserialize a query row into a Rust type (`FromRow`) — the read-direction
//! counterpart to the `#[derive(FromRow)]` macro.

use crate::error::DuckDBConversionError;
use crate::raw::row::DuckRow;
use crate::result_set::ResultSet;

/// Builds `Self` from a single query [`DuckRow`], reading each field by column
/// name through [`FromDuckValue`](crate::types::from_value::FromDuckValue).
///
/// Implement it by hand, or derive it with `#[derive(FromRow)]` (the `derive`
/// feature). A whole result set is deserialized with
/// [`ResultSet::to_structs`](ResultSet::to_structs).
pub trait FromRow: Sized {
    /// Reads one row into `Self`.
    ///
    /// # Errors
    ///
    /// Returns a [`DuckDBConversionError`] if a required column is missing, a
    /// value has the wrong type, or a `NULL` is read into a non-`Option` field.
    fn from_row(row: &DuckRow) -> Result<Self, DuckDBConversionError>;
}

impl ResultSet {
    /// Deserializes every row of this result set into `T` via [`FromRow`].
    ///
    /// # Errors
    ///
    /// Returns the first row's [`DuckDBConversionError`], if any.
    pub fn to_structs<T: FromRow>(&self) -> Result<Vec<T>, DuckDBConversionError> {
        self.rows().iter().map(T::from_row).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Connection;
    use crate::types::from_value::FromDuckValue;

    struct Point {
        x: i32,
        y: Option<i32>,
    }

    impl FromRow for Point {
        fn from_row(row: &DuckRow) -> Result<Self, DuckDBConversionError> {
            let read = |col: &str| {
                row.get(col).ok_or_else(|| {
                    DuckDBConversionError::ConversionError(format!("missing column {col:?}"))
                })
            };
            Ok(Point {
                x: i32::from_duck_value(read("x")?)?,
                y: Option::<i32>::from_duck_value(read("y")?)?,
            })
        }
    }

    #[test]
    fn to_structs_reads_every_row() {
        let mut conn = Connection::open_in_memory().unwrap();
        let result = conn
            .execute("SELECT * FROM (VALUES (1, 10), (2, NULL)) AS t(x, y)")
            .unwrap()
            .materialize()
            .unwrap();
        let points: Vec<Point> = result.to_structs().unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].x, 1);
        assert_eq!(points[0].y, Some(10));
        assert_eq!(points[1].x, 2);
        assert_eq!(points[1].y, None);
    }

    #[test]
    fn to_structs_propagates_a_conversion_error() {
        let mut conn = Connection::open_in_memory().unwrap();
        // Column `y` is text here, so reading it as i32 fails — surfaced by `to_structs`.
        let result = conn.execute("SELECT 1 AS x, 'oops' AS y").unwrap().materialize().unwrap();
        assert!(result.to_structs::<Point>().is_err());
    }

    #[test]
    fn to_structs_on_empty_result_is_empty() {
        let mut conn = Connection::open_in_memory().unwrap();
        let result =
            conn.execute("SELECT 1 AS x, 2 AS y WHERE 1 = 0").unwrap().materialize().unwrap();
        let points: Vec<Point> = result.to_structs().unwrap();
        assert!(points.is_empty());
    }
}
