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
