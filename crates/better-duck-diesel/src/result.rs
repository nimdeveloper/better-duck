//! Error conversion between `better_duck_core` errors and Diesel's error types.

use std::fmt;

/// Wraps a [`better_duck_core::error::Error`] for conversion into a Diesel error.
pub struct DuckDbError {
    orig: better_duck_core::error::Error,
}

impl DuckDbError {
    /// Creates a new [`DuckDbError`] wrapping a core error.
    pub fn new(e: better_duck_core::error::Error) -> Self {
        DuckDbError { orig: e }
    }
}

impl fmt::Display for DuckDbError {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        self.orig.fmt(f)
    }
}

impl fmt::Debug for DuckDbError {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(f, "DuckDbError({:?})", self.orig)
    }
}

impl From<DuckDbError> for diesel::result::Error {
    fn from(e: DuckDbError) -> Self {
        use better_duck_core::error::Error as CE;
        use diesel::result::{DatabaseErrorKind as K, Error as DE};

        match e.orig {
            CE::QueryReturnedNoRows => DE::NotFound,
            CE::NulError(n) => DE::InvalidCString(n),
            CE::Utf8Error(u) => DE::SerializationError(Box::new(u)),
            CE::ToSqlConversionFailure(b) => DE::SerializationError(b),
            CE::InvalidColumnIndex(i) => {
                DE::DeserializationError(format!("invalid column index {i}").into())
            },
            CE::InvalidColumnName(n) => {
                DE::DeserializationError(format!("unknown column '{n}'").into())
            },
            CE::DuckDBFailure(_, msg) => {
                let msg = msg.unwrap_or_else(|| "duckdb error".to_owned());
                DE::DatabaseError(K::Unknown, Box::new(msg))
            },
            other => DE::DatabaseError(K::Unknown, Box::new(format!("{other}"))),
        }
    }
}
