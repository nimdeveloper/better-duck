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

#[cfg(test)]
mod tests {
    use super::*;
    use better_duck_core::{
        error::Error as CoreError,
        ffi::{DuckDBError, Error as FfiError},
    };
    use diesel::result::{DatabaseErrorKind, Error as DieselError};
    use std::{ffi::CString, str};

    fn utf8_error() -> str::Utf8Error {
        let bytes = vec![0x80];
        str::from_utf8(&bytes).expect_err("test input must be invalid UTF-8")
    }

    fn message(error: DieselError) -> String {
        match error {
            DieselError::DatabaseError(DatabaseErrorKind::Unknown, info) => info.message().into(),
            other => panic!("expected unknown database error, got {other:?}"),
        }
    }

    #[test]
    fn display_and_debug_delegate_to_the_core_error() {
        let wrapped = DuckDbError::new(CoreError::InvalidColumnIndex(7));
        assert_eq!(wrapped.to_string(), "Invalid column index: 7");
        assert!(format!("{wrapped:?}").contains("InvalidColumnIndex(7)"));
    }

    #[test]
    fn maps_not_found_and_invalid_c_string() {
        assert!(matches!(
            DieselError::from(DuckDbError::new(CoreError::QueryReturnedNoRows)),
            DieselError::NotFound
        ));

        let nul = CString::new(b"a\0b".to_vec()).unwrap_err();
        assert!(matches!(
            DieselError::from(DuckDbError::new(CoreError::NulError(nul))),
            DieselError::InvalidCString(error) if error.nul_position() == 1
        ));
    }

    #[test]
    fn maps_serialization_errors() {
        assert!(matches!(
            DieselError::from(DuckDbError::new(CoreError::Utf8Error(utf8_error()))),
            DieselError::SerializationError(error) if error.to_string().contains("utf-8")
        ));

        let source = std::io::Error::other("cannot serialize");
        assert!(matches!(
            DieselError::from(DuckDbError::new(CoreError::ToSqlConversionFailure(Box::new(source)))),
            DieselError::SerializationError(error) if error.to_string() == "cannot serialize"
        ));
    }

    #[test]
    fn maps_column_errors_to_deserialization_errors() {
        assert!(matches!(
            DieselError::from(DuckDbError::new(CoreError::InvalidColumnIndex(4))),
            DieselError::DeserializationError(error) if error.to_string() == "invalid column index 4"
        ));
        assert!(matches!(
            DieselError::from(DuckDbError::new(CoreError::InvalidColumnName("total".into()))),
            DieselError::DeserializationError(error) if error.to_string() == "unknown column 'total'"
        ));
    }

    #[test]
    fn maps_duckdb_failures_with_and_without_context() {
        let with_context =
            CoreError::DuckDBFailure(FfiError::new(DuckDBError), Some("constraint failed".into()));
        assert_eq!(message(DuckDbError::new(with_context).into()), "constraint failed");

        let without_context = CoreError::DuckDBFailure(FfiError::new(DuckDBError), None);
        assert_eq!(message(DuckDbError::new(without_context).into()), "duckdb error");
    }

    #[test]
    fn maps_other_core_errors_to_unknown_database_errors() {
        assert_eq!(
            message(DuckDbError::new(CoreError::InvalidParameterCount(1, 2)).into()),
            "Wrong number of parameters passed to query. Got 1, needed 2"
        );
    }
}
