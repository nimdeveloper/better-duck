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
            CE::Engine(engine) => {
                let kind = database_error_kind(engine.kind);
                let message = engine.message.unwrap_or_else(|| engine.kind.to_string());
                DE::DatabaseError(kind, Box::new(message))
            },
            CE::DuckDBFailure(_, msg) => {
                let msg = msg.unwrap_or_else(|| "duckdb error".to_owned());
                DE::DatabaseError(K::Unknown, Box::new(msg))
            },
            other => DE::DatabaseError(K::Unknown, Box::new(format!("{other}"))),
        }
    }
}

/// Maps DuckDB's own error classification onto Diesel's.
///
/// Driven entirely by [`EngineErrorKind`], never by inspecting the message text —
/// message formats are not a stable interface and differ across DuckDB releases.
///
/// Most kinds deliberately stay [`DatabaseErrorKind::Unknown`]. In particular
/// `Constraint` is *not* mapped to `UniqueViolation`: DuckDB reports a single
/// `DUCKDB_ERROR_CONSTRAINT` type for unique, foreign-key, not-null and check
/// violations alike, so picking one would be a guess dressed up as a fact. A
/// caller that needs the distinction still has the full message.
fn database_error_kind(
    kind: better_duck_core::error::EngineErrorKind
) -> diesel::result::DatabaseErrorKind {
    use better_duck_core::error::EngineErrorKind as EK;
    use diesel::result::DatabaseErrorKind as K;

    match kind {
        // A transaction conflict is what Diesel means by a serialization failure:
        // the caller should retry the transaction.
        EK::Serialization | EK::Transaction => K::SerializationFailure,
        // DuckDB raises CONNECTION when the connection is no longer usable.
        EK::Connection => K::ClosedConnection,
        _ => K::Unknown,
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

    #[test]
    fn engine_errors_map_by_kind_not_by_message_text() {
        use better_duck_core::error::{EngineError, EngineErrorKind as EK};

        let classify = |kind: EK, text: &str| -> DatabaseErrorKind {
            let engine = EngineError { kind, message: Some(text.to_owned()) };
            match DieselError::from(DuckDbError::new(CoreError::Engine(engine))) {
                DieselError::DatabaseError(kind, _) => kind,
                other => panic!("expected a database error, got {other:?}"),
            }
        };

        // A message that *looks* like a unique violation must not change the kind:
        // classification comes from DuckDB's typed error, never from the text.
        assert!(matches!(
            classify(EK::Constraint, "Duplicate key violates unique constraint"),
            DatabaseErrorKind::Unknown
        ));
        assert!(matches!(
            classify(EK::Serialization, "write-write conflict"),
            DatabaseErrorKind::SerializationFailure
        ));
        assert!(matches!(
            classify(EK::Transaction, "conflict"),
            DatabaseErrorKind::SerializationFailure
        ));
        assert!(matches!(
            classify(EK::Connection, "connection closed"),
            DatabaseErrorKind::ClosedConnection
        ));
        // An error type newer than this build still maps, rather than panicking.
        assert!(matches!(
            classify(EK::Unknown(9_999), "from the future"),
            DatabaseErrorKind::Unknown
        ));
    }

    #[test]
    fn engine_error_without_message_falls_back_to_the_kind() {
        use better_duck_core::error::{EngineError, EngineErrorKind as EK};

        let engine = EngineError { kind: EK::Catalog, message: None };
        assert_eq!(message(DuckDbError::new(CoreError::Engine(engine)).into()), "Catalog");

        let engine = EngineError::unavailable(None);
        assert_eq!(
            message(DuckDbError::new(CoreError::Engine(engine)).into()),
            "unclassified engine error"
        );
    }
}
