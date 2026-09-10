#![allow(dead_code)]
// Direct copy from DuckDB

use crate::ffi::{duckdb_type, Error as FFIError};
use std::{error, fmt, path::PathBuf, result, str};

/// Describes why a value conversion from or to DuckDB failed.
#[derive(Debug)]
pub enum DuckDBConversionError {
    /// The DuckDB column type did not match the expected Rust type.
    TypeMismatch {
        /// The type that was expected.
        expected: duckdb_type,
        /// The type that was actually found.
        found: duckdb_type,
    },
    /// A general conversion error with a description.
    ConversionError(String),
    /// A null value was encountered where a non-null value was required.
    NullValue,
    /// The conversion would lose precision (e.g. Decimal scale overflow).
    PrecisionLoss(String),
}

/// Enum listing possible errors from duckdb.
#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
#[non_exhaustive]
pub enum Error {
    /// An error from an underlying DuckDB call.
    DuckDBFailure(FFIError, Option<String>),

    /// Error when the value of a particular column is requested, but it cannot
    /// be converted to the requested Rust type.
    // FromSqlConversionFailure(usize, Type, Box<dyn error::Error + Send + Sync + 'static>),

    /// Error when DuckDB gives us an integral value outside the range of the
    /// requested type (e.g., trying to get the value 1000 into a `u8`).
    /// The associated `usize` is the column index,
    /// and the associated `i64` is the value returned by DuckDB.
    IntegralValueOutOfRange(usize, i128),

    /// Error converting a string to UTF-8.
    Utf8Error(str::Utf8Error),

    /// Error converting a string to a C-compatible string because it contained
    /// an embedded nul.
    NulError(::std::ffi::NulError),

    /// Error when using SQL named parameters and passing a parameter name not
    /// present in the SQL.
    InvalidParameterName(String),

    /// Error converting a file path to a string.
    InvalidPath(PathBuf),

    /// Error returned when an [`execute`](crate::connection::Connection::execute) call
    /// returns rows.
    ExecuteReturnedResults,

    /// Error when a query that was expected to return at least one row did not
    /// return any.
    QueryReturnedNoRows,

    /// Error when the value of a particular column is requested, but the index
    /// is out of range for the statement.
    InvalidColumnIndex(usize),

    /// Error when the value of a named column is requested, but no column
    /// matches the name for the statement.
    InvalidColumnName(String),

    /// Error when the value of a particular column is requested, but the type
    /// of the result in that column cannot be converted to the requested
    /// Rust type.
    // InvalidColumnType(usize, String, Type),

    /// Error when a query that was expected to insert one row did not insert
    /// any or inserted many.
    StatementChangedRows(usize),

    /// Error available for the implementors of the
    /// [`AppendAble`](crate::types::appendable::AppendAble) trait.
    ToSqlConversionFailure(Box<dyn error::Error + Send + Sync + 'static>),

    /// Error when the SQL is not a `SELECT`, is not read-only.
    InvalidQuery,

    /// Error when the SQL contains multiple statements.
    MultipleStatement,

    /// Error when the number of bound parameters does not match the number of
    /// parameters in the query. The first `usize` is how many parameters were
    /// given, the 2nd is how many were expected.
    InvalidParameterCount(usize, usize),

    /// An error occurred while appending a value via the DuckDB appender API.
    AppendError,

    /// A value conversion error.
    ConversionError(DuckDBConversionError),

    /// An unexpected error with no more specific classification.
    #[allow(non_camel_case_types)]
    UNKNOWN(Box<dyn ::std::error::Error + Send + Sync + 'static>),

    /// A background blocking task panicked or was cancelled before it produced a result.
    BackgroundTaskFailed(String),

    /// A connection pool operation failed (checkout timeout, manager error).
    Pool(String),
}

/// A typedef of the result returned by many methods.
pub type Result<T, E = Error> = result::Result<T, E>;

impl PartialEq for Error {
    fn eq(
        &self,
        other: &Error,
    ) -> bool {
        match (self, other) {
            (Error::DuckDBFailure(e1, s1), Error::DuckDBFailure(e2, s2)) => e1 == e2 && s1 == s2,
            (Error::IntegralValueOutOfRange(i1, n1), Error::IntegralValueOutOfRange(i2, n2)) => {
                i1 == i2 && n1 == n2
            },
            (Error::Utf8Error(e1), Error::Utf8Error(e2)) => e1 == e2,
            (Error::NulError(e1), Error::NulError(e2)) => e1 == e2,
            (Error::InvalidParameterName(n1), Error::InvalidParameterName(n2)) => n1 == n2,
            (Error::InvalidPath(p1), Error::InvalidPath(p2)) => p1 == p2,
            (Error::ExecuteReturnedResults, Error::ExecuteReturnedResults) => true,
            (Error::QueryReturnedNoRows, Error::QueryReturnedNoRows) => true,
            (Error::InvalidColumnIndex(i1), Error::InvalidColumnIndex(i2)) => i1 == i2,
            (Error::InvalidColumnName(n1), Error::InvalidColumnName(n2)) => n1 == n2,
            // (Error::InvalidColumnType(i1, n1, t1), Error::InvalidColumnType(i2, n2, t2)) => {
            //     i1 == i2 && t1 == t2 && n1 == n2
            // }
            (Error::StatementChangedRows(n1), Error::StatementChangedRows(n2)) => n1 == n2,
            (Error::InvalidParameterCount(i1, n1), Error::InvalidParameterCount(i2, n2)) => {
                i1 == i2 && n1 == n2
            },
            (..) => false,
        }
    }
}

impl From<str::Utf8Error> for Error {
    #[cold]
    fn from(err: str::Utf8Error) -> Error {
        Error::Utf8Error(err)
    }
}

impl From<::std::ffi::NulError> for Error {
    #[cold]
    fn from(err: ::std::ffi::NulError) -> Error {
        Error::NulError(err)
    }
}

const UNKNOWN_COLUMN: usize = usize::MAX;

/// The conversion isn't precise, but it's convenient to have it
/// to allow use of `get_raw(…).as_…()?` in callbacks that take `Error`.
/// ```rust,ignore
/// impl From<FromSqlError> for Error {
///     #[cold]
///     fn from(err: FromSqlError) -> Error {
///         // The error type requires index and type fields, but they aren't known in this
///         // context.
///         match err {
///             FromSqlError::OutOfRange(val) => Error::IntegralValueOutOfRange(UNKNOWN_COLUMN, val),
///             #[cfg(feature = "uuid")]
///             FromSqlError::InvalidUuidSize(_) => {
///                 Error::FromSqlConversionFailure(UNKNOWN_COLUMN, Type::Blob, Box::new(err))
///             }
///             FromSqlError::Other(source) => {
///                 Error::FromSqlConversionFailure(UNKNOWN_COLUMN, Type::Null, source)
///             }
///             _ => Error::FromSqlConversionFailure(UNKNOWN_COLUMN, Type::Null, Box::new(err)),
///         }
///     }
/// }
/// ```
///
impl fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Error::DuckDBFailure(ref err, None) => err.fmt(f),
            Error::DuckDBFailure(_, Some(ref s)) => write!(f, "{s}"),
            // Error::FromSqlConversionFailure(i, ref t, ref err) => {
            //     if i != UNKNOWN_COLUMN {
            //         write!(f, "Conversion error from type {t} at index: {i}, {err}")
            //     } else {
            //         err.fmt(f)
            //     }
            // }
            Error::IntegralValueOutOfRange(col, val) => {
                if *col != UNKNOWN_COLUMN {
                    write!(f, "Integer {val} out of range at index {col}")
                } else {
                    write!(f, "Integer {val} out of range")
                }
            },
            Error::Utf8Error(ref err) => err.fmt(f),
            Error::NulError(ref err) => err.fmt(f),
            Error::InvalidParameterName(ref name) => write!(f, "Invalid parameter name: {name}"),
            Error::InvalidPath(ref p) => write!(f, "Invalid path: {}", p.to_string_lossy()),
            Error::ExecuteReturnedResults => {
                write!(f, "Execute returned results - did you mean to call query?")
            },
            Error::QueryReturnedNoRows => write!(f, "Query returned no rows"),
            Error::InvalidColumnIndex(i) => write!(f, "Invalid column index: {i}"),
            Error::InvalidColumnName(ref name) => write!(f, "Invalid column name: {name}"),
            // Error::InvalidColumnType(i, ref name, ref t) => {
            //     write!(f, "Invalid column type {t} at index: {i}, name: {name}")
            // }
            // Error::ArrowTypeToDuckdbType(ref name, ref t) => {
            //     write!(f, "Invalid column type {t} , name: {name}")
            // }
            Error::InvalidParameterCount(i1, n1) => {
                write!(f, "Wrong number of parameters passed to query. Got {i1}, needed {n1}")
            },
            Error::StatementChangedRows(i) => write!(f, "Query changed {i} rows"),
            Error::ToSqlConversionFailure(ref err) => err.fmt(f),
            Error::InvalidQuery => write!(f, "Query is not read-only"),
            Error::MultipleStatement => write!(f, "Multiple statements provided"),
            Error::AppendError => write!(f, "Append error"),
            Error::ConversionError(ref err) => match err {
                DuckDBConversionError::TypeMismatch { expected, found } => {
                    write!(f, "Type mismatch: expected {expected}, found {found}")
                },
                DuckDBConversionError::ConversionError(ref msg) => {
                    write!(f, "Conversion error: {msg}")
                },
                DuckDBConversionError::NullValue => write!(f, "Null value encountered"),
                DuckDBConversionError::PrecisionLoss(ref msg) => write!(f, "Precision loss: {msg}"),
            },
            Error::UNKNOWN(e) => write!(f, "Unknown error: {e}"),
            Error::BackgroundTaskFailed(ref msg) => write!(f, "Background task failed: {msg}"),
            Error::Pool(ref msg) => write!(f, "Connection pool error: {msg}"),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::DuckDBFailure(ref err, _) => Some(err),
            Error::Utf8Error(ref err) => Some(err),
            Error::NulError(ref err) => Some(err),

            Error::IntegralValueOutOfRange(..)
            | Error::InvalidParameterName(_)
            | Error::ExecuteReturnedResults
            | Error::QueryReturnedNoRows
            | Error::InvalidColumnIndex(_)
            | Error::InvalidColumnName(_)
            // | Error::InvalidColumnType(..)
            | Error::InvalidPath(_)
            | Error::InvalidParameterCount(..)
            | Error::StatementChangedRows(_)
            | Error::InvalidQuery
            | Error::AppendError
            // | Error::ArrowTypeToDuckdbType(..)
            | Error::MultipleStatement
            | Error::ConversionError(_) => None,
            // Error::FromSqlConversionFailure(_, _, ref err)
            Error::ToSqlConversionFailure(ref err) => Some(&**err),
            Error::UNKNOWN(e) => Some(e.as_ref()),
            Error::BackgroundTaskFailed(_) | Error::Pool(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::{DuckDBError, DuckDBSuccess};
    use std::{error::Error as _, ffi::CString, io};

    fn invalid_utf8() -> str::Utf8Error {
        let bytes = vec![0xff];
        str::from_utf8(&bytes).unwrap_err()
    }

    #[test]
    fn equality_covers_comparable_variants_and_rejects_others() {
        assert_eq!(
            Error::DuckDBFailure(FFIError::new(DuckDBError), Some("context".into())),
            Error::DuckDBFailure(FFIError::new(DuckDBError), Some("context".into()))
        );
        assert_ne!(
            Error::DuckDBFailure(FFIError::new(DuckDBSuccess), None),
            Error::DuckDBFailure(FFIError::new(DuckDBError), None)
        );
        assert_eq!(Error::IntegralValueOutOfRange(2, 300), Error::IntegralValueOutOfRange(2, 300));
        assert_ne!(Error::IntegralValueOutOfRange(2, 300), Error::IntegralValueOutOfRange(3, 300));
        assert_eq!(Error::Utf8Error(invalid_utf8()), Error::Utf8Error(invalid_utf8()));
        assert_eq!(
            Error::NulError(CString::new("a\0b").unwrap_err()),
            Error::NulError(CString::new("a\0b").unwrap_err())
        );
        assert_eq!(
            Error::InvalidParameterName("p".into()),
            Error::InvalidParameterName("p".into())
        );
        assert_eq!(
            Error::InvalidPath(PathBuf::from("file.db")),
            Error::InvalidPath(PathBuf::from("file.db"))
        );
        assert_eq!(Error::ExecuteReturnedResults, Error::ExecuteReturnedResults);
        assert_eq!(Error::QueryReturnedNoRows, Error::QueryReturnedNoRows);
        assert_eq!(Error::InvalidColumnIndex(4), Error::InvalidColumnIndex(4));
        assert_eq!(
            Error::InvalidColumnName("value".into()),
            Error::InvalidColumnName("value".into())
        );
        assert_eq!(Error::StatementChangedRows(3), Error::StatementChangedRows(3));
        assert_eq!(Error::InvalidParameterCount(1, 2), Error::InvalidParameterCount(1, 2));
        assert_ne!(
            Error::ToSqlConversionFailure(Box::new(io::Error::other("same"))),
            Error::ToSqlConversionFailure(Box::new(io::Error::other("same")))
        );
        assert_ne!(Error::InvalidQuery, Error::InvalidQuery);
        assert_ne!(Error::MultipleStatement, Error::MultipleStatement);
        assert_ne!(Error::AppendError, Error::AppendError);
        assert_ne!(
            Error::ConversionError(DuckDBConversionError::TypeMismatch {
                expected: crate::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
                found: crate::ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR
            }),
            Error::ConversionError(DuckDBConversionError::TypeMismatch {
                expected: crate::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
                found: crate::ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR
            })
        );
        assert_ne!(
            Error::ConversionError(DuckDBConversionError::NullValue),
            Error::ConversionError(DuckDBConversionError::NullValue)
        );
        assert_ne!(
            Error::BackgroundTaskFailed("same".into()),
            Error::BackgroundTaskFailed("same".into())
        );
        assert_ne!(Error::Pool("same".into()), Error::Pool("same".into()));
        assert_ne!(Error::InvalidQuery, Error::MultipleStatement);
    }

    #[test]
    fn utf8_and_nul_errors_convert_and_compare() {
        let utf8 = invalid_utf8();
        let converted: Error = utf8.into();
        assert_eq!(converted, Error::Utf8Error(utf8));

        let nul = CString::new("a\0b").unwrap_err();
        let expected = CString::new("a\0b").unwrap_err();
        let converted: Error = nul.into();
        assert_eq!(converted, Error::NulError(expected));
    }

    #[test]
    fn display_formats_error_context() {
        assert_eq!(
            Error::IntegralValueOutOfRange(2, 300).to_string(),
            "Integer 300 out of range at index 2"
        );
        assert_eq!(
            Error::IntegralValueOutOfRange(usize::MAX, 300).to_string(),
            "Integer 300 out of range"
        );
        assert_eq!(
            Error::ConversionError(DuckDBConversionError::ConversionError("bad value".into()))
                .to_string(),
            "Conversion error: bad value"
        );
        assert_eq!(
            Error::ConversionError(DuckDBConversionError::NullValue).to_string(),
            "Null value encountered"
        );
        assert_eq!(
            Error::ConversionError(DuckDBConversionError::PrecisionLoss("rounded".into()))
                .to_string(),
            "Precision loss: rounded"
        );
        assert_eq!(Error::AppendError.to_string(), "Append error");
        assert_eq!(Error::InvalidQuery.to_string(), "Query is not read-only");
        assert_eq!(Error::MultipleStatement.to_string(), "Multiple statements provided");
        assert_eq!(
            Error::BackgroundTaskFailed("worker stopped".into()).to_string(),
            "Background task failed: worker stopped"
        );
        assert_eq!(Error::Pool("timed out".into()).to_string(), "Connection pool error: timed out");
        assert_eq!(
            Error::DuckDBFailure(FFIError::new(DuckDBError), Some("query failed".into()))
                .to_string(),
            "query failed"
        );
        assert_eq!(
            Error::DuckDBFailure(FFIError::new(DuckDBError), None).to_string(),
            "DuckDB call failed with result code 1"
        );
    }

    #[test]
    fn sources_are_exposed_only_for_wrapped_errors() {
        let utf8 = Error::Utf8Error(invalid_utf8());
        let nul = Error::NulError(CString::new("a\0b").unwrap_err());
        let ffi = Error::DuckDBFailure(FFIError::new(DuckDBError), None);
        let sql = Error::ToSqlConversionFailure(Box::new(io::Error::other("sql")));
        let unknown = Error::UNKNOWN(Box::new(io::Error::other("unknown")));

        assert!(ffi.source().is_some());
        assert!(utf8.source().is_some());
        assert!(nul.source().is_some());
        assert_eq!(sql.source().unwrap().to_string(), "sql");
        assert_eq!(unknown.source().unwrap().to_string(), "unknown");
        assert!(Error::AppendError.source().is_none());
        assert!(Error::ConversionError(DuckDBConversionError::NullValue).source().is_none());
        assert!(Error::BackgroundTaskFailed("stopped".into()).source().is_none());
        assert!(Error::Pool("timeout".into()).source().is_none());
    }
}
