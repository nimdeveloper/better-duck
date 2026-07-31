#![allow(dead_code)]
// Direct copy from DuckDB

use crate::ffi::{duckdb_type, Error as FFIError};
use std::{error, fmt, path::PathBuf, result, str};

/// A DuckDB error's broad category (`duckdb_error_type`) — lets callers match
/// on *why* a query failed (e.g. a constraint violation vs. a syntax error)
/// without string-matching the error message. Read via
/// [`Error::error_type`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DuckDbErrorType {
    /// Generic/unspecified error.
    Invalid,
    /// A value fell outside the range its type or column can represent.
    OutOfRange,
    /// A value could not be converted between types.
    Conversion,
    /// An unrecognized type was encountered.
    UnknownType,
    /// A DECIMAL-specific error (e.g. width/scale overflow).
    Decimal,
    /// Two values or expressions had incompatible types.
    MismatchType,
    /// A division (or modulo) by zero was attempted.
    DivideByZero,
    /// A value or object exceeded a size limit.
    ObjectSize,
    /// A type was used in a context it doesn't support.
    InvalidType,
    /// Serialization or deserialization of internal state failed.
    Serialization,
    /// A transaction could not be started, committed, or continued.
    Transaction,
    /// The requested feature or operation isn't implemented.
    NotImplemented,
    /// A SQL expression was malformed or invalid.
    Expression,
    /// A catalog lookup failed (unknown table, schema, column, function, …).
    Catalog,
    /// SQL parsing failed.
    Parser,
    /// Query planning failed.
    Planner,
    /// Task scheduling failed.
    Scheduler,
    /// Query execution failed at runtime.
    Executor,
    /// A constraint (e.g. `NOT NULL`, `UNIQUE`, `CHECK`) was violated.
    Constraint,
    /// An index operation failed.
    Index,
    /// Statistics collection or use failed.
    Stat,
    /// The connection is in an invalid state for the attempted operation.
    Connection,
    /// The SQL had a syntax error.
    Syntax,
    /// A configuration/settings value was invalid.
    Settings,
    /// Query binding (name/type resolution) failed.
    Binder,
    /// A network operation failed.
    Network,
    /// Query optimization failed.
    Optimizer,
    /// An unexpected null pointer was encountered internally.
    NullPointer,
    /// An I/O operation failed.
    Io,
    /// The operation was interrupted (see [`crate::interrupt::InterruptHandle`]).
    Interrupt,
    /// An unrecoverable internal error occurred.
    Fatal,
    /// An internal invariant was violated — likely a DuckDB bug.
    Internal,
    /// A caller-supplied input was invalid.
    InvalidInput,
    /// Memory allocation failed.
    OutOfMemory,
    /// The operation was not permitted.
    Permission,
    /// A query parameter could not be resolved to a value.
    ParameterNotResolved,
    /// A query parameter was used somewhere it isn't allowed.
    ParameterNotAllowed,
    /// A dependency (e.g. an extension) required by the operation is missing.
    Dependency,
    /// An HTTP request failed.
    Http,
    /// A required extension is not loaded.
    MissingExtension,
    /// Automatic extension loading failed.
    Autoload,
    /// A sequence operation (e.g. `nextval`) failed.
    Sequence,
}

impl DuckDbErrorType {
    /// Converts a raw `duckdb_error_type` value, or `None` if it doesn't
    /// match any variant known to this build (e.g. a newer DuckDB version's
    /// error type this crate hasn't been updated for).
    pub(crate) fn from_raw(raw: crate::ffi::duckdb_error_type) -> Option<Self> {
        use crate::ffi::*;
        #[allow(non_upper_case_globals)]
        Some(match raw {
            duckdb_error_type_DUCKDB_ERROR_INVALID => Self::Invalid,
            duckdb_error_type_DUCKDB_ERROR_OUT_OF_RANGE => Self::OutOfRange,
            duckdb_error_type_DUCKDB_ERROR_CONVERSION => Self::Conversion,
            duckdb_error_type_DUCKDB_ERROR_UNKNOWN_TYPE => Self::UnknownType,
            duckdb_error_type_DUCKDB_ERROR_DECIMAL => Self::Decimal,
            duckdb_error_type_DUCKDB_ERROR_MISMATCH_TYPE => Self::MismatchType,
            duckdb_error_type_DUCKDB_ERROR_DIVIDE_BY_ZERO => Self::DivideByZero,
            duckdb_error_type_DUCKDB_ERROR_OBJECT_SIZE => Self::ObjectSize,
            duckdb_error_type_DUCKDB_ERROR_INVALID_TYPE => Self::InvalidType,
            duckdb_error_type_DUCKDB_ERROR_SERIALIZATION => Self::Serialization,
            duckdb_error_type_DUCKDB_ERROR_TRANSACTION => Self::Transaction,
            duckdb_error_type_DUCKDB_ERROR_NOT_IMPLEMENTED => Self::NotImplemented,
            duckdb_error_type_DUCKDB_ERROR_EXPRESSION => Self::Expression,
            duckdb_error_type_DUCKDB_ERROR_CATALOG => Self::Catalog,
            duckdb_error_type_DUCKDB_ERROR_PARSER => Self::Parser,
            duckdb_error_type_DUCKDB_ERROR_PLANNER => Self::Planner,
            duckdb_error_type_DUCKDB_ERROR_SCHEDULER => Self::Scheduler,
            duckdb_error_type_DUCKDB_ERROR_EXECUTOR => Self::Executor,
            duckdb_error_type_DUCKDB_ERROR_CONSTRAINT => Self::Constraint,
            duckdb_error_type_DUCKDB_ERROR_INDEX => Self::Index,
            duckdb_error_type_DUCKDB_ERROR_STAT => Self::Stat,
            duckdb_error_type_DUCKDB_ERROR_CONNECTION => Self::Connection,
            duckdb_error_type_DUCKDB_ERROR_SYNTAX => Self::Syntax,
            duckdb_error_type_DUCKDB_ERROR_SETTINGS => Self::Settings,
            duckdb_error_type_DUCKDB_ERROR_BINDER => Self::Binder,
            duckdb_error_type_DUCKDB_ERROR_NETWORK => Self::Network,
            duckdb_error_type_DUCKDB_ERROR_OPTIMIZER => Self::Optimizer,
            duckdb_error_type_DUCKDB_ERROR_NULL_POINTER => Self::NullPointer,
            duckdb_error_type_DUCKDB_ERROR_IO => Self::Io,
            duckdb_error_type_DUCKDB_ERROR_INTERRUPT => Self::Interrupt,
            duckdb_error_type_DUCKDB_ERROR_FATAL => Self::Fatal,
            duckdb_error_type_DUCKDB_ERROR_INTERNAL => Self::Internal,
            duckdb_error_type_DUCKDB_ERROR_INVALID_INPUT => Self::InvalidInput,
            duckdb_error_type_DUCKDB_ERROR_OUT_OF_MEMORY => Self::OutOfMemory,
            duckdb_error_type_DUCKDB_ERROR_PERMISSION => Self::Permission,
            duckdb_error_type_DUCKDB_ERROR_PARAMETER_NOT_RESOLVED => Self::ParameterNotResolved,
            duckdb_error_type_DUCKDB_ERROR_PARAMETER_NOT_ALLOWED => Self::ParameterNotAllowed,
            duckdb_error_type_DUCKDB_ERROR_DEPENDENCY => Self::Dependency,
            duckdb_error_type_DUCKDB_ERROR_HTTP => Self::Http,
            duckdb_error_type_DUCKDB_ERROR_MISSING_EXTENSION => Self::MissingExtension,
            duckdb_error_type_DUCKDB_ERROR_AUTOLOAD => Self::Autoload,
            duckdb_error_type_DUCKDB_ERROR_SEQUENCE => Self::Sequence,
            _ => return None,
        })
    }
}

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

    /// [`DuckResult::fetch_chunk`](crate::DuckResult::fetch_chunk) was called
    /// on a result that had already started row-by-row iteration (or vice
    /// versa) — the two iteration modes cannot be mixed on one `DuckResult`,
    /// since pulling a chunk mid-row-iteration would silently drop the
    /// partially-consumed chunk. Pick one mode per result.
    MixedIterationMode,
}

/// A typedef of the result returned by many methods.
pub type Result<T, E = Error> = result::Result<T, E>;

impl Error {
    /// Returns this error's DuckDB error category, if it's a
    /// [`Error::DuckDBFailure`] that came from a call site with one
    /// available (e.g. a query result, via `duckdb_result_error_type`) and
    /// the raw value matches a variant this crate knows about.
    ///
    /// `None` for every other `Error` variant, and for a `DuckDBFailure` from
    /// a call site with no `duckdb_result` to ask (e.g. `duckdb_open_ext`
    /// failing before any query ever ran).
    #[must_use]
    pub fn error_type(&self) -> Option<DuckDbErrorType> {
        match self {
            Error::DuckDBFailure(ffi_err, _) => {
                ffi_err.error_type.and_then(DuckDbErrorType::from_raw)
            },
            _ => None,
        }
    }
}

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
            Error::MixedIterationMode => {
                write!(f, "cannot mix row-by-row and chunk-based iteration on the same DuckResult")
            },
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
            Error::BackgroundTaskFailed(_) | Error::Pool(_) | Error::MixedIterationMode => None,
        }
    }
}
