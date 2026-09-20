#![allow(dead_code)]
// Direct copy from DuckDB

use crate::ffi::{duckdb_error_type, duckdb_type, Error as FFIError};
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

/// How DuckDB itself classified an engine error.
///
/// Mirrors `duckdb_error_type`. Marked `#[non_exhaustive]` because DuckDB adds
/// error types across releases: a value this build does not recognise is kept
/// verbatim in [`EngineErrorKind::Unknown`] rather than being flattened into a
/// catch-all, so no information is lost and matching stays forward-compatible.
///
/// [`EngineErrorKind::Unavailable`] is distinct from `Unknown`: it means the
/// DuckDB API in question exposes only a status code and a message, with no
/// typed classification at all. This driver never invents a concrete kind by
/// parsing message text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EngineErrorKind {
    /// `DUCKDB_ERROR_INVALID`
    Invalid,
    /// `DUCKDB_ERROR_OUT_OF_RANGE`
    OutOfRange,
    /// `DUCKDB_ERROR_CONVERSION`
    Conversion,
    /// `DUCKDB_ERROR_UNKNOWN_TYPE`
    UnknownType,
    /// `DUCKDB_ERROR_DECIMAL`
    Decimal,
    /// `DUCKDB_ERROR_MISMATCH_TYPE`
    MismatchType,
    /// `DUCKDB_ERROR_DIVIDE_BY_ZERO`
    DivideByZero,
    /// `DUCKDB_ERROR_OBJECT_SIZE`
    ObjectSize,
    /// `DUCKDB_ERROR_INVALID_TYPE`
    InvalidType,
    /// `DUCKDB_ERROR_SERIALIZATION`
    Serialization,
    /// `DUCKDB_ERROR_TRANSACTION`
    Transaction,
    /// `DUCKDB_ERROR_NOT_IMPLEMENTED`
    NotImplemented,
    /// `DUCKDB_ERROR_EXPRESSION`
    Expression,
    /// `DUCKDB_ERROR_CATALOG`
    Catalog,
    /// `DUCKDB_ERROR_PARSER`
    Parser,
    /// `DUCKDB_ERROR_PLANNER`
    Planner,
    /// `DUCKDB_ERROR_SCHEDULER`
    Scheduler,
    /// `DUCKDB_ERROR_EXECUTOR`
    Executor,
    /// `DUCKDB_ERROR_CONSTRAINT` — DuckDB does not say *which* constraint.
    Constraint,
    /// `DUCKDB_ERROR_INDEX`
    Index,
    /// `DUCKDB_ERROR_STAT`
    Stat,
    /// `DUCKDB_ERROR_CONNECTION`
    Connection,
    /// `DUCKDB_ERROR_SYNTAX`
    Syntax,
    /// `DUCKDB_ERROR_SETTINGS`
    Settings,
    /// `DUCKDB_ERROR_BINDER`
    Binder,
    /// `DUCKDB_ERROR_NETWORK`
    Network,
    /// `DUCKDB_ERROR_OPTIMIZER`
    Optimizer,
    /// `DUCKDB_ERROR_NULL_POINTER`
    NullPointer,
    /// `DUCKDB_ERROR_IO`
    Io,
    /// `DUCKDB_ERROR_INTERRUPT`
    Interrupt,
    /// `DUCKDB_ERROR_FATAL`
    Fatal,
    /// `DUCKDB_ERROR_INTERNAL`
    Internal,
    /// `DUCKDB_ERROR_INVALID_INPUT`
    InvalidInput,
    /// `DUCKDB_ERROR_OUT_OF_MEMORY`
    OutOfMemory,
    /// `DUCKDB_ERROR_PERMISSION`
    Permission,
    /// `DUCKDB_ERROR_PARAMETER_NOT_RESOLVED`
    ParameterNotResolved,
    /// `DUCKDB_ERROR_PARAMETER_NOT_ALLOWED`
    ParameterNotAllowed,
    /// `DUCKDB_ERROR_DEPENDENCY`
    Dependency,
    /// `DUCKDB_ERROR_HTTP`
    Http,
    /// `DUCKDB_ERROR_MISSING_EXTENSION`
    MissingExtension,
    /// `DUCKDB_ERROR_AUTOLOAD`
    Autoload,
    /// `DUCKDB_ERROR_SEQUENCE`
    Sequence,
    /// `DUCKDB_INVALID_CONFIGURATION`
    InvalidConfiguration,
    /// DuckDB reported a classification this build does not know; the raw value
    /// is preserved so nothing is lost across DuckDB upgrades.
    Unknown(duckdb_error_type),
    /// The originating DuckDB API offers no typed classification — only a status
    /// code and a message (prepare, extracted statements, pending, table
    /// description). Not a guess: an explicit absence.
    Unavailable,
}

impl EngineErrorKind {
    /// Every kind that maps 1:1 onto a `duckdb_error_type` this build knows.
    ///
    /// Excludes [`Unknown`](EngineErrorKind::Unknown) and
    /// [`Unavailable`](EngineErrorKind::Unavailable), which have no single raw value.
    pub const KNOWN: &'static [EngineErrorKind] = &[
        EngineErrorKind::Invalid,
        EngineErrorKind::OutOfRange,
        EngineErrorKind::Conversion,
        EngineErrorKind::UnknownType,
        EngineErrorKind::Decimal,
        EngineErrorKind::MismatchType,
        EngineErrorKind::DivideByZero,
        EngineErrorKind::ObjectSize,
        EngineErrorKind::InvalidType,
        EngineErrorKind::Serialization,
        EngineErrorKind::Transaction,
        EngineErrorKind::NotImplemented,
        EngineErrorKind::Expression,
        EngineErrorKind::Catalog,
        EngineErrorKind::Parser,
        EngineErrorKind::Planner,
        EngineErrorKind::Scheduler,
        EngineErrorKind::Executor,
        EngineErrorKind::Constraint,
        EngineErrorKind::Index,
        EngineErrorKind::Stat,
        EngineErrorKind::Connection,
        EngineErrorKind::Syntax,
        EngineErrorKind::Settings,
        EngineErrorKind::Binder,
        EngineErrorKind::Network,
        EngineErrorKind::Optimizer,
        EngineErrorKind::NullPointer,
        EngineErrorKind::Io,
        EngineErrorKind::Interrupt,
        EngineErrorKind::Fatal,
        EngineErrorKind::Internal,
        EngineErrorKind::InvalidInput,
        EngineErrorKind::OutOfMemory,
        EngineErrorKind::Permission,
        EngineErrorKind::ParameterNotResolved,
        EngineErrorKind::ParameterNotAllowed,
        EngineErrorKind::Dependency,
        EngineErrorKind::Http,
        EngineErrorKind::MissingExtension,
        EngineErrorKind::Autoload,
        EngineErrorKind::Sequence,
        EngineErrorKind::InvalidConfiguration,
    ];

    /// Classifies a raw `duckdb_error_type`, preserving unrecognised values.
    #[must_use]
    pub fn from_raw(raw: duckdb_error_type) -> EngineErrorKind {
        use crate::ffi as f;
        match raw {
            f::duckdb_error_type_DUCKDB_ERROR_INVALID => EngineErrorKind::Invalid,
            f::duckdb_error_type_DUCKDB_ERROR_OUT_OF_RANGE => EngineErrorKind::OutOfRange,
            f::duckdb_error_type_DUCKDB_ERROR_CONVERSION => EngineErrorKind::Conversion,
            f::duckdb_error_type_DUCKDB_ERROR_UNKNOWN_TYPE => EngineErrorKind::UnknownType,
            f::duckdb_error_type_DUCKDB_ERROR_DECIMAL => EngineErrorKind::Decimal,
            f::duckdb_error_type_DUCKDB_ERROR_MISMATCH_TYPE => EngineErrorKind::MismatchType,
            f::duckdb_error_type_DUCKDB_ERROR_DIVIDE_BY_ZERO => EngineErrorKind::DivideByZero,
            f::duckdb_error_type_DUCKDB_ERROR_OBJECT_SIZE => EngineErrorKind::ObjectSize,
            f::duckdb_error_type_DUCKDB_ERROR_INVALID_TYPE => EngineErrorKind::InvalidType,
            f::duckdb_error_type_DUCKDB_ERROR_SERIALIZATION => EngineErrorKind::Serialization,
            f::duckdb_error_type_DUCKDB_ERROR_TRANSACTION => EngineErrorKind::Transaction,
            f::duckdb_error_type_DUCKDB_ERROR_NOT_IMPLEMENTED => EngineErrorKind::NotImplemented,
            f::duckdb_error_type_DUCKDB_ERROR_EXPRESSION => EngineErrorKind::Expression,
            f::duckdb_error_type_DUCKDB_ERROR_CATALOG => EngineErrorKind::Catalog,
            f::duckdb_error_type_DUCKDB_ERROR_PARSER => EngineErrorKind::Parser,
            f::duckdb_error_type_DUCKDB_ERROR_PLANNER => EngineErrorKind::Planner,
            f::duckdb_error_type_DUCKDB_ERROR_SCHEDULER => EngineErrorKind::Scheduler,
            f::duckdb_error_type_DUCKDB_ERROR_EXECUTOR => EngineErrorKind::Executor,
            f::duckdb_error_type_DUCKDB_ERROR_CONSTRAINT => EngineErrorKind::Constraint,
            f::duckdb_error_type_DUCKDB_ERROR_INDEX => EngineErrorKind::Index,
            f::duckdb_error_type_DUCKDB_ERROR_STAT => EngineErrorKind::Stat,
            f::duckdb_error_type_DUCKDB_ERROR_CONNECTION => EngineErrorKind::Connection,
            f::duckdb_error_type_DUCKDB_ERROR_SYNTAX => EngineErrorKind::Syntax,
            f::duckdb_error_type_DUCKDB_ERROR_SETTINGS => EngineErrorKind::Settings,
            f::duckdb_error_type_DUCKDB_ERROR_BINDER => EngineErrorKind::Binder,
            f::duckdb_error_type_DUCKDB_ERROR_NETWORK => EngineErrorKind::Network,
            f::duckdb_error_type_DUCKDB_ERROR_OPTIMIZER => EngineErrorKind::Optimizer,
            f::duckdb_error_type_DUCKDB_ERROR_NULL_POINTER => EngineErrorKind::NullPointer,
            f::duckdb_error_type_DUCKDB_ERROR_IO => EngineErrorKind::Io,
            f::duckdb_error_type_DUCKDB_ERROR_INTERRUPT => EngineErrorKind::Interrupt,
            f::duckdb_error_type_DUCKDB_ERROR_FATAL => EngineErrorKind::Fatal,
            f::duckdb_error_type_DUCKDB_ERROR_INTERNAL => EngineErrorKind::Internal,
            f::duckdb_error_type_DUCKDB_ERROR_INVALID_INPUT => EngineErrorKind::InvalidInput,
            f::duckdb_error_type_DUCKDB_ERROR_OUT_OF_MEMORY => EngineErrorKind::OutOfMemory,
            f::duckdb_error_type_DUCKDB_ERROR_PERMISSION => EngineErrorKind::Permission,
            f::duckdb_error_type_DUCKDB_ERROR_PARAMETER_NOT_RESOLVED => {
                EngineErrorKind::ParameterNotResolved
            },
            f::duckdb_error_type_DUCKDB_ERROR_PARAMETER_NOT_ALLOWED => {
                EngineErrorKind::ParameterNotAllowed
            },
            f::duckdb_error_type_DUCKDB_ERROR_DEPENDENCY => EngineErrorKind::Dependency,
            f::duckdb_error_type_DUCKDB_ERROR_HTTP => EngineErrorKind::Http,
            f::duckdb_error_type_DUCKDB_ERROR_MISSING_EXTENSION => {
                EngineErrorKind::MissingExtension
            },
            f::duckdb_error_type_DUCKDB_ERROR_AUTOLOAD => EngineErrorKind::Autoload,
            f::duckdb_error_type_DUCKDB_ERROR_SEQUENCE => EngineErrorKind::Sequence,
            f::duckdb_error_type_DUCKDB_INVALID_CONFIGURATION => {
                EngineErrorKind::InvalidConfiguration
            },
            other => EngineErrorKind::Unknown(other),
        }
    }

    /// Returns the raw `duckdb_error_type` for this kind.
    ///
    /// [`Unavailable`](EngineErrorKind::Unavailable) has no DuckDB counterpart and
    /// maps to `DUCKDB_ERROR_INVALID`, which is what DuckDB itself uses for an
    /// unclassified error.
    #[must_use]
    pub fn to_raw(self) -> duckdb_error_type {
        use crate::ffi as f;
        match self {
            EngineErrorKind::Invalid => f::duckdb_error_type_DUCKDB_ERROR_INVALID,
            EngineErrorKind::OutOfRange => f::duckdb_error_type_DUCKDB_ERROR_OUT_OF_RANGE,
            EngineErrorKind::Conversion => f::duckdb_error_type_DUCKDB_ERROR_CONVERSION,
            EngineErrorKind::UnknownType => f::duckdb_error_type_DUCKDB_ERROR_UNKNOWN_TYPE,
            EngineErrorKind::Decimal => f::duckdb_error_type_DUCKDB_ERROR_DECIMAL,
            EngineErrorKind::MismatchType => f::duckdb_error_type_DUCKDB_ERROR_MISMATCH_TYPE,
            EngineErrorKind::DivideByZero => f::duckdb_error_type_DUCKDB_ERROR_DIVIDE_BY_ZERO,
            EngineErrorKind::ObjectSize => f::duckdb_error_type_DUCKDB_ERROR_OBJECT_SIZE,
            EngineErrorKind::InvalidType => f::duckdb_error_type_DUCKDB_ERROR_INVALID_TYPE,
            EngineErrorKind::Serialization => f::duckdb_error_type_DUCKDB_ERROR_SERIALIZATION,
            EngineErrorKind::Transaction => f::duckdb_error_type_DUCKDB_ERROR_TRANSACTION,
            EngineErrorKind::NotImplemented => f::duckdb_error_type_DUCKDB_ERROR_NOT_IMPLEMENTED,
            EngineErrorKind::Expression => f::duckdb_error_type_DUCKDB_ERROR_EXPRESSION,
            EngineErrorKind::Catalog => f::duckdb_error_type_DUCKDB_ERROR_CATALOG,
            EngineErrorKind::Parser => f::duckdb_error_type_DUCKDB_ERROR_PARSER,
            EngineErrorKind::Planner => f::duckdb_error_type_DUCKDB_ERROR_PLANNER,
            EngineErrorKind::Scheduler => f::duckdb_error_type_DUCKDB_ERROR_SCHEDULER,
            EngineErrorKind::Executor => f::duckdb_error_type_DUCKDB_ERROR_EXECUTOR,
            EngineErrorKind::Constraint => f::duckdb_error_type_DUCKDB_ERROR_CONSTRAINT,
            EngineErrorKind::Index => f::duckdb_error_type_DUCKDB_ERROR_INDEX,
            EngineErrorKind::Stat => f::duckdb_error_type_DUCKDB_ERROR_STAT,
            EngineErrorKind::Connection => f::duckdb_error_type_DUCKDB_ERROR_CONNECTION,
            EngineErrorKind::Syntax => f::duckdb_error_type_DUCKDB_ERROR_SYNTAX,
            EngineErrorKind::Settings => f::duckdb_error_type_DUCKDB_ERROR_SETTINGS,
            EngineErrorKind::Binder => f::duckdb_error_type_DUCKDB_ERROR_BINDER,
            EngineErrorKind::Network => f::duckdb_error_type_DUCKDB_ERROR_NETWORK,
            EngineErrorKind::Optimizer => f::duckdb_error_type_DUCKDB_ERROR_OPTIMIZER,
            EngineErrorKind::NullPointer => f::duckdb_error_type_DUCKDB_ERROR_NULL_POINTER,
            EngineErrorKind::Io => f::duckdb_error_type_DUCKDB_ERROR_IO,
            EngineErrorKind::Interrupt => f::duckdb_error_type_DUCKDB_ERROR_INTERRUPT,
            EngineErrorKind::Fatal => f::duckdb_error_type_DUCKDB_ERROR_FATAL,
            EngineErrorKind::Internal => f::duckdb_error_type_DUCKDB_ERROR_INTERNAL,
            EngineErrorKind::InvalidInput => f::duckdb_error_type_DUCKDB_ERROR_INVALID_INPUT,
            EngineErrorKind::OutOfMemory => f::duckdb_error_type_DUCKDB_ERROR_OUT_OF_MEMORY,
            EngineErrorKind::Permission => f::duckdb_error_type_DUCKDB_ERROR_PERMISSION,
            EngineErrorKind::ParameterNotResolved => {
                f::duckdb_error_type_DUCKDB_ERROR_PARAMETER_NOT_RESOLVED
            },
            EngineErrorKind::ParameterNotAllowed => {
                f::duckdb_error_type_DUCKDB_ERROR_PARAMETER_NOT_ALLOWED
            },
            EngineErrorKind::Dependency => f::duckdb_error_type_DUCKDB_ERROR_DEPENDENCY,
            EngineErrorKind::Http => f::duckdb_error_type_DUCKDB_ERROR_HTTP,
            EngineErrorKind::MissingExtension => {
                f::duckdb_error_type_DUCKDB_ERROR_MISSING_EXTENSION
            },
            EngineErrorKind::Autoload => f::duckdb_error_type_DUCKDB_ERROR_AUTOLOAD,
            EngineErrorKind::Sequence => f::duckdb_error_type_DUCKDB_ERROR_SEQUENCE,
            EngineErrorKind::InvalidConfiguration => {
                f::duckdb_error_type_DUCKDB_INVALID_CONFIGURATION
            },
            EngineErrorKind::Unknown(raw) => raw,
            EngineErrorKind::Unavailable => f::duckdb_error_type_DUCKDB_ERROR_INVALID,
        }
    }
}

impl fmt::Display for EngineErrorKind {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            EngineErrorKind::Unknown(raw) => write!(f, "unknown engine error type {raw}"),
            EngineErrorKind::Unavailable => write!(f, "unclassified engine error"),
            other => write!(f, "{other:?}"),
        }
    }
}

/// A fully owned engine error: DuckDB's own classification plus its message.
///
/// Pure Rust — every field is copied out of DuckDB memory when the error is
/// built, so an `EngineError` stays valid after the handle it came from is
/// destroyed and after the connection is closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineError {
    /// How DuckDB classified the failure.
    pub kind: EngineErrorKind,
    /// DuckDB's message, if it supplied one.
    pub message: Option<String>,
}

impl EngineError {
    /// Builds an engine error with no typed classification available.
    ///
    /// Use for DuckDB APIs that expose only a status code and a message, so the
    /// absence of a kind is explicit rather than guessed.
    #[must_use]
    pub fn unavailable(message: Option<String>) -> EngineError {
        EngineError { kind: EngineErrorKind::Unavailable, message }
    }
}

impl fmt::Display for EngineError {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match &self.message {
            Some(message) => write!(f, "{}: {message}", self.kind),
            None => write!(f, "{}", self.kind),
        }
    }
}

impl error::Error for EngineError {}

/// Enum listing possible errors from duckdb.
#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
#[non_exhaustive]
pub enum Error {
    /// An error from an underlying DuckDB call.
    DuckDBFailure(FFIError, Option<String>),

    /// A typed engine error carrying DuckDB's own classification.
    Engine(EngineError),

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
            (Error::Engine(a), Error::Engine(b)) => a == b,
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
            Error::Engine(ref err) => err.fmt(f),
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
            Error::Engine(ref err) => Some(err),
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
    fn engine_error_kinds_round_trip_and_preserve_unknown_values() {
        // Every known kind survives a raw round trip.
        for kind in EngineErrorKind::KNOWN {
            assert_eq!(
                EngineErrorKind::from_raw(kind.to_raw()),
                *kind,
                "kind {kind:?} did not round-trip"
            );
        }

        // A value this build does not know is kept verbatim rather than flattened.
        let future = EngineErrorKind::from_raw(9_999);
        assert_eq!(future, EngineErrorKind::Unknown(9_999));
        assert_eq!(future.to_raw(), 9_999);
        assert_eq!(future.to_string(), "unknown engine error type 9999");

        // `Unavailable` is an explicit absence, distinct from `Unknown`.
        assert_ne!(EngineErrorKind::Unavailable, EngineErrorKind::Unknown(0));
        assert_eq!(EngineErrorKind::Unavailable.to_string(), "unclassified engine error");
    }

    #[test]
    fn engine_errors_display_and_compare_by_value() {
        let with_message =
            EngineError { kind: EngineErrorKind::Catalog, message: Some("no such table".into()) };
        assert_eq!(with_message.to_string(), "Catalog: no such table");
        assert_eq!(with_message, with_message.clone());

        let without = EngineError { kind: EngineErrorKind::Binder, message: None };
        assert_eq!(without.to_string(), "Binder");

        assert_eq!(EngineError::unavailable(None).kind, EngineErrorKind::Unavailable);

        // The wrapping `Error` delegates Display and exposes the engine error as source.
        let wrapped = Error::Engine(with_message.clone());
        assert_eq!(wrapped.to_string(), "Catalog: no such table");
        assert!(wrapped.source().is_some());
        assert_eq!(wrapped, Error::Engine(with_message));
        assert_ne!(
            Error::Engine(EngineError::unavailable(None)),
            Error::Engine(EngineError { kind: EngineErrorKind::Io, message: None })
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
