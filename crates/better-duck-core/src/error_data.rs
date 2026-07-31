//! [`ErrorData`] — an owned, self-describing DuckDB error
//! (`duckdb_error_data`), independent of any `duckdb_result` or `Error`.

use std::ffi::{CStr, CString};

use crate::{
    error::{DuckDbErrorType, Result},
    ffi::{
        self, duckdb_create_error_data, duckdb_destroy_error_data, duckdb_error_data,
        duckdb_error_data_error_type, duckdb_error_data_has_error, duckdb_error_data_message,
    },
};

/// An owned `duckdb_error_data` handle: a typed error category plus message,
/// independent of any query result. DuckDB returns this shape from a growing
/// set of newer C API functions (e.g. appender error introspection, Arrow
/// conversion); this wrapper also lets callers construct one directly via
/// [`ErrorData::new`].
pub struct ErrorData {
    inner: duckdb_error_data,
}

impl ErrorData {
    /// Builds a new `ErrorData` from an error type and message.
    ///
    /// # Errors
    ///
    /// Returns an error if `message` contains an interior NUL byte.
    pub fn new(
        error_type: DuckDbErrorType,
        message: &str,
    ) -> Result<Self> {
        let c_message = CString::new(message)?;
        // SAFETY: `c_message` is a valid, NUL-terminated C string for the
        // duration of this call. The returned handle is newly allocated and
        // owned by `self`.
        let inner = unsafe { duckdb_create_error_data(error_type.to_raw(), c_message.as_ptr()) };
        Ok(Self { inner })
    }

    /// The broad category of this error.
    pub fn error_type(&self) -> Option<DuckDbErrorType> {
        // SAFETY: `self.inner` is a valid, non-destroyed error data handle
        // for the lifetime of `self`.
        DuckDbErrorType::from_raw(unsafe { duckdb_error_data_error_type(self.inner) })
    }

    /// The error message. Never allocates — borrows DuckDB-owned memory tied
    /// to `self`.
    pub fn message(&self) -> &str {
        // SAFETY: `self.inner` is valid for the lifetime of `self`;
        // `duckdb_error_data_message` returns a pointer into memory owned by
        // `self.inner`, not freed until `self` is dropped.
        let raw = unsafe { duckdb_error_data_message(self.inner) };
        // SAFETY: DuckDB guarantees a null-terminated, valid-UTF-8 message.
        unsafe { CStr::from_ptr(raw) }.to_str().unwrap_or("<invalid UTF-8 in error message>")
    }

    /// Whether this `ErrorData` actually represents an error.
    pub fn has_error(&self) -> bool {
        // SAFETY: `self.inner` is valid for the lifetime of `self`.
        unsafe { duckdb_error_data_has_error(self.inner) }
    }
}

impl Drop for ErrorData {
    fn drop(&mut self) {
        // SAFETY: `self.inner` was created by `duckdb_create_error_data` (or
        // handed to us already owned via `from_raw`) and destroyed exactly
        // once, here.
        unsafe { duckdb_destroy_error_data(&mut self.inner) };
    }
}

impl DuckDbErrorType {
    fn to_raw(self) -> ffi::duckdb_error_type {
        use crate::ffi::*;
        #[allow(non_upper_case_globals)]
        match self {
            Self::Invalid => duckdb_error_type_DUCKDB_ERROR_INVALID,
            Self::OutOfRange => duckdb_error_type_DUCKDB_ERROR_OUT_OF_RANGE,
            Self::Conversion => duckdb_error_type_DUCKDB_ERROR_CONVERSION,
            Self::UnknownType => duckdb_error_type_DUCKDB_ERROR_UNKNOWN_TYPE,
            Self::Decimal => duckdb_error_type_DUCKDB_ERROR_DECIMAL,
            Self::MismatchType => duckdb_error_type_DUCKDB_ERROR_MISMATCH_TYPE,
            Self::DivideByZero => duckdb_error_type_DUCKDB_ERROR_DIVIDE_BY_ZERO,
            Self::ObjectSize => duckdb_error_type_DUCKDB_ERROR_OBJECT_SIZE,
            Self::InvalidType => duckdb_error_type_DUCKDB_ERROR_INVALID_TYPE,
            Self::Serialization => duckdb_error_type_DUCKDB_ERROR_SERIALIZATION,
            Self::Transaction => duckdb_error_type_DUCKDB_ERROR_TRANSACTION,
            Self::NotImplemented => duckdb_error_type_DUCKDB_ERROR_NOT_IMPLEMENTED,
            Self::Expression => duckdb_error_type_DUCKDB_ERROR_EXPRESSION,
            Self::Catalog => duckdb_error_type_DUCKDB_ERROR_CATALOG,
            Self::Parser => duckdb_error_type_DUCKDB_ERROR_PARSER,
            Self::Planner => duckdb_error_type_DUCKDB_ERROR_PLANNER,
            Self::Scheduler => duckdb_error_type_DUCKDB_ERROR_SCHEDULER,
            Self::Executor => duckdb_error_type_DUCKDB_ERROR_EXECUTOR,
            Self::Constraint => duckdb_error_type_DUCKDB_ERROR_CONSTRAINT,
            Self::Index => duckdb_error_type_DUCKDB_ERROR_INDEX,
            Self::Stat => duckdb_error_type_DUCKDB_ERROR_STAT,
            Self::Connection => duckdb_error_type_DUCKDB_ERROR_CONNECTION,
            Self::Syntax => duckdb_error_type_DUCKDB_ERROR_SYNTAX,
            Self::Settings => duckdb_error_type_DUCKDB_ERROR_SETTINGS,
            Self::Binder => duckdb_error_type_DUCKDB_ERROR_BINDER,
            Self::Network => duckdb_error_type_DUCKDB_ERROR_NETWORK,
            Self::Optimizer => duckdb_error_type_DUCKDB_ERROR_OPTIMIZER,
            Self::NullPointer => duckdb_error_type_DUCKDB_ERROR_NULL_POINTER,
            Self::Io => duckdb_error_type_DUCKDB_ERROR_IO,
            Self::Interrupt => duckdb_error_type_DUCKDB_ERROR_INTERRUPT,
            Self::Fatal => duckdb_error_type_DUCKDB_ERROR_FATAL,
            Self::Internal => duckdb_error_type_DUCKDB_ERROR_INTERNAL,
            Self::InvalidInput => duckdb_error_type_DUCKDB_ERROR_INVALID_INPUT,
            Self::OutOfMemory => duckdb_error_type_DUCKDB_ERROR_OUT_OF_MEMORY,
            Self::Permission => duckdb_error_type_DUCKDB_ERROR_PERMISSION,
            Self::ParameterNotResolved => duckdb_error_type_DUCKDB_ERROR_PARAMETER_NOT_RESOLVED,
            Self::ParameterNotAllowed => duckdb_error_type_DUCKDB_ERROR_PARAMETER_NOT_ALLOWED,
            Self::Dependency => duckdb_error_type_DUCKDB_ERROR_DEPENDENCY,
            Self::Http => duckdb_error_type_DUCKDB_ERROR_HTTP,
            Self::MissingExtension => duckdb_error_type_DUCKDB_ERROR_MISSING_EXTENSION,
            Self::Autoload => duckdb_error_type_DUCKDB_ERROR_AUTOLOAD,
            Self::Sequence => duckdb_error_type_DUCKDB_ERROR_SEQUENCE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_round_trips_type_and_message() {
        let err = ErrorData::new(DuckDbErrorType::Constraint, "unique violation").unwrap();
        assert_eq!(err.error_type(), Some(DuckDbErrorType::Constraint));
        assert_eq!(err.message(), "unique violation");
        assert!(err.has_error());
    }

    #[test]
    fn new_rejects_a_message_with_an_interior_nul_byte() {
        let err = ErrorData::new(DuckDbErrorType::Invalid, "bad\0message");
        assert!(err.is_err());
    }

    #[test]
    fn every_error_type_variant_round_trips_through_to_raw_and_from_raw() {
        let variants = [
            DuckDbErrorType::Invalid,
            DuckDbErrorType::OutOfRange,
            DuckDbErrorType::Conversion,
            DuckDbErrorType::UnknownType,
            DuckDbErrorType::Decimal,
            DuckDbErrorType::MismatchType,
            DuckDbErrorType::DivideByZero,
            DuckDbErrorType::ObjectSize,
            DuckDbErrorType::InvalidType,
            DuckDbErrorType::Serialization,
            DuckDbErrorType::Transaction,
            DuckDbErrorType::NotImplemented,
            DuckDbErrorType::Expression,
            DuckDbErrorType::Catalog,
            DuckDbErrorType::Parser,
            DuckDbErrorType::Planner,
            DuckDbErrorType::Scheduler,
            DuckDbErrorType::Executor,
            DuckDbErrorType::Constraint,
            DuckDbErrorType::Index,
            DuckDbErrorType::Stat,
            DuckDbErrorType::Connection,
            DuckDbErrorType::Syntax,
            DuckDbErrorType::Settings,
            DuckDbErrorType::Binder,
            DuckDbErrorType::Network,
            DuckDbErrorType::Optimizer,
            DuckDbErrorType::NullPointer,
            DuckDbErrorType::Io,
            DuckDbErrorType::Interrupt,
            DuckDbErrorType::Fatal,
            DuckDbErrorType::Internal,
            DuckDbErrorType::InvalidInput,
            DuckDbErrorType::OutOfMemory,
            DuckDbErrorType::Permission,
            DuckDbErrorType::ParameterNotResolved,
            DuckDbErrorType::ParameterNotAllowed,
            DuckDbErrorType::Dependency,
            DuckDbErrorType::Http,
            DuckDbErrorType::MissingExtension,
            DuckDbErrorType::Autoload,
            DuckDbErrorType::Sequence,
        ];
        for variant in variants {
            let err = ErrorData::new(variant, "x").unwrap();
            assert_eq!(err.error_type(), Some(variant), "round-trip failed for {variant:?}");
        }
    }
}
