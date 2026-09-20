//! RAII wrapper over DuckDB's `duckdb_error_data` handle.
//!
//! `duckdb_error_data` is the only DuckDB error channel that carries a *typed*
//! classification alongside the message. Everything else (prepare, extracted,
//! pending, table description) exposes a bare status plus a string, which is why
//! [`EngineErrorKind::Unavailable`] exists — this driver never guesses a concrete
//! kind by pattern-matching message text.

use std::{ffi::CStr, marker::PhantomData, ptr};

use crate::{
    error::{EngineError, EngineErrorKind},
    ffi::{
        duckdb_destroy_error_data, duckdb_error_data, duckdb_error_data_error_type,
        duckdb_error_data_has_error, duckdb_error_data_message,
    },
};

/// Owns one `duckdb_error_data` handle and destroys it exactly once.
///
/// # Thread safety
///
/// Deliberately `!Send + !Sync` (via the [`PhantomData`] below). DuckDB gives no
/// concurrency guarantee for error data, and the borrowed message pointer is only
/// valid until the handle is destroyed, so the handle must not travel between
/// threads. Every accessor copies out of C memory before that can happen.
pub(crate) struct ErrorData {
    handle: duckdb_error_data,
    /// Pins the handle to one thread: a raw pointer is neither `Send` nor `Sync`.
    _not_thread_safe: PhantomData<*const ()>,
}

impl ErrorData {
    /// Takes ownership of an error-data handle returned by DuckDB.
    ///
    /// Returns `None` for a null handle, so callers cannot accidentally build an
    /// `ErrorData` that would call a destructor on null.
    ///
    /// # Safety
    ///
    /// `handle` must be a live `duckdb_error_data` that ownership is being
    /// transferred for, and must not be destroyed by anyone else.
    #[inline]
    pub(crate) unsafe fn from_raw(handle: duckdb_error_data) -> Option<ErrorData> {
        if handle.is_null() {
            return None;
        }
        Some(ErrorData { handle, _not_thread_safe: PhantomData })
    }

    /// Returns `true` if this error data actually carries an error.
    #[inline]
    pub(crate) fn has_error(&self) -> bool {
        // SAFETY: `self.handle` is non-null (enforced by `from_raw`) and live until Drop.
        unsafe { duckdb_error_data_has_error(self.handle) }
    }

    /// Returns the typed classification DuckDB assigned to this error.
    #[inline]
    pub(crate) fn kind(&self) -> EngineErrorKind {
        // SAFETY: `self.handle` is non-null and live until Drop.
        EngineErrorKind::from_raw(unsafe { duckdb_error_data_error_type(self.handle) })
    }

    /// Copies the error message out of DuckDB-owned memory.
    ///
    /// The pointer DuckDB returns must not be freed and is only valid while this
    /// handle lives, so the string is copied before it can dangle. Returns `None`
    /// if DuckDB supplies no message.
    pub(crate) fn message(&self) -> Option<String> {
        // SAFETY: `self.handle` is non-null and live. `duckdb_error_data_message`
        // returns a borrowed, DuckDB-owned C string (or null); we only read it here
        // and copy it into an owned `String` before returning.
        unsafe {
            let raw = duckdb_error_data_message(self.handle);
            if raw.is_null() {
                return None;
            }
            Some(CStr::from_ptr(raw).to_string_lossy().into_owned())
        }
    }

    /// Materialises a fully owned, pure-Rust error, copying every field out of C
    /// memory. The handle is destroyed when the returned value is built.
    pub(crate) fn to_engine_error(&self) -> EngineError {
        EngineError { kind: self.kind(), message: self.message() }
    }
}

impl Drop for ErrorData {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: `self.handle` is non-null (enforced by `from_raw`) and has not been
        // destroyed before — `ErrorData` owns it exclusively and is not `Copy`, so this
        // runs exactly once. `duckdb_destroy_error_data` nulls the pointer it is given.
        unsafe { duckdb_destroy_error_data(&mut self.handle) };
        debug_assert!(self.handle.is_null(), "duckdb_destroy_error_data must null the handle");
        self.handle = ptr::null_mut();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::Config, helpers::path::path_to_cstring, raw::connection::RawConnection};

    #[test]
    fn from_raw_rejects_null() {
        // SAFETY: null is explicitly the documented error case and is never dereferenced.
        assert!(unsafe { ErrorData::from_raw(ptr::null_mut()) }.is_none());
    }

    /// Drives a real DuckDB failure through an appender so the readers run against
    /// a genuine `duckdb_error_data` handle rather than a synthesised one.
    fn appender_error() -> EngineError {
        let c_path = path_to_cstring(":memory:".as_ref()).unwrap();
        let config = Config::default().with("duckdb_api", "rust").unwrap();
        let mut conn = RawConnection::open_with_flags(&c_path, config).unwrap();
        conn.query("CREATE TABLE t (id INTEGER CHECK (id > 0))").unwrap();

        let mut appender = conn.appender("t", "main").unwrap();
        appender.append(&mut crate::types::value::DuckValue::Int(-1)).unwrap();
        match appender.save() {
            Err(crate::error::Error::Engine(engine)) => engine,
            other => panic!("expected a typed engine error, got {other:?}"),
        }
    }

    #[test]
    fn readers_expose_duckdb_classification_and_message() {
        let engine = appender_error();
        // DuckDB typed this as a constraint failure; the driver reports that kind
        // rather than parsing the message.
        assert_eq!(engine.kind, EngineErrorKind::Constraint);
        let message = engine.message.expect("DuckDB should supply a message");
        assert!(message.contains("CHECK"), "unexpected message: {message}");
    }

    #[test]
    fn owned_error_survives_the_handle_and_the_connection() {
        // `appender_error` returns after its connection and error-data handle are
        // dropped; the owned message must not point into freed C memory.
        let engine = appender_error();
        assert!(engine.message.as_deref().is_some_and(|m| m.contains("CHECK")));
    }
}
