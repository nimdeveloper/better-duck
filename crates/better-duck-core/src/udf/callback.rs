//! Panic and error containment for DuckDB C callbacks.
//!
//! DuckDB invokes user-defined-function bind/init/execute callbacks through
//! `extern "C"` function pointers. A Rust panic must not unwind through that
//! boundary: since Rust 1.81, a panic escaping a plain `extern "C"` frame aborts
//! the process regardless of panic strategy, and under this workspace's own
//! `panic = "abort"` release profile it always aborts anyway. [`contain_callback`]
//! is therefore the outermost thing in every trampoline in [`crate::udf`] — it
//! converts both ordinary `Err` returns and panics into a DuckDB error string,
//! and never lets a panic escape.
//!
//! Adapted from the `duckdb` crate's `callback.rs`, which is hardened against
//! panicking `Display` impls, panicking destructors, deeply nested and cyclic
//! error-source chains, and non-string panic payloads — those edge cases are
//! preserved here rather than reimplemented from scratch.

use std::{
    any::{type_name, Any},
    error::Error,
    ffi::{c_void, CStr, CString},
    io::{self, Write as _},
    mem,
    panic::{catch_unwind, AssertUnwindSafe},
};

const NON_STRING_PANIC_PAYLOAD: &str =
    "non-string panic payload; use a string panic message for details";
const MAX_ERROR_CAUSES: usize = 16;

/// Receives failures from a contained DuckDB callback.
pub(crate) trait CallbackErrorSink {
    fn set_c_error(
        &self,
        error: &CStr,
    );

    /// Reports a Rust error message, escaping interior NUL bytes as `\0`.
    fn report_error(
        &self,
        error: &str,
    ) {
        self.set_c_error(&error_c_string(error));
    }
}

/// Runs a callback and reports any failure through DuckDB.
///
/// Callback panics and panics from error formatting or destruction are
/// contained here and never unwind to the caller. Failures are converted to a
/// DuckDB error before this function returns.
pub(crate) fn contain_callback(
    sink: &impl CallbackErrorSink,
    callback: impl FnOnce() -> Result<(), Box<dyn Error + Send + Sync + 'static>>,
) {
    if let Err(error) = catch_boxed_callback(callback) {
        sink.report_error(&error);
    }
}

fn catch_boxed_callback(
    callback: impl FnOnce() -> Result<(), Box<dyn Error + Send + Sync + 'static>>
) -> Result<(), String> {
    match catch_unwind(AssertUnwindSafe(callback)) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => {
            let message = describe_error(error.as_ref());
            drop_or_report("error returned by callback", error);
            Err(message)
        },
        Err(payload) => Err(format!("Rust callback panicked: {}", take_panic_payload(payload))),
    }
}

fn describe_error(error: &dyn Error) -> String {
    catch_format(|| format_error_chain(error)).unwrap_or_else(|formatting_panic| {
        let debug_context = match catch_format(|| format!("{error:?}")) {
            Ok(message) => format!("callback error Debug context: {message}"),
            Err(debug_panic) => {
                format!("callback error Debug formatting also panicked: {debug_panic}")
            },
        };
        format!("Rust callback error formatting panicked: {formatting_panic}; {debug_context}")
    })
}

/// Runs a formatting closure, converting a panic into its payload message.
fn catch_format(format: impl FnOnce() -> String) -> Result<String, String> {
    catch_unwind(AssertUnwindSafe(format)).map_err(take_panic_payload)
}

fn format_error_chain(error: &dyn Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    let mut depth = 0;
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
        depth += 1;
        if depth >= MAX_ERROR_CAUSES && source.is_some() {
            message.push_str(": additional error sources omitted");
            break;
        }
    }
    message
}

/// Frees a boxed callback state without allowing its destructor to unwind
/// across the C callback boundary.
///
/// # Safety
///
/// `ptr` must have been created by `Box::into_raw` for a `Box<T>` and must not
/// have been freed already.
pub(crate) unsafe extern "C" fn drop_boxed<T>(ptr: *mut c_void) {
    // SAFETY: the caller guarantees `ptr` came from `Box::into_raw::<T>` and has
    // not been freed yet.
    drop_or_report(type_name::<T>(), unsafe { Box::from_raw(ptr.cast::<T>()) });
}

/// Converts a Rust callback error into a C string.
///
/// Interior NUL bytes are escaped as the two-character sequence `\0`.
fn error_c_string(error: &str) -> CString {
    CString::new(error.replace('\0', "\\0")).expect("NUL replacement must produce a valid C string")
}

fn drop_or_report<T>(
    context: &str,
    value: T,
) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(move || drop(value))) {
        let message = take_destructor_panic_payload(payload);
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _ = writeln!(
                io::stderr().lock(),
                "[better-duck] caught a callback destructor panic for {context}: {message}"
            );
        }));
    }
}

fn take_panic_payload(payload: Box<dyn Any + Send>) -> String {
    take_payload_message(payload, |payload| drop_or_report("panic payload", payload))
}

/// Extracts a destructor panic payload's message.
///
/// This path must never dispose through `drop_or_report`: a hostile destructor
/// payload could then recurse without bound. Make one final drop attempt and
/// stop even if it produces another hostile payload.
fn take_destructor_panic_payload(payload: Box<dyn Any + Send>) -> String {
    take_payload_message(payload, drop_or_forget)
}

/// Extracts a panic payload's message, handing non-string payloads to
/// `dispose_unknown`.
fn take_payload_message(
    payload: Box<dyn Any + Send>,
    dispose_unknown: impl FnOnce(Box<dyn Any + Send>),
) -> String {
    match downcast_message(payload) {
        Ok(message) => message,
        Err(payload) => {
            dispose_unknown(payload);
            NON_STRING_PANIC_PAYLOAD.to_owned()
        },
    }
}

fn downcast_message(payload: Box<dyn Any + Send>) -> Result<String, Box<dyn Any + Send>> {
    payload
        .downcast::<String>()
        .map(|message| *message)
        .or_else(|payload| payload.downcast::<&'static str>().map(|message| (*message).to_owned()))
}

fn drop_or_forget<T>(value: T) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(move || drop(value))) {
        // Reclaim known string payloads. An unknown payload may reproduce the
        // destructor panic, so it is deliberately forgotten (and leaked).
        if let Err(payload) = downcast_message(payload) {
            mem::forget(payload);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, fmt, panic::panic_any};

    use super::{contain_callback, drop_boxed, CallbackErrorSink};

    #[derive(Default)]
    struct CapturingErrorSink(RefCell<Option<String>>);

    impl CallbackErrorSink for CapturingErrorSink {
        fn set_c_error(
            &self,
            error: &std::ffi::CStr,
        ) {
            self.0.replace(Some(error.to_string_lossy().into_owned()));
        }
    }

    #[derive(Debug)]
    struct PlainError(&'static str);
    impl fmt::Display for PlainError {
        fn fmt(
            &self,
            f: &mut fmt::Formatter<'_>,
        ) -> fmt::Result {
            f.write_str(self.0)
        }
    }
    impl std::error::Error for PlainError {}

    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("destructor panic")
        }
    }

    #[test]
    fn ok_callback_reports_nothing() {
        let sink = CapturingErrorSink::default();
        contain_callback(&sink, || Ok(()));
        assert_eq!(sink.0.take(), None);
    }

    #[test]
    fn err_callback_reports_the_message() {
        let sink = CapturingErrorSink::default();
        contain_callback(&sink, || Err(Box::new(PlainError("boom"))));
        assert_eq!(sink.0.take().as_deref(), Some("boom"));
    }

    #[test]
    fn panicking_callback_is_contained_and_reported() {
        let sink = CapturingErrorSink::default();
        contain_callback(&sink, || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            panic!("callback panic")
        });
        assert!(sink.0.take().unwrap().contains("callback panic"));
    }

    #[test]
    fn nul_bytes_in_error_message_are_escaped() {
        let sink = CapturingErrorSink::default();
        contain_callback(&sink, || Err("before\0after".into()));
        assert_eq!(sink.0.take().as_deref(), Some("before\\0after"));
    }

    #[test]
    fn drop_boxed_survives_a_panicking_destructor() {
        let ptr = Box::into_raw(Box::new(PanicOnDrop)).cast();
        // SAFETY: `ptr` was created by `Box::into_raw` for a `Box<PanicOnDrop>` above.
        unsafe { drop_boxed::<PanicOnDrop>(ptr) };
    }

    #[test]
    fn non_string_panic_payload_is_described_generically() {
        let sink = CapturingErrorSink::default();
        contain_callback(&sink, || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            panic_any(7_u8)
        });
        assert!(sink.0.take().unwrap().contains("non-string panic payload"));
    }
}
