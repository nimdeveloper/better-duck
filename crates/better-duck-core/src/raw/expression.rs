//! RAII wrapper for a bound `duckdb_expression`, with constant folding.
//!
//! DuckDB hands a bound expression to a scalar function's bind callback (one per
//! argument, via `duckdb_scalar_function_bind_get_argument`). The handle is owned —
//! it must be destroyed with `duckdb_destroy_expression` — so [`Expression`] wraps it
//! in RAII. It exposes the expression's return type, whether it is *foldable* into a
//! constant, and [`fold`](Expression::fold), which evaluates a foldable expression to
//! a value (e.g. to specialise a function on a constant argument at bind time).
//!
//! `duckdb_expression_fold` returns a `duckdb_error_data`, read through the same
//! [`ErrorData`] RAII wrapper the rest of the driver uses, so a fold failure surfaces
//! as a typed [`EngineError`] rather than a bare string.
// FFI pointer args are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::ffi::{c_void, CStr};

use crate::{
    error::{EngineError, EngineErrorKind},
    ffi::{
        duckdb_destroy_expression, duckdb_destroy_value, duckdb_expression, duckdb_expression_fold,
        duckdb_expression_is_foldable, duckdb_expression_return_type, duckdb_free, duckdb_get_bool,
        duckdb_get_double, duckdb_get_int32, duckdb_get_int64, duckdb_get_type_id,
        duckdb_get_value_type, duckdb_get_varchar, duckdb_value, DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
        DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
        DUCKDB_TYPE_DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
    },
    raw::{client_context::ClientContext, error_data::ErrorData},
    types::{value::DuckValue, LogicalType},
};

/// An owned, bound `duckdb_expression` (destroyed once on drop).
pub struct Expression {
    expr: duckdb_expression,
}

impl Expression {
    /// Takes ownership of a raw expression handle, or `None` if null.
    ///
    /// # Safety
    ///
    /// `expr` must be a live `duckdb_expression` whose ownership is transferred here
    /// (destroyed on drop).
    pub(crate) unsafe fn from_raw(expr: duckdb_expression) -> Option<Expression> {
        if expr.is_null() {
            return None;
        }
        Some(Expression { expr })
    }

    /// The expression's return type, or `None` if DuckDB reports none.
    #[must_use]
    pub fn return_type(&self) -> Option<LogicalType> {
        // SAFETY: `self.expr` is a valid expression; the returned logical type is
        // owned (destroy once) and wrapped in RAII. Null → None.
        LogicalType::from_raw(unsafe { duckdb_expression_return_type(self.expr) }).ok()
    }

    /// Whether the expression can be folded into a constant value.
    #[must_use]
    pub fn is_foldable(&self) -> bool {
        // SAFETY: `self.expr` is a valid expression.
        unsafe { duckdb_expression_is_foldable(self.expr) }
    }

    /// Folds a foldable expression into a scalar [`DuckValue`] using `context`.
    ///
    /// # Errors
    ///
    /// Returns an [`EngineError`] if DuckDB reports a fold error, or if the folded
    /// value is of a type this scalar extractor does not cover.
    pub fn fold(
        &self,
        context: &ClientContext<'_>,
    ) -> Result<DuckValue, EngineError> {
        let mut out: duckdb_value = std::ptr::null_mut();
        // SAFETY: `context` and `self.expr` are valid; `out` is a valid out-pointer.
        // `duckdb_expression_fold` returns an owned error-data handle (read via RAII)
        // and, on success, writes an owned `duckdb_value` into `out`.
        let err = unsafe { duckdb_expression_fold(context.as_raw(), self.expr, &mut out) };
        // SAFETY: `err` is an owned error-data handle (or null); `ErrorData` owns it.
        if let Some(data) = unsafe { ErrorData::from_raw(err) } {
            if data.has_error() {
                if !out.is_null() {
                    // SAFETY: `out` was written by fold; destroy exactly once.
                    unsafe { duckdb_destroy_value(&mut out) };
                }
                return Err(data.to_engine_error());
            }
        }
        if out.is_null() {
            return Err(EngineError {
                kind: EngineErrorKind::Unavailable,
                message: Some("expression folded to no value".to_owned()),
            });
        }
        // SAFETY: `out` is an owned scalar value; the helper reads and destroys it.
        unsafe { scalar_value_to_duckvalue(out) }
    }
}

/// Reads an owned scalar `duckdb_value` into a [`DuckValue`], destroying it.
///
/// # Safety
///
/// `value` must be an owned, non-null `duckdb_value`; ownership is taken here.
unsafe fn scalar_value_to_duckvalue(mut value: duckdb_value) -> Result<DuckValue, EngineError> {
    // SAFETY: `value` is valid; `duckdb_get_value_type` returns a *borrowed* logical
    // type owned by `value` (must NOT be destroyed); we only read its type id.
    let type_id = unsafe { duckdb_get_type_id(duckdb_get_value_type(value)) };
    // SAFETY: `value` is a valid scalar value of the type just read.
    let result = unsafe {
        match type_id {
            DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => Ok(DuckValue::Boolean(duckdb_get_bool(value))),
            DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => Ok(DuckValue::Int(duckdb_get_int32(value))),
            DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => Ok(DuckValue::BigInt(duckdb_get_int64(value))),
            DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => Ok(DuckValue::Double(duckdb_get_double(value))),
            DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => {
                let c = duckdb_get_varchar(value);
                if c.is_null() {
                    Ok(DuckValue::Text(String::new()))
                } else {
                    let s = CStr::from_ptr(c).to_string_lossy().into_owned();
                    duckdb_free(c as *mut c_void);
                    Ok(DuckValue::Text(s))
                }
            },
            other => Err(EngineError {
                kind: EngineErrorKind::Unavailable,
                message: Some(format!("folded value type {other} is not supported")),
            }),
        }
    };
    // SAFETY: `value` is owned here; destroy exactly once.
    unsafe { duckdb_destroy_value(&mut value) };
    result
}

impl Drop for Expression {
    fn drop(&mut self) {
        if !self.expr.is_null() {
            // SAFETY: `self.expr` is a valid, non-null expression owned by `self` and
            // not yet destroyed; destroyed exactly once here.
            unsafe { duckdb_destroy_expression(&mut self.expr) };
        }
    }
}
