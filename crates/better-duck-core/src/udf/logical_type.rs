//! An RAII wrapper around a `duckdb_logical_type` handle.

use crate::{
    error::{Error, Result},
    ffi::{duckdb_destroy_logical_type, duckdb_logical_type},
    types::DuckLogicalType,
};

/// An owned DuckDB logical type handle.
///
/// [`DuckLogicalType::duck_logical_type`] returns a raw, caller-destroys pointer;
/// this type is the single adapter that takes that raw form into RAII. Every
/// user-defined-function registration path in [`crate::udf`] uses `LogicalType`
/// rather than the raw pointer, so a `duckdb_logical_type` is never held across
/// more than one statement.
pub struct LogicalType(duckdb_logical_type);

impl LogicalType {
    /// Returns the logical type of the Rust type `T`.
    ///
    /// # Errors
    ///
    /// Returns an error if `T` has no fixed DuckDB representation.
    pub fn of<T: DuckLogicalType>() -> Result<Self> {
        let raw = T::duck_logical_type().map_err(Error::ConversionError)?;
        Self::from_raw(raw)
    }

    /// Takes ownership of a raw `duckdb_logical_type` handle.
    ///
    /// # Errors
    ///
    /// Returns an error if `raw` is null.
    pub(crate) fn from_raw(raw: duckdb_logical_type) -> Result<Self> {
        if raw.is_null() {
            return Err(Error::ConversionError(
                crate::error::DuckDBConversionError::ConversionError(
                    "duck_logical_type() returned a null logical type".to_owned(),
                ),
            ));
        }
        Ok(Self(raw))
    }

    /// Returns the underlying raw handle, still owned by `self`.
    ///
    /// The handle is valid only for the lifetime of `self`. DuckDB's
    /// `add_parameter`/`set_return_type`/`add_result_column`-style functions all
    /// copy the logical type they are given, so dropping `self` immediately after
    /// passing this to one of them is correct.
    pub(crate) fn as_raw(&self) -> duckdb_logical_type {
        self.0
    }
}

impl Drop for LogicalType {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: `self.0` is a valid, non-null `duckdb_logical_type` owned by
            // this `LogicalType` and not yet destroyed (guarded by the null check
            // above); `duckdb_destroy_logical_type` is called exactly once here.
            unsafe { duckdb_destroy_logical_type(&mut self.0) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn of_bool_succeeds_and_drops_cleanly() {
        let lt = LogicalType::of::<bool>().unwrap();
        assert!(!lt.as_raw().is_null());
        drop(lt);
    }

    #[test]
    fn of_i32_succeeds() {
        let lt = LogicalType::of::<i32>().unwrap();
        assert!(!lt.as_raw().is_null());
    }

    #[test]
    fn of_string_succeeds() {
        let lt = LogicalType::of::<String>().unwrap();
        assert!(!lt.as_raw().is_null());
    }
}
