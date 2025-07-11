#![allow(dead_code)]
pub mod appendable;
// pub mod blob;
#[cfg(feature = "chrono")]
mod date_chrono;
#[cfg(not(feature = "chrono"))]
mod date_native;
pub mod numeric;
pub mod value;
pub mod varchar;
use crate::error::DuckDBConversionError;

use crate::error::Result;
use appendable::AppendAble;
use libduckdb_sys::duckdb_bind_boolean;

use crate::ffi::{
    duckdb_append_bool, duckdb_create_bool, duckdb_get_bool, duckdb_value,
    DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
};

/// Trait for converting between DuckDB values and Rust types.
///
/// This trait provides methods to safely convert DuckDB values to Rust types and vice versa,
/// handling type safety and conversion errors. Implementors must ensure that conversions
/// are performed only between compatible types to avoid undefined behavior.
///
/// # Safety
///
/// The `from_duck` method assumes that the provided `duckdb_value` matches the expected DuckDB type
/// for the implementing Rust type. Passing a value of an incorrect type may result in panics or
/// memory safety issues. Always perform type checking at a higher level before calling this method.
///
/// # Errors
///
/// Both `from_duck` and `to_duck` return a [`DuckDBConversionError`] if the conversion fails.
///
/// # Example
///
/// ```rust
/// use better_duck_core::types::{DuckDialect, DuckDBConversionError};
///
/// // Assume `duckdb_value` is a valid DuckDB boolean value obtained from DuckDB.
/// let duckdb_value: duckdb_value = /* obtain from DuckDB */;
///
/// // Convert DuckDB value to Rust bool
/// let rust_bool = bool::from_duck(duckdb_value)?;
///
/// // Convert Rust bool back to DuckDB value
/// let duckdb_value2 = rust_bool.to_duck()?;
/// # Ok::<(), DuckDBConversionError>(())
/// ```
pub trait DuckDialect
where
    Self: Sized,
{
    /// Converts a DuckDB value to the implementing Rust type.
    ///
    /// # Safety
    ///
    /// This method assumes that the provided `duckdb_value` is of the correct DuckDB type
    /// for the implementing Rust type. Calling this method with a value of the wrong type
    /// is undefined behavior and may lead to panics or memory safety issues.
    ///
    /// # Best Practice
    ///
    /// Always ensure that the DuckDB type of the value matches the expected Rust type
    /// before calling this method. Type checking should be performed at a higher level.
    ///
    /// # Example
    ///
    /// ```
    /// use better_duck_core::types::{DuckDialect, DuckDBConversionError};
    /// // Assume `duckdb_value` is a valid DuckDB boolean value.
    /// let value: duckdb_value = /* obtained from DuckDB */;
    /// let rust_bool = bool::from_duck(value)?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a `DuckDBConversionError` if the conversion fails.
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError>
    where
        Self: Sized;

    /// Converts the implementing Rust type to a DuckDB value.
    ///
    /// This method transforms the Rust value into a DuckDB-compatible value (`duckdb_value`),
    /// suitable for insertion or update operations in DuckDB.
    ///
    /// # Errors
    ///
    /// Returns a [`DuckDBConversionError`] if the conversion fails, for example due to
    /// unsupported types, precision loss, or null values.
    ///
    /// # Example
    ///
    /// ```
    /// use better_duck_core::types::{DuckDialect, DuckDBConversionError};
    ///
    /// let rust_bool = true;
    /// let duckdb_value = rust_bool.to_duck()?;
    /// # Ok::<(), DuckDBConversionError>(())
    /// ```
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError>;
}

pub trait DuckType {
    // Converts the current type to a DuckDB-compatible format.
    fn from_duck() -> ();
    // Converts a DuckDB-compatible format to the current type.
    fn to_duck() -> ();
}

// Macro to implement DuckDialect for types

macro_rules! impl_duck_append_able {
    ($rust_type:ty, $duck_type:expr, $duck_append_fn:expr, $duck_bind_fn:expr) => {
        impl AppendAble for $rust_type {
            fn appender_append(
                &mut self,
                appender: crate::ffi::duckdb_appender,
            ) -> Result<()> {
                unsafe { $duck_append_fn(appender, *self) };
                Ok(())
            }
            fn stmt_append(
                &mut self,
                idx: u64,
                stmt: crate::ffi::duckdb_prepared_statement,
            ) -> Result<()> {
                unsafe { $duck_bind_fn(stmt, idx, *self) };
                Ok(())
            }
        }
    };
}
macro_rules! impl_duck_dialect {
    ($rust_type:ty, $duck_type:expr, $to_duck_fn:expr, $from_duck_fn:expr) => {
        impl DuckDialect for $rust_type {
            fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
                // if type_ != $duck_type {
                //     return Err(DuckDBConversionError::TypeMismatch {
                //         expected: $duck_type,
                //         found: type_,
                //     });
                // }
                Ok(unsafe { $from_duck_fn(value) })
            }

            fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
                Ok(unsafe { $to_duck_fn(*self) })
            }
        }
    };
}

// Implementations for various types
impl_duck_dialect!(bool, DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN, duckdb_create_bool, duckdb_get_bool);
impl_duck_append_able!(
    bool,
    DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
    duckdb_append_bool,
    duckdb_bind_boolean
);

// impl_duck_dialect_string!(str, DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);

// Implementations for other DuckDB types would follow the same pattern
// (Date, Time, Timestamp, Decimal, etc. would need custom handling)
