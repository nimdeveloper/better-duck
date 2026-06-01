#![allow(dead_code)]
// Suppress: `DuckDialect::from_duck`/`to_duck` and `AppendAble` methods accept
// raw FFI pointer parameters by design — implementations are responsible for safety.
#![allow(clippy::not_unsafe_ptr_arg_deref)]
/// Public type modules.
pub mod appendable;
/// `Vec<u8>` DuckDB BLOB type conversion.
pub mod blob;
#[cfg(feature = "chrono")]
mod date_chrono;
/// No-chrono date/time component types and DuckDialect implementations.
#[cfg(not(feature = "chrono"))]
pub mod date_native;
/// Numeric DuckDB type conversions and `AppendAble` implementations.
pub mod numeric;
/// The `DuckValue` enum representing any DuckDB column value.
pub mod value;
/// A reference-based variant of `DuckValue` for zero-copy scenarios.
pub mod value_ref;
/// `String` DuckDB type conversion.
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
/// This trait provides methods to safely convert DuckDB values to Rust types and vice versa.
/// Implementors must ensure that conversions are performed only between compatible types to
/// avoid undefined behavior.
///
/// # Safety
///
/// The `from_duck` method assumes that the provided `duckdb_value` matches the expected DuckDB
/// type for the implementing Rust type. Passing a value of an incorrect type may result in
/// panics or memory safety issues. Always perform type checking at a higher level before
/// calling this method.
///
/// # Errors
///
/// Both `from_duck` and `to_duck` return a [`DuckDBConversionError`] if the conversion fails.
///
/// # Examples
///
/// ```rust
/// use better_duck_core::types::DuckDialect;
/// use better_duck_core::ffi::{duckdb_create_bool, duckdb_destroy_value};
///
/// // SAFETY: duckdb_create_bool always succeeds for any bool value.
/// let mut duck_val = unsafe { duckdb_create_bool(true) };
/// let rust_val = bool::from_duck(duck_val).expect("conversion succeeds");
/// assert!(rust_val);
/// // SAFETY: duck_val was created by duckdb_create_bool above.
/// unsafe { duckdb_destroy_value(&mut duck_val) };
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
    /// # Errors
    ///
    /// Returns a `DuckDBConversionError` if the conversion fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use better_duck_core::types::DuckDialect;
    /// use better_duck_core::ffi::{duckdb_create_bool, duckdb_destroy_value};
    ///
    /// // SAFETY: duckdb_create_bool always succeeds.
    /// let mut val = unsafe { duckdb_create_bool(false) };
    /// let result = bool::from_duck(val).unwrap();
    /// assert!(!result);
    /// // SAFETY: val was created by duckdb_create_bool above.
    /// unsafe { duckdb_destroy_value(&mut val) };
    /// ```
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError>
    where
        Self: Sized;

    /// Converts the implementing Rust type to a DuckDB value.
    ///
    /// # Errors
    ///
    /// Returns a [`DuckDBConversionError`] if the conversion fails, for example due to
    /// unsupported types, precision loss, or null values.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use better_duck_core::types::DuckDialect;
    /// use better_duck_core::ffi::duckdb_destroy_value;
    ///
    /// let rust_bool = true;
    /// let mut duckdb_value = rust_bool.to_duck().expect("conversion succeeds");
    /// // SAFETY: duckdb_value was created by to_duck above.
    /// unsafe { duckdb_destroy_value(&mut duckdb_value) };
    /// ```
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError>;
}

macro_rules! impl_duck_append_able {
    ($rust_type:ty, $duck_type:expr, $duck_append_fn:expr, $duck_bind_fn:expr) => {
        impl AppendAble for $rust_type {
            fn appender_append(
                &mut self,
                appender: crate::ffi::duckdb_appender,
            ) -> Result<()> {
                // SAFETY: `appender` is a valid duckdb_appender. The value is a copy of
                // a valid Rust primitive compatible with the DuckDB column type.
                unsafe { $duck_append_fn(appender, *self) };
                Ok(())
            }
            fn stmt_append(
                &mut self,
                idx: u64,
                stmt: crate::ffi::duckdb_prepared_statement,
            ) -> Result<()> {
                // SAFETY: `stmt` is a valid prepared statement. `idx` is a 1-based parameter
                // index within the statement's parameter count, as required by the DuckDB C API.
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
                // SAFETY: `value` is a valid duckdb_value of the matching DuckDB type.
                Ok(unsafe { $from_duck_fn(value) })
            }

            fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
                // SAFETY: The value is a copy of a valid Rust primitive.
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
/// Represents a DuckDB column data type for use in Rust.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Type {
    /// The value is a `NULL` value.
    Null,
    /// The value is a boolean.
    Boolean,
    /// The value is a signed tiny integer.
    TinyInt,
    /// The value is a signed small integer.
    SmallInt,
    /// The value is a signed integer.
    Int,
    /// The value is a signed big integer.
    BigInt,
    /// The value is a signed huge integer.
    HugeInt,
    /// The value is a unsigned tiny integer.
    UTinyInt,
    /// The value is a unsigned small integer.
    USmallInt,
    /// The value is a unsigned integer.
    UInt,
    /// The value is a unsigned big integer.
    UBigInt,
    /// The value is a unsigned huge integer.
    UHugeInt,
    /// The value is a f32.
    Float,
    /// The value is a f64.
    Double,
    /// The value is a timestamp.
    Timestamp,
    /// The value is a date.
    Date,
    /// The value is a time.
    Time,
    /// The value is an interval (month, day, nano).
    Interval,
    /// The value is a text string.
    Text,
    #[cfg(feature = "decimal")]
    /// The value is a Decimal.
    Decimal,
    /// The value is a blob of data.
    Blob,
    /// The value is a list.
    List,
    /// The value is an enum.
    Enum,
    /// The value is an array with fixed length.
    Array(Box<[Type]>),
    /// The value is a union.
    Union(Box<Type>),
    /// Any DuckDB type.
    Any,
}
