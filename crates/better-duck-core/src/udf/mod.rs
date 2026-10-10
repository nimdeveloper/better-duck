//! User-defined DuckDB functions, and the shared data-chunk / vector types they
//! build on.
//!
//! The columnar read/write types — [`DataChunkHandle`], [`VectorRef`],
//! [`VectorMut`], the [`ScalarArg`](crate::udf::ScalarArg)/[`ScalarRet`](crate::udf::ScalarRet) marshalling traits, and the
//! [`FixedWidth`] marker — are available with either the `udf` or the `columnar`
//! feature. The user-defined-function registration surface (scalar/table/
//! aggregate/cast functions and the `#[duckdb_scalar]` family of macros) requires
//! the `udf` feature and is documented below.
#![cfg_attr(feature = "udf", doc = include_str!("mod_doc.md"))]
#![allow(clippy::not_unsafe_ptr_arg_deref)]

/// DuckDB scalar functions: row-wise functions used in a `SELECT` list or
/// `WHERE` clause.
#[cfg(feature = "udf")]
pub mod aggregate;
#[cfg(feature = "udf")]
pub(crate) mod callback;
#[cfg(feature = "udf")]
pub mod cast;
#[cfg(feature = "udf")]
mod context;
mod data_chunk;
/// DuckDB replacement scans: rewrite an unresolved table reference into a
/// table function call. **Experimental** — see the module docs.
#[cfg(feature = "udf")]
pub mod replacement;

#[cfg(feature = "udf")]
pub mod scalar;
/// DuckDB table functions: functions used in a `FROM` clause that produce rows
/// and columns.
#[cfg(feature = "udf")]
pub mod table;
mod vector;

/// An owned DuckDB logical type handle.
pub use crate::types::LogicalType;
/// The trait behind DuckDB scalar functions, and its signature type.
#[cfg(feature = "udf")]
pub use aggregate::{AggregateSetBuilder, VAggregate};
#[cfg(feature = "udf")]
pub use cast::VCast;
/// An owned-or-borrowed DuckDB data chunk.
pub use data_chunk::DataChunkHandle;
/// The trait behind DuckDB replacement scans, and its callback-info type.
/// **Experimental** — see the [`replacement`] module docs.
#[cfg(feature = "udf")]
pub use replacement::{ReplacementScan, ReplacementScanInfo};

#[cfg(feature = "udf")]
pub use scalar::{ScalarBindInfo, ScalarSignature, VScalar};
/// Opt-in trait for a [`VTab`] with per-worker-thread ("local") init data.
#[cfg(feature = "udf")]
pub use table::VTabLocalInit;
/// The row type produced by a `#[duckdb_table_function]`-generated function,
/// and the shared init-data/execution-loop helpers it compiles down to.
#[cfg(feature = "udf")]
pub use table::{run_table_func, TableInitData, TableRow};
/// The trait behind DuckDB table functions, and its callback-info types.
#[cfg(feature = "udf")]
pub use table::{BindInfo, InitInfo, TableFunctionInfo, VTab};
/// Read and write views over a single column of a [`DataChunkHandle`], plus the
/// [`ScalarArg`]/[`ScalarRet`] marshalling traits.
pub use vector::{FixedWidth, ScalarArg, ScalarRet, VectorMut, VectorRef};

/// The error type returned by user-defined-function callbacks.
///
/// `+ Send + Sync` because callbacks may run on DuckDB worker threads.
pub type UdfResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

/// Returns early from a fallible user-defined-function body with a formatted
/// error message.
///
/// Expands to `return Err(format!(...).into())` — the surrounding function's
/// error type must implement `From<String>`, which is true for `String` itself
/// and for `Box<dyn std::error::Error + Send + Sync>` (what `#[duckdb_scalar]`/
/// `#[duckdb_table_function]` route a `Result<T, E>` return through).
///
/// # Examples
///
/// ```
/// use better_duck_core::duck_bail;
///
/// fn check(stop: i64, start: i64) -> Result<i64, String> {
///     if stop < start {
///         duck_bail!("stop ({stop}) must be >= start ({start})");
///     }
///     Ok(stop - start)
/// }
/// assert!(check(1, 10).is_err());
/// ```
#[cfg(feature = "udf")]
#[macro_export]
macro_rules! duck_bail {
    ($($arg:tt)*) => {
        return Err(::std::format!($($arg)*).into())
    };
}

/// Returns the 0-based column indices the current query actually needs, as a
/// `Vec<usize>`, from inside a `#[duckdb_table_function(projection_pushdown)]`
/// function body.
///
/// Empty if the query didn't request pushdown (every column is wanted).
///
/// # Panics
///
/// Panics if called outside a table function's `init` callback — contained by
/// this crate's callback machinery, so it always surfaces as a normal query
/// error, never undefined behavior.
///
/// # Examples
///
/// ```
/// use better_duck_core::{connection::Connection, duck_projection, duckdb_table_function};
///
/// #[duckdb_table_function(columns("a", "b"), projection_pushdown)]
/// fn two_cols() -> impl Iterator<Item = (i32, i32)> + Send {
///     let wanted = duck_projection!();
///     // `wanted` lists which of columns 0 (`a`) / 1 (`b`) the query needs.
///     let _ = wanted;
///     std::iter::once((1, 2))
/// }
/// # fn main() -> better_duck_core::error::Result<()> {
/// let mut conn = Connection::open_in_memory()?;
/// two_cols::register(&mut conn)?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "udf")]
#[macro_export]
macro_rules! duck_projection {
    () => {
        $crate::udf::__private::current_projection()
    };
}

/// Returns a clone of the current table function's registration-time "extra
/// info" (see [`Connection::register_table_function_with_extra_info`](crate::connection::Connection::register_table_function_with_extra_info)),
/// from inside a `#[duckdb_table_function]` function body.
///
/// `Type` must be `Clone` — the macro always returns an independently owned
/// value, never a reference into ambient context.
///
/// # Panics
///
/// Panics if called outside a table function callback registered with extra
/// info, or if `Type` doesn't match what was actually registered — contained
/// by this crate's callback machinery, so it always surfaces as a normal
/// query error, never undefined behavior.
#[cfg(feature = "udf")]
#[macro_export]
macro_rules! duck_extra_info {
    ($ty:ty) => {
        $crate::udf::__private::current_table_extra_info::<$ty>()
    };
}

/// Returns a clone of the current scalar function's `State`, from inside a
/// `#[duckdb_scalar(state = ...)]` function body.
///
/// `Type` must be `Clone` — the macro always returns an independently owned
/// value, never a reference into ambient context.
///
/// # Panics
///
/// Panics if called outside a scalar function callback, or if `Type` doesn't
/// match the function's declared state type — contained by this crate's
/// callback machinery, so it always surfaces as a normal query error, never
/// undefined behavior.
#[cfg(feature = "udf")]
#[macro_export]
macro_rules! duck_state {
    ($ty:ty) => {
        $crate::udf::__private::current_scalar_state::<$ty>()
    };
}

/// Items referenced by code generated by the `#[duckdb_scalar]`/
/// `#[duckdb_table_function]` attribute macros.
///
/// Not part of the public API: renamed or removed without notice. Referenced by
/// generated code as `::better_duck_core::udf::__private::…` so that a user
/// crate shadowing e.g. `Result` cannot break macro expansion.
#[cfg(feature = "udf")]
#[doc(hidden)]
pub mod __private {
    pub use crate::connection::Connection;
    pub use crate::error::Result;
    pub use crate::types::DuckLogicalType;
    pub use crate::udf::context::{
        current_projection, current_scalar_state, current_table_extra_info, ProjectionGuard,
        ScalarStateGuard, TableExtraInfoGuard,
    };
    pub use crate::udf::{
        run_table_func, BindInfo, DataChunkHandle, InitInfo, LogicalType, ScalarArg,
        ScalarBindInfo, ScalarRet, ScalarSignature, TableFunctionInfo, TableInitData, TableRow,
        UdfResult, VAggregate, VCast, VScalar, VTab, VTabLocalInit, VectorMut, VectorRef,
    };
    pub use std::boxed::Box;
    pub use std::result::Result as StdResult;
    pub use std::vec::Vec;

    /// Boxes any suitable error into the UDF error type. Used by generated code
    /// for a user function returning `Result<T, E>`.
    pub fn boxed_error<E>(e: E) -> Box<dyn std::error::Error + Send + Sync + 'static>
    where
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        e.into()
    }
}
