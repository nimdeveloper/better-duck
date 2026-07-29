//! User-defined DuckDB functions: scalar functions and table functions.
//!
//! Requires the `udf` feature.
//!
//! Two kinds of function can be registered on a [`Connection`](crate::connection::Connection):
//!
//! - **Scalar functions** compute one value per row, for use in a `SELECT` list
//!   or `WHERE` clause: `SELECT my_func(x) FROM t`.
//! - **Table functions** produce rows and columns, for use in a `FROM` clause:
//!   `SELECT * FROM my_func(1, 2)`.
//!
//! The [`duckdb_scalar`](crate::duckdb_scalar) and
//! [`duckdb_table_function`](crate::duckdb_table_function) attribute macros turn
//! an ordinary Rust function into either kind without requiring any `unsafe` code
//! or manual vector handling. Parameter and return types are inferred from the
//! Rust signature via [`types::DuckLogicalType`](crate::types::DuckLogicalType) —
//! any type that already round-trips through [`AppendAble`](crate::AppendAble)
//! (all the integer widths, floats, `String`/`&str`, `bool`, …) works here too,
//! with no extra type table for the macros to maintain.
//!
//! # Scalar functions
//!
//! ```
//! use better_duck_core::{connection::Connection, duckdb_scalar};
//!
//! /// Repeats `s` `n` times.
//! #[duckdb_scalar]
//! fn repeat_str(s: &str, n: i32) -> String {
//!     s.repeat(n.max(0) as usize)
//! }
//!
//! /// `Option` parameters/returns propagate `NULL` explicitly.
//! #[duckdb_scalar]
//! fn double_or_null(x: Option<i32>) -> Option<i32> {
//!     x.map(|v| v * 2)
//! }
//!
//! /// A `Result<T, E>` return fails the query with `E`'s message on `Err`.
//! #[duckdb_scalar(name = "to_int")]
//! fn parse_int(s: &str) -> Result<i32, std::num::ParseIntError> {
//!     s.parse()
//! }
//!
//! # fn main() -> better_duck_core::error::Result<()> {
//! let mut conn = Connection::open_in_memory()?;
//! repeat_str::register(&mut conn)?;
//! parse_int::register(&mut conn)?;
//!
//! let mut result = conn.execute("SELECT repeat_str('ab', 3) AS r")?;
//! assert_eq!(result.next().unwrap()?.get("r"), Some(&better_duck_core::types::value::DuckValue::text("ababab")));
//! # Ok(())
//! # }
//! ```
//!
//! # Table functions
//!
//! The function returns `impl Iterator<Item = T> + Send` — `T` for a single
//! column, or a tuple `(A, B, ...)` for several. DuckDB pulls rows in chunks, so
//! the iterator is driven lazily and may be scanned across several calls.
//!
//! ```
//! use better_duck_core::{connection::Connection, duckdb_table_function};
//!
//! /// The integers in `[start, stop)`.
//! #[duckdb_table_function(name = "series", columns("n"))]
//! fn series(start: i64, stop: i64) -> impl Iterator<Item = i64> + Send {
//!     start..stop
//! }
//!
//! # fn main() -> better_duck_core::error::Result<()> {
//! let mut conn = Connection::open_in_memory()?;
//! series::register(&mut conn)?;
//! let mut result = conn.execute("SELECT sum(n) AS total FROM series(1, 101)")?;
//! assert_eq!(result.next().unwrap()?.get("total"), Some(&better_duck_core::types::value::DuckValue::HugeInt(5050)));
//! # Ok(())
//! # }
//! ```
//!
//! # Attribute options
//!
//! | option | scalar | table | meaning |
//! |---|---|---|---|
//! | `name = "sql_name"` | yes | yes | the SQL function name (defaults to the Rust fn's name) |
//! | `crate = ::path` | yes | yes | re-export escape hatch for generated code's `::better_duck_core` path |
//! | `volatile` | yes | — | disables DuckDB's zero-argument constant-folding |
//! | `columns("a", "b")` | — | yes | result column names (defaults: the fn name for one column, `column_0`, `column_1`, … for several) |
//!
//! `special_handling` (whether `NULL` inputs still invoke the function) is
//! inferred automatically: present whenever any parameter is `Option<T>`.
//!
//! # Panics and `panic = "abort"`
//!
//! Callback panics are caught and reported to DuckDB as a query error whenever
//! the crate is built with `panic = "unwind"` (the default for `dev` and `test`
//! profiles). Under a `panic = "abort"` build — this workspace's own `release`
//! profile, for instance — a panicking user-defined function aborts the process
//! instead: `catch_unwind` cannot catch anything once unwinding itself has been
//! compiled out. Prefer returning `Err` from a fallible user-defined function
//! over panicking; the ordinary error path never depends on unwinding.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

pub(crate) mod callback;
mod data_chunk;
mod logical_type;
/// DuckDB scalar functions: row-wise functions used in a `SELECT` list or
/// `WHERE` clause.
pub mod scalar;
/// DuckDB table functions: functions used in a `FROM` clause that produce rows
/// and columns.
pub mod table;
mod vector;

/// An owned-or-borrowed DuckDB data chunk.
pub use data_chunk::DataChunkHandle;
/// An owned DuckDB logical type handle.
pub use logical_type::LogicalType;
/// The trait behind DuckDB scalar functions, and its signature type.
pub use scalar::{ScalarSignature, VScalar};
/// The row type produced by a `#[duckdb_table_function]`-generated function,
/// and the shared init-data/execution-loop helpers it compiles down to.
pub use table::{run_table_func, TableInitData, TableRow};
/// The trait behind DuckDB table functions, and its callback-info types.
pub use table::{BindInfo, InitInfo, TableFunctionInfo, VTab};
/// Read and write views over a single column of a [`DataChunkHandle`], plus the
/// [`ScalarArg`]/[`ScalarRet`] marshalling traits.
pub use vector::{ScalarArg, ScalarRet, VectorMut, VectorRef};

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
#[macro_export]
macro_rules! duck_bail {
    ($($arg:tt)*) => {
        return Err(::std::format!($($arg)*).into())
    };
}

/// Items referenced by code generated by the `#[duckdb_scalar]`/
/// `#[duckdb_table_function]` attribute macros.
///
/// Not part of the public API: renamed or removed without notice. Referenced by
/// generated code as `::better_duck_core::udf::__private::…` so that a user
/// crate shadowing e.g. `Result` cannot break macro expansion.
#[doc(hidden)]
pub mod __private {
    pub use crate::connection::Connection;
    pub use crate::error::Result;
    pub use crate::types::DuckLogicalType;
    pub use crate::udf::{
        run_table_func, BindInfo, DataChunkHandle, InitInfo, LogicalType, ScalarArg, ScalarRet,
        ScalarSignature, TableFunctionInfo, TableInitData, TableRow, UdfResult, VScalar, VTab,
        VectorMut, VectorRef,
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
