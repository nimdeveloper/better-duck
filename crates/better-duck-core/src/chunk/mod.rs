//! DuckDB data chunks: DuckDB's native columnar batch representation, and
//! typed read/write views over their individual columns.
//!
//! Ungated — unlike [`crate::udf`] (which re-exports everything here for
//! backward compatibility), this module does not require the `udf` feature.
//! It backs both the user-defined-function machinery and, directly, the
//! columnar bulk read/write APIs on [`DuckResult`](crate::raw::result::DuckResult)
//! and [`Appender`](crate::raw::appender::Appender).

mod data_chunk;
mod logical_type;
mod vector;

/// An owned-or-borrowed DuckDB data chunk.
pub use data_chunk::DataChunkHandle;
/// An owned DuckDB logical type handle.
pub use logical_type::LogicalType;
/// Read and write views over a single column of a [`DataChunkHandle`], plus the
/// [`ScalarArg`]/[`ScalarRet`] marshalling traits and the [`FixedWidth`] bound
/// behind [`VectorRef::as_slice`].
pub use vector::{FixedWidth, ScalarArg, ScalarRet, VectorMut, VectorRef};

/// The error type returned by chunk-level value conversions, and by
/// user-defined-function callbacks (re-exported as [`crate::udf::UdfResult`]).
///
/// `+ Send + Sync` because UDF callbacks may run on DuckDB worker threads.
pub type UdfResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;
