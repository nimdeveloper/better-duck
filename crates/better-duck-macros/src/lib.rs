//! Procedural macros for `better-duck` user-defined functions.
//!
//! This crate is not meant to be used directly — depend on `better-duck-core`
//! with the `udf` feature enabled, which re-exports [`duckdb_scalar`],
//! [`duckdb_table_function`], [`duckdb_aggregate`], and [`duckdb_cast`].

mod attrs;
mod sig;
mod udf;

use proc_macro::TokenStream;
use syn::{parse_macro_input, ItemFn, ItemMod};

/// Registers a plain Rust function as a DuckDB scalar function.
///
/// See `better_duck_core::udf` for usage and supported attribute options.
#[proc_macro_attribute]
pub fn duckdb_scalar(
    attr: TokenStream,
    item: TokenStream,
) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let original = input.clone();
    match attrs::parse_scalar_attrs(attr).and_then(|attrs| udf::scalar::expand(attrs, input)) {
        Ok(expanded) => expanded.into(),
        Err(err) => {
            // Re-emit the original item alongside the error so the user gets one
            // good diagnostic instead of a cascade of "cannot find function".
            let mut out: TokenStream = quote::quote!(#original).into();
            out.extend(TokenStream::from(err.to_compile_error()));
            out
        },
    }
}

/// Registers a plain Rust function as a DuckDB table function.
///
/// See `better_duck_core::udf` for usage and supported attribute options.
#[proc_macro_attribute]
pub fn duckdb_table_function(
    attr: TokenStream,
    item: TokenStream,
) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let original = input.clone();
    match attrs::parse_table_attrs(attr).and_then(|attrs| udf::table::expand(attrs, input)) {
        Ok(expanded) => expanded.into(),
        Err(err) => {
            let mut out: TokenStream = quote::quote!(#original).into();
            out.extend(TokenStream::from(err.to_compile_error()));
            out
        },
    }
}

/// Registers a single Rust function as a DuckDB custom cast (`CAST`/`TRY_CAST`).
///
/// Applied to `fn(source) -> target` (or `-> Result<target, E>`); the source and
/// target logical types are inferred from the signature. See `better_duck_core::udf`.
#[proc_macro_attribute]
pub fn duckdb_cast(
    attr: TokenStream,
    item: TokenStream,
) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let original = input.clone();
    match attrs::parse_cast_attrs(attr).and_then(|attrs| udf::cast::expand(attrs, input)) {
        Ok(expanded) => expanded.into(),
        Err(err) => {
            let mut out: TokenStream = quote::quote!(#original).into();
            out.extend(TokenStream::from(err.to_compile_error()));
            out
        },
    }
}

/// Registers a `mod` of `init`/`update`/`combine`/`finalize` functions as a DuckDB
/// aggregate function.
///
/// The state type is inferred from `init`'s return, the argument types from
/// `update`, and the result type from `finalize`. See `better_duck_core::udf`.
#[proc_macro_attribute]
pub fn duckdb_aggregate(
    attr: TokenStream,
    item: TokenStream,
) -> TokenStream {
    let input = parse_macro_input!(item as ItemMod);
    let original = input.clone();
    match attrs::parse_aggregate_attrs(attr).and_then(|attrs| udf::aggregate::expand(attrs, input)) {
        Ok(expanded) => expanded.into(),
        Err(err) => {
            let mut out: TokenStream = quote::quote!(#original).into();
            out.extend(TokenStream::from(err.to_compile_error()));
            out
        },
    }
}
