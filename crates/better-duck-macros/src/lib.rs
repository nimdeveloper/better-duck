//! Procedural macros for `better-duck` user-defined functions.
//!
//! This crate is not meant to be used directly — depend on `better-duck-core`
//! with the `udf` feature enabled, which re-exports [`duckdb_scalar`] and
//! [`duckdb_table_function`].

mod attrs;
mod scalar;
mod sig;
mod table;

use proc_macro::TokenStream;
use syn::{parse_macro_input, ItemFn};

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
    match attrs::parse_scalar_attrs(attr).and_then(|attrs| scalar::expand(attrs, input)) {
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
    match crate::attrs::parse_table_attrs(attr).and_then(|attrs| crate::table::expand(attrs, input))
    {
        Ok(expanded) => expanded.into(),
        Err(err) => {
            let mut out: TokenStream = quote::quote!(#original).into();
            out.extend(TokenStream::from(err.to_compile_error()));
            out
        },
    }
}
