//! DuckDB scalar functions, grouped by domain, exposed as Diesel DSL via
//! `define_sql_function!`. These are backend-agnostic SQL calls that Diesel does
//! not provide out of the box.

pub mod conditional;
pub mod numeric;
pub mod string;
