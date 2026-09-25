//! DuckDB aggregate functions, exposed as Diesel DSL via `define_sql_function!`
//! with `#[aggregate]`. These are the aggregates diesel does not ship (beyond
//! `count`/`sum`/`avg`/`min`/`max`).

pub mod general;
pub mod statistical;
