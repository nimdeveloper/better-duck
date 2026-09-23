//! Query-builder helpers and `debug_query` rendering.
//!
//! `better_duck_diesel::query_builder` (re-exported as `qb`) exposes the DuckDB
//! `DuckDbQueryBuilder` plus the `Comma` and `In` fragment helpers. Rendering a `QueryFragment`
//! against `DuckDbQueryBuilder` shows the raw SQL a fragment produces. `diesel::debug_query`
//! renders a whole query for the DuckDB backend, showing `$1`-style numbered placeholders and
//! double-quoted identifiers.

use better_duck_diesel::backend::DuckDb;
use better_duck_diesel::query_builder::{Comma, DuckDbQueryBuilder, In};
use diesel::prelude::*;
use diesel::query_builder::{QueryBuilder, QueryFragment};

diesel::table! {
    accounts (id) {
        id   -> Integer,
        name -> Text,
    }
}

/// Render a standalone fragment to the SQL string it emits on the DuckDB backend.
fn render<T: QueryFragment<DuckDb>>(fragment: &T) -> String {
    let mut builder = DuckDbQueryBuilder::default();
    fragment.to_sql(&mut builder, &DuckDb).expect("render fragment");
    builder.finish()
}

fn literal(sql: &'static str) -> diesel::expression::SqlLiteral<diesel::sql_types::Integer> {
    diesel::dsl::sql(sql)
}

fn main() {
    println!("=== Comma helper: values joined by ', ' ===");
    let empty: [diesel::expression::SqlLiteral<diesel::sql_types::Integer>; 0] = [];
    assert_eq!(render(&Comma::new(&empty)), "");
    let three = [literal("1"), literal("2"), literal("3")];
    let comma_sql = render(&Comma::new(&three));
    assert_eq!(comma_sql, "1, 2, 3");
    println!("  Comma::new([1,2,3]) -> {comma_sql:?}");

    println!("=== In helper: IN (...) and NOT IN (...) ===");
    let values = [literal("1"), literal("2")];
    let in_sql = render(&In::new(&values));
    let not_in_sql = render(&In::new_not_in(&values));
    assert_eq!(in_sql, " IN (1, 2)");
    assert_eq!(not_in_sql, " NOT IN (1, 2)");
    println!("  In::new([1,2]) -> {in_sql:?}");
    println!("  In::new_not_in([1,2]) -> {not_in_sql:?}");

    println!("=== DuckDbQueryBuilder: numbered binds and quoted identifiers ===");
    let mut builder = DuckDbQueryBuilder::default();
    builder.push_identifier("odd\"name").expect("push identifier");
    // Embedded double-quotes are escaped by doubling.
    assert_eq!(builder.finish(), "\"odd\"\"name\"");
    let mut builder = DuckDbQueryBuilder::default();
    builder.push_bind_param();
    builder.push_sql(", ");
    builder.push_bind_param();
    assert_eq!(builder.finish(), "$1, $2");
    println!("  identifiers double-quoted; binds rendered as $1, $2");

    println!("=== debug_query over a real query ===");
    let query = accounts::table.filter(accounts::name.eq("ada")).select(accounts::id);
    let sql = diesel::debug_query::<DuckDb, _>(&query).to_string();
    // Identifiers are double-quoted and the bound value becomes a $-placeholder.
    assert!(sql.contains("\"accounts\""), "expected quoted table: {sql}");
    assert!(sql.contains("\"name\""), "expected quoted column: {sql}");
    assert!(sql.contains("$1"), "expected numbered placeholder: {sql}");
    println!("  {sql}");

    println!("\nQuery-builder helpers verified.");
}
