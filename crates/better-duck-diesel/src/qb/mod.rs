//! Contains DuckDB-specific query builder implementations

pub mod limit_offset;

use diesel::query_builder::{AstPass, QueryBuilder, QueryFragment, QueryId};
use diesel::result::QueryResult;

use crate::backend::DuckDb;

/// The query builder for DuckDB
#[derive(Debug, Default)]
pub struct DuckDbQueryBuilder {
    sql: String,
    bind_idx: u32,
}

impl QueryBuilder<DuckDb> for DuckDbQueryBuilder {
    fn push_sql(
        &mut self,
        sql: &str,
    ) {
        self.sql.push_str(sql);
    }

    fn push_identifier(
        &mut self,
        identifier: &str,
    ) -> QueryResult<()> {
        self.push_sql("\"");
        self.push_sql(&identifier.replace('"', "\"\""));
        self.push_sql("\"");
        Ok(())
    }

    fn push_bind_param(&mut self) {
        self.bind_idx += 1;
        self.sql.push('$');
        let mut buf = itoa::Buffer::new();
        self.sql.push_str(buf.format(self.bind_idx));
    }

    fn push_bind_param_value_only(&mut self) {
        self.bind_idx += 1;
    }

    fn finish(self) -> String {
        self.sql
    }
}

/// A helper for concatenating multiple things separated by a delimiter
#[derive(Debug, Default)]
pub struct Comma<'a, T> {
    values: &'a [T],
}

impl<'a, T> Comma<'a, T> {
    /// Creates a new comma helper for the given slice
    pub fn new(values: &'a [T]) -> Self {
        Self { values }
    }
}

impl<'a, T, DB> QueryFragment<DB> for Comma<'a, T>
where
    DB: diesel::backend::Backend,
    T: QueryFragment<DB>,
{
    fn walk_ast<'b>(
        &'b self,
        mut out: AstPass<'_, 'b, DB>,
    ) -> QueryResult<()> {
        for (index, value) in self.values.iter().enumerate() {
            if index > 0 {
                out.push_sql(", ");
            }
            value.walk_ast(out.reborrow())?;
        }
        Ok(())
    }
}

/// A helper for constructing IN clauses
#[derive(Debug)]
pub struct In<'a, T> {
    values: &'a [T],
    negated: bool,
}

impl<'a, T> In<'a, T> {
    /// Creates a new IN clause for the given slice
    pub fn new(values: &'a [T]) -> Self {
        Self { values, negated: false }
    }

    /// Creates a new NOT IN clause for the given slice
    pub fn new_not_in(values: &'a [T]) -> Self {
        Self { values, negated: true }
    }
}

impl<'a, T, DB> QueryFragment<DB> for In<'a, T>
where
    DB: diesel::backend::Backend,
    T: QueryFragment<DB>,
{
    fn walk_ast<'b>(
        &'b self,
        mut out: AstPass<'_, 'b, DB>,
    ) -> QueryResult<()> {
        if self.negated {
            out.push_sql(" NOT IN (");
        } else {
            out.push_sql(" IN (");
        }

        for (i, value) in self.values.iter().enumerate() {
            if i > 0 {
                out.push_sql(", ");
            }
            value.walk_ast(out.reborrow())?;
        }
        out.push_sql(")");
        Ok(())
    }
}

impl<'a, T> QueryId for In<'a, T> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

#[cfg(test)]
mod tests {
    use diesel::expression::SqlLiteral;
    use diesel::query_builder::{QueryBuilder, QueryFragment};

    use super::{Comma, DuckDbQueryBuilder, In};
    use crate::backend::DuckDb;

    fn render<T>(fragment: &T) -> String
    where
        T: QueryFragment<DuckDb>,
    {
        let mut builder = DuckDbQueryBuilder::default();
        fragment.to_sql(&mut builder, &DuckDb).unwrap();
        builder.finish()
    }

    fn literal(sql: &'static str) -> SqlLiteral<diesel::sql_types::Integer> {
        diesel::dsl::sql(sql)
    }

    #[test]
    fn comma_renders_empty_single_and_multiple_values() {
        let empty: [SqlLiteral<diesel::sql_types::Integer>; 0] = [];
        assert_eq!(render(&Comma::new(&empty)), "");

        let single = [literal("1")];
        assert_eq!(render(&Comma::new(&single)), "1");

        let multiple = [literal("1"), literal("2"), literal("3")];
        assert_eq!(render(&Comma::new(&multiple)), "1, 2, 3");
    }

    #[test]
    fn in_renders_positive_negated_and_empty_lists() {
        let values = [literal("1"), literal("2")];
        assert_eq!(render(&In::new(&values)), " IN (1, 2)");
        assert_eq!(render(&In::new_not_in(&values)), " NOT IN (1, 2)");

        let empty: [SqlLiteral<diesel::sql_types::Integer>; 0] = [];
        assert_eq!(render(&In::new(&empty)), " IN ()");
        assert_eq!(render(&In::new_not_in(&empty)), " NOT IN ()");
    }

    #[test]
    fn query_builder_quotes_and_escapes_identifiers() {
        let mut builder = DuckDbQueryBuilder::default();
        builder.push_identifier("odd\"name").unwrap();
        assert_eq!(builder.finish(), "\"odd\"\"name\"");
    }

    #[test]
    fn query_builder_numbers_bind_parameters() {
        let mut builder = DuckDbQueryBuilder::default();
        builder.push_bind_param();
        builder.push_sql(", ");
        builder.push_bind_param_value_only();
        builder.push_bind_param();
        assert_eq!(builder.finish(), "$1, $3");
    }

    #[test]
    fn debug_query_uses_escaped_identifier_and_numbered_bind() {
        diesel::table! {
            #[sql_name = "odd\"table"]
            odd_table (id) {
                id -> Integer,
            }
        }

        use diesel::prelude::*;
        let query = odd_table::table.filter(odd_table::id.eq(42));
        let sql = diesel::debug_query::<DuckDb, _>(&query).to_string();
        assert!(sql.contains("\"odd\"\"table\""), "unexpected SQL: {sql}");
        assert!(sql.contains("$1"), "unexpected SQL: {sql}");
    }
}
