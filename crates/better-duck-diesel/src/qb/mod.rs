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

//
//
// TODO: Check below
//
//

/// A helper for concatenating multiple things separated by a delimiter
#[derive(Debug, Default)]
pub struct Comma<'a, T> {
    values: &'a [T],
    already_appended: bool,
}

impl<'a, T> Comma<'a, T> {
    /// Creates a new comma helper for the given slice
    pub fn new(values: &'a [T]) -> Self {
        Self { values, already_appended: false }
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
        for value in self.values {
            if self.already_appended {
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
