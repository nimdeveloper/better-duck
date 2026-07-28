use crate::raw::row::DuckRow;

/// A fully materialized, owned query result.
///
/// Unlike [`DuckResult`](crate::raw::result::DuckResult), a `ResultSet` holds no FFI
/// handles: it is `Send`, `Sync`, and `Clone`, and can be moved across threads or
/// returned from a blocking task. Build one with
/// [`DuckResult::materialize`](crate::raw::result::DuckResult::materialize).
#[derive(Debug, Clone)]
pub struct ResultSet {
    rows: Vec<DuckRow>,
    changes: u64,
    column_names: Box<[Box<str>]>,
}

impl ResultSet {
    /// Constructs a `ResultSet` from already-materialized parts.
    pub(crate) fn new(
        rows: Vec<DuckRow>,
        changes: u64,
        column_names: Box<[Box<str>]>,
    ) -> ResultSet {
        ResultSet { rows, changes, column_names }
    }

    /// Returns the rows as a slice.
    pub fn rows(&self) -> &[DuckRow] {
        &self.rows
    }

    /// Consumes this result set, returning the owned rows.
    pub fn into_rows(self) -> Vec<DuckRow> {
        self.rows
    }

    /// Returns the number of rows changed by the query that produced this result.
    ///
    /// `0` for `SELECT` statements.
    pub fn changes(&self) -> u64 {
        self.changes
    }

    /// Returns the column names in result order.
    pub fn column_names(&self) -> &[Box<str>] {
        &self.column_names
    }

    /// Returns the number of rows in this result set.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` if this result set has no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns the first row, if any.
    pub fn first(&self) -> Option<&DuckRow> {
        self.rows.first()
    }
}

impl IntoIterator for ResultSet {
    type Item = DuckRow;
    type IntoIter = std::vec::IntoIter<DuckRow>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

impl<'a> IntoIterator for &'a ResultSet {
    type Item = &'a DuckRow;
    type IntoIter = std::slice::Iter<'a, DuckRow>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.iter()
    }
}
