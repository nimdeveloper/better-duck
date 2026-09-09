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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::types::value::DuckValue;

    fn column_names() -> Box<[Box<str>]> {
        vec![Box::from("id"), Box::from("name")].into_boxed_slice()
    }

    fn result_set() -> ResultSet {
        let column_names = column_names();
        let row_column_names = Arc::from(column_names.clone());
        let rows = vec![
            DuckRow::new(
                vec![DuckValue::Int(20), DuckValue::Text("second".into())],
                Arc::clone(&row_column_names),
            ),
            DuckRow::new(
                vec![DuckValue::Int(10), DuckValue::Text("first".into())],
                row_column_names,
            ),
        ];

        ResultSet::new(rows, 2, column_names)
    }

    fn ids<'a>(rows: impl IntoIterator<Item = &'a DuckRow>) -> Vec<i32> {
        rows.into_iter()
            .map(|row| match row.get_idx(0) {
                Some(DuckValue::Int(id)) => *id,
                value => panic!("expected integer id, got {value:?}"),
            })
            .collect()
    }

    #[test]
    fn empty_result_exposes_metadata_and_no_rows() {
        let result = ResultSet::new(Vec::new(), 3, column_names());

        assert!(result.is_empty());
        assert_eq!(result.len(), 0);
        assert!(result.rows().is_empty());
        assert!(result.first().is_none());
        assert_eq!(result.changes(), 3);
        assert_eq!(result.column_names(), &[Box::from("id"), Box::from("name")]);
        assert!(result.into_rows().is_empty());
    }

    #[test]
    fn non_empty_accessors_clone_and_debug_preserve_data() {
        let result = result_set();
        let cloned = result.clone();

        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        assert_eq!(result.changes(), 2);
        assert_eq!(result.column_names(), &[Box::from("id"), Box::from("name")]);
        assert_eq!(
            result.first().and_then(|row| row.get("name")),
            Some(&DuckValue::Text("second".into()))
        );
        assert_eq!(ids(result.rows()), vec![20, 10]);
        assert_eq!(ids(cloned.rows()), vec![20, 10]);

        let debug = format!("{result:?}");
        assert!(debug.contains("ResultSet"));
        assert!(debug.contains("changes: 2"));
        assert!(debug.contains("second"));
    }

    #[test]
    fn borrowed_and_consuming_iteration_retain_order() {
        let result = result_set();

        assert_eq!(ids(&result), vec![20, 10]);
        assert_eq!(result.len(), 2, "borrowing must not consume the result");

        let names: Vec<_> = result.into_iter().map(|row| row.get("name").cloned()).collect();
        assert_eq!(
            names,
            vec![Some(DuckValue::Text("second".into())), Some(DuckValue::Text("first".into())),]
        );
    }
}
