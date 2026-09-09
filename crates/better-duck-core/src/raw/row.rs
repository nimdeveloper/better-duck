use crate::{
    error::{Error, Result},
    ffi::{self, DUCKDB_TYPE},
    raw::data_chunk::DataChunk,
    types::value::DuckValue,
};
use std::ptr;
use std::sync::Arc;

/// A single row of data returned by a DuckDB query, consisting of typed values
/// and their associated column names.
///
/// Values are accessible by column index via [`get_idx`](DuckRow::get_idx)
/// or by column name via [`get`](DuckRow::get).
///
/// Column names are shared via `Arc` across every row of the same result set:
/// cloning a `DuckRow` (as the iterator does internally when rewind support is
/// enabled) is one `Arc` bump, not a fresh per-row allocation of every column name.
#[derive(Debug, Clone)]
pub struct DuckRow(Vec<DuckValue>, Arc<[Box<str>]>);

impl DuckRow {
    /// Creates a new [`DuckRow`] from a vector of values and a shared, owned slice of
    /// column names.
    ///
    /// # Arguments
    ///
    /// * `result` - The column values for this row.
    /// * `col_names` - Owned column names, one per value, shared across the result set.
    pub fn new(
        result: Vec<DuckValue>,
        col_names: Arc<[Box<str>]>,
    ) -> DuckRow {
        DuckRow(result, col_names)
    }

    /// Returns a reference to the value for the given column name, or `None` if
    /// no column with that name exists in this row.
    ///
    /// The comparison is case-sensitive and matches by string value (not by pointer).
    ///
    /// # Arguments
    ///
    /// * `name` - The column name to look up.
    #[allow(unused)]
    pub fn get(
        &self,
        name: &str,
    ) -> Option<&DuckValue> {
        self.1
            .iter()
            .zip(self.0.iter())
            .find(|(col_name, _)| col_name.as_ref() == name)
            .map(|(_, value)| value)
    }

    /// Returns a reference to the value at the given column index, or `None` if the
    /// index is out of range.
    ///
    /// # Arguments
    ///
    /// * `idx` - Zero-based column index.
    #[allow(unused)]
    pub fn get_idx(
        &self,
        idx: usize,
    ) -> Option<&DuckValue> {
        if idx < self.0.len() {
            Some(&self.0[idx])
        } else {
            None
        }
    }

    /// Returns the number of columns in this row.
    pub fn column_count(&self) -> u64 {
        self.1.len() as u64
    }

    /// Constructs a [`DuckRow`] from the current position of a `DataChunk`.
    ///
    /// # Errors
    ///
    /// Returns an error if the chunk has no columns or if a column vector is null.
    pub fn from_chunk(
        chunk: &mut DataChunk,
        col_names: Arc<[Box<str>]>,
        col_types: &[DUCKDB_TYPE],
    ) -> Result<Self> {
        let row_idx = chunk.current_row() - 1; // Adjust for 0-based index
        let column_count = col_names.len() as u64;
        if column_count == 0 {
            return Err(Error::DuckDBFailure(
                ffi::Error::new(ffi::DuckDBError),
                Some("No columns in result".to_owned()),
            ));
        }
        let mut values: Vec<DuckValue> = Vec::with_capacity(column_count as usize);
        let values_ptr: *mut DuckValue = values.as_mut_ptr();

        for col_idx in 0..column_count {
            // SAFETY: `**chunk` is a valid duckdb_data_chunk; `col_idx` is within
            // [0, column_count). `duckdb_data_chunk_get_vector` returns null on failure,
            // which we check immediately below.
            let col_vec = unsafe { ffi::duckdb_data_chunk_get_vector(**chunk, col_idx) };

            if col_vec.is_null() {
                return Err(Error::DuckDBFailure(
                    ffi::Error::new(ffi::DuckDBError),
                    Some("Column returned invalid null ptr".to_owned()),
                ));
            }
            let val = DuckValue::from_duckdb_vec(col_vec, col_types[col_idx as usize], row_idx)
                .map_err(Error::ConversionError)?;

            // SAFETY: `values_ptr` points to the allocation backing `values` with capacity
            // `column_count`. `col_idx` is within that capacity, so `add(col_idx)` is in
            // bounds. We set the length after all writes succeed.
            unsafe { ptr::write(values_ptr.add(col_idx as usize), val) };
        }
        // SAFETY: We have written exactly `column_count` initialized elements starting at
        // `values_ptr`. Setting the length to `column_count` is therefore sound.
        unsafe { values.set_len(column_count as usize) };

        Ok(DuckRow(values, col_names))
    }
}

#[cfg(test)]
#[allow(clippy::undocumented_unsafe_blocks)]
mod tests {
    use super::*;
    use crate::{config::Config, helpers::path::path_to_cstring, raw::connection::RawConnection};

    fn get_test_connection() -> RawConnection {
        let c_path = path_to_cstring(":memory:".as_ref()).unwrap();
        let config = Config::default().with("duckdb_api", "rust").unwrap();
        RawConnection::open_with_flags(&c_path, config).unwrap()
    }

    fn column_names(names: &[&str]) -> Arc<[Box<str>]> {
        names.iter().map(|name| Box::<str>::from(*name)).collect::<Vec<_>>().into()
    }

    fn empty_data_chunk_at_current_row() -> DataChunk {
        let mut types = Vec::<ffi::duckdb_logical_type>::new();
        let raw = unsafe { ffi::duckdb_create_data_chunk(types.as_mut_ptr(), 0) };
        let mut chunk = DataChunk::new(raw).expect("DuckDB should create a zero-column chunk");
        chunk.1 = 1;
        chunk
    }

    #[test]
    fn test_get_by_column_name() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        con.query("INSERT INTO t VALUES (42, 'hello')").unwrap();

        let mut stmt = con.prepare("SELECT id, name FROM t").unwrap();
        let mut result = stmt.execute().unwrap();

        let row = result.next().expect("expected a row").unwrap();
        assert_eq!(row.get("id"), Some(&DuckValue::Int(42)));
        assert_eq!(row.get("name"), Some(&DuckValue::Text("hello".to_string())));
        assert_eq!(row.get("nonexistent"), None);
    }

    #[test]
    fn new_preserves_values_names_and_order() {
        let names = column_names(&["id", "active", "note"]);
        let row = DuckRow::new(
            vec![DuckValue::Int(7), DuckValue::Boolean(true), DuckValue::Null],
            Arc::clone(&names),
        );

        assert_eq!(row.column_count(), 3);
        assert_eq!(row.0, [DuckValue::Int(7), DuckValue::Boolean(true), DuckValue::Null]);
        assert!(Arc::ptr_eq(&row.1, &names));
        assert_eq!(row.get_idx(0), Some(&DuckValue::Int(7)));
        assert_eq!(row.get_idx(1), Some(&DuckValue::Boolean(true)));
        assert_eq!(row.get_idx(2), Some(&DuckValue::Null));
    }

    #[test]
    fn empty_row_reports_no_columns_or_values() {
        let row = DuckRow::new(Vec::new(), column_names(&[]));

        assert_eq!(row.column_count(), 0);
        assert!(row.0.is_empty());
        assert_eq!(row.get_idx(0), None);
        assert_eq!(row.get(""), None);
        assert_eq!(row.get("anything"), None);
    }

    #[test]
    fn get_idx_returns_values_at_boundaries_and_none_out_of_bounds() {
        let row = DuckRow::new(
            vec![DuckValue::Int(-1), DuckValue::Text("last".into())],
            column_names(&["first", "last"]),
        );

        assert_eq!(row.get_idx(0), Some(&DuckValue::Int(-1)));
        assert_eq!(row.get_idx(1), Some(&DuckValue::Text("last".into())));
        assert_eq!(row.get_idx(2), None);
        assert_eq!(row.get_idx(usize::MAX), None);
    }

    #[test]
    fn get_is_case_sensitive_and_matches_exact_names() {
        let row = DuckRow::new(
            vec![DuckValue::Int(1), DuckValue::Int(2), DuckValue::Int(3)],
            column_names(&["Name", "name", "name "]),
        );

        assert_eq!(row.get("Name"), Some(&DuckValue::Int(1)));
        assert_eq!(row.get("name"), Some(&DuckValue::Int(2)));
        assert_eq!(row.get("name "), Some(&DuckValue::Int(3)));
        assert_eq!(row.get("NAME"), None);
        assert_eq!(row.get(" name"), None);
    }

    #[test]
    fn get_returns_first_value_for_duplicate_names() {
        let row = DuckRow::new(
            vec![DuckValue::Text("first".into()), DuckValue::Text("second".into())],
            column_names(&["duplicate", "duplicate"]),
        );

        assert_eq!(row.get("duplicate"), Some(&DuckValue::Text("first".into())));
        assert_eq!(row.get_idx(1), Some(&DuckValue::Text("second".into())));
    }

    #[test]
    fn lookup_only_pairs_names_with_existing_values() {
        let row = DuckRow::new(vec![DuckValue::Int(10)], column_names(&["first", "orphan"]));

        assert_eq!(row.column_count(), 2);
        assert_eq!(row.get("first"), Some(&DuckValue::Int(10)));
        assert_eq!(row.get("orphan"), None);
    }

    #[test]
    fn clone_has_independent_values_and_shared_column_names() {
        let names = column_names(&["value"]);
        let original = DuckRow::new(vec![DuckValue::Text("before".into())], Arc::clone(&names));
        let mut cloned = original.clone();

        assert!(Arc::ptr_eq(&original.1, &cloned.1));
        assert!(Arc::ptr_eq(&cloned.1, &names));
        cloned.0[0] = DuckValue::Text("after".into());
        assert_eq!(original.get("value"), Some(&DuckValue::Text("before".into())));
        assert_eq!(cloned.get("value"), Some(&DuckValue::Text("after".into())));
    }

    #[test]
    fn debug_includes_values_and_column_names() {
        let row = DuckRow::new(vec![DuckValue::Int(9)], column_names(&["score"]));
        let debug = format!("{row:?}");

        assert!(debug.contains("Int(9)"), "unexpected debug output: {debug}");
        assert!(debug.contains("score"), "unexpected debug output: {debug}");
    }

    #[test]
    fn from_chunk_rejects_empty_column_names_before_accessing_chunk() {
        let mut chunk = empty_data_chunk_at_current_row();

        let error = DuckRow::from_chunk(&mut chunk, column_names(&[]), &[]).unwrap_err();
        assert_eq!(
            error,
            Error::DuckDBFailure(
                ffi::Error::new(ffi::DuckDBError),
                Some("No columns in result".to_owned()),
            )
        );
    }

    #[test]
    fn from_chunk_rejects_a_name_without_a_corresponding_vector() {
        let mut chunk = empty_data_chunk_at_current_row();

        let error = DuckRow::from_chunk(
            &mut chunk,
            column_names(&["missing"]),
            &[ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER],
        )
        .unwrap_err();
        assert_eq!(
            error,
            Error::DuckDBFailure(
                ffi::Error::new(ffi::DuckDBError),
                Some("Column returned invalid null ptr".to_owned()),
            )
        );
    }
}
