use crate::{
    error::{Error, Result},
    ffi::{self, DUCKDB_TYPE},
    raw::data_chunk::DataChunk,
    types::value::DuckValue,
};
use std::ptr;

/// A single row of data returned by a DuckDB query, consisting of typed values
/// and their associated column names.
///
/// Values are accessible by column index via [`get_idx`](DuckRow::get_idx)
/// or by column name via [`get`](DuckRow::get).
#[derive(Debug)]
pub struct DuckRow(Vec<DuckValue>, Box<[Box<str>]>);

impl DuckRow {
    /// Creates a new [`DuckRow`] from a vector of values and an owned boxed slice of
    /// column names.
    ///
    /// # Arguments
    ///
    /// * `result` - The column values for this row.
    /// * `col_names` - Owned column names, one per value.
    pub fn new(
        result: Vec<DuckValue>,
        col_names: Box<[Box<str>]>,
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

    /// Constructs a [`DuckRow`] from the current position of a [`DataChunk`].
    ///
    /// # Errors
    ///
    /// Returns an error if the chunk has no columns or if a column vector is null.
    pub fn from_chunk(
        chunk: &mut DataChunk,
        col_names: Box<[Box<str>]>,
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
}
