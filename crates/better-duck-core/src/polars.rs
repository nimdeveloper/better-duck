//! Polars `DataFrame` interop (feature `polars`), bridged through the Arrow C Data
//! Interface — no `arrow-rs` is involved. `better-duck-core` links `polars` (for the
//! `DataFrame` type) and `polars-arrow` (for the C Data Interface FFI); the DuckDB side
//! keeps using its own dependency-free Arrow export ([`crate::arrow`]).
//!
//! Direction supported here:
//! - [`Connection::query_polars`] — run SQL and collect the result into a `DataFrame`.
//!
//! (`register_polars`, the `DataFrame` → DuckDB direction, follows.)

use polars::prelude::{CompatLevel, DataFrame, PlSmallStr, PolarsResult};
use polars_arrow::array::{Array, StructArray};
use polars_arrow::datatypes::{ArrowDataType, Field};
use polars_arrow::ffi::{
    export_iterator, import_array_from_c, import_field_from_c, ArrowArray, ArrowArrayStream,
    ArrowSchema,
};

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::types::appendable::AppendAble;

/// Maps a polars error into our error type.
fn polars_err(e: impl std::fmt::Display) -> Error {
    Error::DuckDBFailure(crate::ffi::Error::new(crate::ffi::DuckDBError), Some(e.to_string()))
}

impl Connection {
    /// Executes `sql` (binding `binds` as positional parameters) and collects the full
    /// result into a Polars [`DataFrame`].
    ///
    /// The data crosses via the Arrow C Data Interface: the result is exported with
    /// DuckDB's Arrow conversion and imported into Polars with `polars-arrow`'s FFI, so
    /// no Arrow IPC or copy through JSON is involved.
    ///
    /// # Errors
    ///
    /// Returns an error if execution, the Arrow conversion, or the Polars import fails.
    pub fn query_polars(
        &mut self,
        sql: impl AsRef<str>,
        binds: &mut [&mut dyn AppendAble],
    ) -> Result<DataFrame> {
        let arrow = self.query_arrow(sql, binds)?;
        let (mut schema, arrays, _chunks) = arrow.into_parts();

        // The exported schema is a single struct whose children are the columns. Import
        // it as a Field; DuckDB only lent us the schema, so `schema` still releases it.
        // SAFETY: `schema` is a valid Arrow C Data Interface schema for the duration of
        // this borrow (it is dropped at the end of the function).
        let field = unsafe {
            import_field_from_c(&*(schema.as_mut_ptr().cast::<ArrowSchema>()))
        }
        .map_err(polars_err)?;
        let dtype = field.dtype.clone();

        let mut out: Option<DataFrame> = None;
        for handle in arrays {
            // Move our exported array into a polars-arrow C array (neutralizing our
            // handle's release), then hand ownership to polars via `import_array_from_c`.
            let mut c_array = std::mem::MaybeUninit::<ArrowArray>::uninit();
            // SAFETY: `c_array` is a writable ArrowArray-layout slot; `export_to` moves
            // our array into it and clears our release so only polars releases it.
            unsafe { handle.export_to(c_array.as_mut_ptr().cast()) };
            // SAFETY: `c_array` now holds a valid Arrow C Data Interface array matching
            // `dtype`; `import_array_from_c` takes ownership of it.
            let array =
                unsafe { import_array_from_c(c_array.assume_init(), dtype.clone()) }
                    .map_err(polars_err)?;

            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| polars_err("expected a struct array from the Arrow import"))?
                .clone();
            let chunk = DataFrame::try_from(struct_array).map_err(polars_err)?;

            out = Some(match out {
                None => chunk,
                Some(mut acc) => {
                    acc.vstack_mut(&chunk).map_err(polars_err)?;
                    acc
                }
            });
        }

        Ok(out.unwrap_or_else(DataFrame::empty))
    }

    /// Registers a Polars [`DataFrame`] as a DuckDB table named `name`, replacing any
    /// existing table of that name. The data is materialized into DuckDB (a real table,
    /// not a view over borrowed memory), so the `DataFrame` need not outlive the call.
    ///
    /// The `DataFrame` is exported to a single Arrow C stream (via `polars-arrow`) and
    /// scanned into a temporary view, from which `CREATE OR REPLACE TABLE … AS SELECT`
    /// materializes the table; the temporary view is dropped before returning.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` contains an interior NUL, or if the Arrow export or the
    /// DuckDB scan / table creation fails.
    pub fn register_polars(&mut self, name: &str, df: &DataFrame) -> Result<()> {
        // One Arrow array per column (each rechunked to a single chunk).
        let columns = df.rechunk_to_arrow(CompatLevel::newest());
        let col_names = df.get_column_names();
        let fields: Vec<Field> = col_names
            .iter()
            .zip(columns.iter())
            .map(|(n, a)| Field::new((*n).clone(), a.dtype().clone(), true))
            .collect();
        let length = df.height();
        let struct_dtype = ArrowDataType::Struct(fields);
        let struct_array = StructArray::new(struct_dtype.clone(), length, columns, None);
        let boxed: Box<dyn Array> = Box::new(struct_array);

        let record_field = Field::new(PlSmallStr::from_static("record"), struct_dtype, false);
        let iter: Box<dyn Iterator<Item = PolarsResult<Box<dyn Array>>>> =
            Box::new(std::iter::once(Ok(boxed)));
        let mut stream: ArrowArrayStream = export_iterator(iter, record_field);

        // Unique temp view name (tied to the stream's address).
        let view = format!("__better_duck_polars_{:p}", std::ptr::addr_of!(stream));
        // `stream` is a valid Arrow C Data Interface stream that stays on this stack
        // across the scan and the materializing SELECT below; DuckDB reads it fully
        // during `CREATE TABLE … AS SELECT` and releases it (release is idempotent, so
        // the eventual drop of `stream` is a no-op).
        crate::arrow::arrow_scan(
            self.raw_con(),
            &view,
            std::ptr::addr_of_mut!(stream).cast(),
        )?;

        let esc_name = name.replace('"', "\"\"");
        let create =
            format!("CREATE OR REPLACE TABLE \"{esc_name}\" AS SELECT * FROM \"{view}\"");
        let outcome = self.execute_batch(create).map(|_| ());
        // Always drop the temporary view, whether or not the CREATE succeeded.
        let _ = self.execute_batch(format!("DROP VIEW IF EXISTS \"{view}\""));
        outcome
    }
}

#[cfg(test)]
mod tests {
    use crate::connection::Connection;

    #[test]
    fn query_polars_reads_rows_into_a_dataframe() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER, name VARCHAR); \
             INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');",
        )
        .unwrap();

        let df = conn.query_polars("SELECT id, name FROM t ORDER BY id", &mut []).unwrap();
        assert_eq!(df.shape(), (3, 2), "three rows, two columns");
        assert!(df.column("id").is_ok(), "id column present");
        assert!(df.column("name").is_ok(), "name column present");

        let ids = df.column("id").unwrap();
        assert_eq!(ids.get(0).unwrap(), polars::prelude::AnyValue::Int32(1));
        assert_eq!(ids.get(2).unwrap(), polars::prelude::AnyValue::Int32(3));
    }

    #[test]
    fn query_polars_empty_result_yields_zero_rows() {
        let mut conn = Connection::open_in_memory().unwrap();
        let df = conn.query_polars("SELECT 1 AS a WHERE 1 = 0", &mut []).unwrap();
        assert_eq!(df.height(), 0);
    }

    #[test]
    fn register_polars_round_trips_a_dataframe() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE src (id INTEGER, name VARCHAR); \
             INSERT INTO src VALUES (1, 'a'), (2, 'b');",
        )
        .unwrap();

        // DataFrame in hand (built from DuckDB so we don't hand-construct one).
        let df = conn.query_polars("SELECT id, name FROM src ORDER BY id", &mut []).unwrap();

        // Register it back as a new DuckDB table and read it again.
        conn.register_polars("dst", &df).unwrap();
        let back = conn.query_polars("SELECT id, name FROM dst ORDER BY id", &mut []).unwrap();

        assert_eq!(back.shape(), (2, 2));
        assert_eq!(
            back.column("id").unwrap().get(1).unwrap(),
            polars::prelude::AnyValue::Int32(2)
        );
    }
}
