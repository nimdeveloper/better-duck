//! Serialize a core Arrow export ([`better_duck_core::arrow::ArrowResult`]) to Arrow IPC
//! stream bytes for the webview (feature `arrow`).
//!
//! The core handles own the Arrow C Data Interface structs; `export_to` moves each into
//! an arrow-rs `FFI_ArrowSchema` / `FFI_ArrowArray` (transferring the `release` callback),
//! which `from_ffi` then imports. The source data chunks are kept alive until the IPC
//! bytes are written.

use std::ffi::c_void;
use std::sync::Arc;

use arrow::array::{RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ipc::writer::StreamWriter;
use better_duck_core::arrow::ArrowResult;

use crate::error::{Error, Result};

fn backend_err(e: impl std::fmt::Display) -> Error {
    Error::Backend(e.to_string())
}

/// Converts a core Arrow export into Arrow IPC stream bytes.
pub(crate) fn to_ipc(result: ArrowResult) -> Result<Vec<u8>> {
    let (schema_handle, arrays, chunks) = result.into_parts();

    // Move the C Data Interface schema into an arrow-rs FFI schema (transfers `release`).
    let mut ffi_schema = FFI_ArrowSchema::empty();
    // SAFETY: `ffi_schema` is a freshly-empty FFI_ArrowSchema of identical ABI layout.
    unsafe { schema_handle.export_to(std::ptr::addr_of_mut!(ffi_schema).cast::<c_void>()) };

    // The result schema is a top-level struct whose children are the columns.
    let root = Field::try_from(&ffi_schema).map_err(backend_err)?;
    let schema = match root.data_type() {
        DataType::Struct(fields) => Arc::new(Schema::new(fields.clone())),
        other => return Err(Error::Backend(format!("expected a struct arrow schema, got {other:?}"))),
    };

    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema).map_err(backend_err)?;
        for array_handle in arrays {
            let mut ffi_array = FFI_ArrowArray::empty();
            // SAFETY: `ffi_array` is a freshly-empty FFI_ArrowArray of identical ABI layout.
            unsafe { array_handle.export_to(std::ptr::addr_of_mut!(ffi_array).cast::<c_void>()) };
            // SAFETY: `ffi_array` was produced by DuckDB for `ffi_schema`.
            let data = unsafe { from_ffi(ffi_array, &ffi_schema) }.map_err(backend_err)?;
            let batch = RecordBatch::from(StructArray::from(data));
            writer.write(&batch).map_err(backend_err)?;
        }
        writer.finish().map_err(backend_err)?;
    }
    drop(chunks); // released only after serialization has copied the data out
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::RecordBatch;
    use arrow::ipc::reader::StreamReader;

    use crate::backend::{DuckBackend, DuckEngine};

    #[test]
    fn query_arrow_roundtrips_via_ipc() {
        const MEM: &str = "duckdb::memory:";
        let engine = DuckEngine::new();
        engine.load(MEM).unwrap();
        engine.execute(MEM, "CREATE TABLE t (id INTEGER, name VARCHAR)", vec![]).unwrap();
        engine.execute(MEM, "INSERT INTO t VALUES (1, 'a'), (2, 'b')", vec![]).unwrap();

        let bytes = engine.query_arrow(MEM, "SELECT id, name FROM t ORDER BY id", vec![]).unwrap();

        let reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
        let batches: Vec<RecordBatch> = reader.map(std::result::Result::unwrap).collect();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 2);
        assert_eq!(batches[0].num_columns(), 2);
    }
}

