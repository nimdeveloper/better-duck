//! Export a query result via the [Arrow C Data Interface] (feature `arrow`).
//!
//! DuckDB's C API can convert a result's schema and each data chunk into the Arrow C
//! Data Interface structs (`ArrowSchema` / `ArrowArray`). This module wraps that in a
//! safe, owning API **without pulling in an Arrow library** — it defines the stable
//! C Data Interface ABI structs directly and manages their `release` callbacks.
//!
//! A downstream consumer that has `arrow-rs` can reinterpret [`ArrowSchemaHandle`] /
//! [`ArrowArrayHandle`] (via [`as_ptr`](ArrowSchemaHandle::as_ptr)) as
//! `arrow::ffi::FFI_ArrowSchema` / `FFI_ArrowArray` — the layout is identical — and build
//! `RecordBatch`es / Arrow IPC from there.
//!
//! [Arrow C Data Interface]: https://arrow.apache.org/docs/format/CDataInterface.html
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::ffi::{c_char, c_void, CStr, CString};
use std::ptr;

use crate::error::{Error, Result};
use crate::ffi;
use crate::raw::data_chunk::DataChunk;
use crate::raw::result::DuckResult;

/// The Arrow C Data Interface `ArrowSchema` struct (stable ABI).
#[repr(C)]
struct CArrowSchema {
    format: *const c_char,
    name: *const c_char,
    metadata: *const c_char,
    flags: i64,
    n_children: i64,
    children: *mut *mut CArrowSchema,
    dictionary: *mut CArrowSchema,
    release: Option<unsafe extern "C" fn(*mut CArrowSchema)>,
    private_data: *mut c_void,
}

/// The Arrow C Data Interface `ArrowArray` struct (stable ABI).
#[repr(C)]
struct CArrowArray {
    length: i64,
    null_count: i64,
    offset: i64,
    n_buffers: i64,
    n_children: i64,
    buffers: *mut *const c_void,
    children: *mut *mut CArrowArray,
    dictionary: *mut CArrowArray,
    release: Option<unsafe extern "C" fn(*mut CArrowArray)>,
    private_data: *mut c_void,
}

impl CArrowSchema {
    fn empty() -> CArrowSchema {
        CArrowSchema {
            format: ptr::null(),
            name: ptr::null(),
            metadata: ptr::null(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        }
    }
}

impl CArrowArray {
    fn empty() -> CArrowArray {
        CArrowArray {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: ptr::null_mut(),
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        }
    }
}

/// An owned Arrow C Data Interface schema; calls its `release` callback on drop.
pub struct ArrowSchemaHandle(CArrowSchema);

/// An owned Arrow C Data Interface array; calls its `release` callback on drop.
pub struct ArrowArrayHandle(CArrowArray);

impl Drop for ArrowSchemaHandle {
    fn drop(&mut self) {
        if let Some(release) = self.0.release {
            // SAFETY: the C Data Interface contract: `release` frees the struct's
            // resources and nulls its own `release` field, so this fires at most once.
            unsafe { release(&mut self.0) };
        }
    }
}

impl Drop for ArrowArrayHandle {
    fn drop(&mut self) {
        if let Some(release) = self.0.release {
            // SAFETY: as above — release is idempotent per the C Data Interface.
            unsafe { release(&mut self.0) };
        }
    }
}

impl ArrowSchemaHandle {
    /// Number of top-level children (columns).
    pub fn n_children(&self) -> i64 {
        self.0.n_children
    }

    /// Pointer to the underlying `ArrowSchema`, reinterpretable as `FFI_ArrowSchema`.
    pub fn as_ptr(&self) -> *const c_void {
        ptr::addr_of!(self.0).cast()
    }

    /// Mutable pointer to the underlying `ArrowSchema`. Reinterpretable as
    /// `FFI_ArrowSchema`; used by the import path (`schema_from_arrow`), which reads
    /// the schema but leaves the `release` responsibility with this handle.
    pub fn as_mut_ptr(&mut self) -> *mut c_void {
        ptr::addr_of_mut!(self.0).cast()
    }

    /// Moves the C Data Interface schema into `dest`, transferring the `release`
    /// responsibility to the caller (this handle no longer releases). `dest` is an
    /// `ArrowSchema`-layout allocation — e.g. an arrow-rs `FFI_ArrowSchema`.
    ///
    /// # Safety
    /// `dest` must point to a writable `ArrowSchema`-layout struct that is not already
    /// holding a live Arrow schema (its contents are overwritten).
    pub unsafe fn export_to(
        mut self,
        dest: *mut c_void,
    ) {
        // SAFETY: `self.0` is a full `ArrowSchema`; `dest` is the same layout and writable.
        unsafe {
            ptr::copy_nonoverlapping(
                ptr::addr_of!(self.0).cast::<u8>(),
                dest.cast::<u8>(),
                std::mem::size_of::<CArrowSchema>(),
            );
        }
        // Release ownership moved to `dest`; neutralize ours so Drop is a no-op.
        self.0.release = None;
    }
}

impl ArrowArrayHandle {
    /// Number of rows in this chunk.
    pub fn len(&self) -> i64 {
        self.0.length
    }

    /// Whether this chunk has no rows.
    pub fn is_empty(&self) -> bool {
        self.0.length == 0
    }

    /// Pointer to the underlying `ArrowArray`, reinterpretable as `FFI_ArrowArray`.
    pub fn as_ptr(&self) -> *const c_void {
        ptr::addr_of!(self.0).cast()
    }

    /// Mutable pointer to the underlying `ArrowArray`, reinterpretable as
    /// `FFI_ArrowArray`. The import path consumes the array (DuckDB takes ownership of
    /// the data), so callers that pass this into `data_chunk_from_arrow` must let that
    /// function neutralize this handle's `release` on success to avoid a double free.
    pub fn as_mut_ptr(&mut self) -> *mut c_void {
        ptr::addr_of_mut!(self.0).cast()
    }

    /// Moves the C Data Interface array into `dest`, transferring the `release`
    /// responsibility to the caller (this handle no longer releases). `dest` is an
    /// `ArrowArray`-layout allocation — e.g. an arrow-rs `FFI_ArrowArray`.
    ///
    /// # Safety
    /// `dest` must point to a writable `ArrowArray`-layout struct that is not already
    /// holding a live Arrow array (its contents are overwritten).
    pub unsafe fn export_to(
        mut self,
        dest: *mut c_void,
    ) {
        // SAFETY: `self.0` is a full `ArrowArray`; `dest` is the same layout and writable.
        unsafe {
            ptr::copy_nonoverlapping(
                ptr::addr_of!(self.0).cast::<u8>(),
                dest.cast::<u8>(),
                std::mem::size_of::<CArrowArray>(),
            );
        }
        // Release ownership moved to `dest`; neutralize ours so Drop is a no-op.
        self.0.release = None;
    }
}

/// A query result exported to Arrow: one schema plus one array per DuckDB data chunk.
pub struct ArrowResult {
    // Field order matters: arrays (and their release) must run before the chunks they
    // may reference are destroyed.
    schema: ArrowSchemaHandle,
    arrays: Vec<ArrowArrayHandle>,
    _chunks: Vec<DataChunk>,
}

impl ArrowResult {
    /// The Arrow schema handle.
    pub fn schema(&self) -> &ArrowSchemaHandle {
        &self.schema
    }

    /// The per-chunk Arrow array handles.
    pub fn arrays(&self) -> &[ArrowArrayHandle] {
        &self.arrays
    }

    /// Total number of rows across all chunks.
    pub fn row_count(&self) -> i64 {
        self.arrays.iter().map(ArrowArrayHandle::len).sum()
    }

    /// Consumes the result into its parts. The returned data chunks must be kept alive
    /// until the arrays have been consumed/released, in case an array references chunk
    /// data.
    pub fn into_parts(self) -> (ArrowSchemaHandle, Vec<ArrowArrayHandle>, Vec<DataChunk>) {
        (self.schema, self.arrays, self._chunks)
    }
}

/// Consumes a raw error-data handle, returning `Err` if it carries a message.
fn take_error(mut err: ffi::duckdb_error_data) -> Result<()> {
    if err.is_null() {
        return Ok(());
    }
    // SAFETY: `err` is a non-null `duckdb_error_data` valid for these reads.
    let result = if unsafe { ffi::duckdb_error_data_has_error(err) } {
        // SAFETY: `err` carries an error, so its message is a valid C string.
        let message = unsafe { CStr::from_ptr(ffi::duckdb_error_data_message(err)) }
            .to_string_lossy()
            .into_owned();
        Err(Error::DuckDBFailure(ffi::Error::new(ffi::DuckDBError), Some(message)))
    } else {
        Ok(())
    };
    // SAFETY: `err` is a non-null handle destroyed exactly once here.
    unsafe { ffi::duckdb_destroy_error_data(&mut err) };
    result
}

/// Exports `result` to the Arrow C Data Interface: builds the schema, then converts each
/// data chunk to an Arrow array. The source chunks are retained so the arrays never
/// outlive data they may reference.
pub(crate) fn export(mut result: DuckResult) -> Result<ArrowResult> {
    let column_count = result.column_count();
    let result_ptr = result.as_mut_ptr();

    // SAFETY: `result_ptr` is a valid `duckdb_result`; arrow options are owned and
    // destroyed below. `duckdb_arrow_options` is a plain pointer (Copy).
    let mut arrow_options = unsafe { ffi::duckdb_result_get_arrow_options(result_ptr) };

    let outcome = build(&mut result, result_ptr, arrow_options, column_count);

    // SAFETY: `arrow_options` was returned by `duckdb_result_get_arrow_options`.
    unsafe { ffi::duckdb_destroy_arrow_options(&mut arrow_options) };
    outcome
}

fn build(
    result: &mut DuckResult,
    result_ptr: *mut ffi::duckdb_result,
    arrow_options: ffi::duckdb_arrow_options,
    column_count: u64,
) -> Result<ArrowResult> {
    // Gather the raw column logical types and names for the schema conversion.
    let mut types: Vec<ffi::duckdb_logical_type> = Vec::with_capacity(column_count as usize);
    let mut names: Vec<CString> = Vec::with_capacity(column_count as usize);
    for col in 0..column_count {
        // SAFETY: `result_ptr` is valid; `col` is in range; the type is owned here.
        types.push(unsafe { ffi::duckdb_column_logical_type(result_ptr, col) });
        let name = result.column_name(col as usize).unwrap_or("");
        names.push(CString::new(name).map_err(|e| {
            Error::DuckDBFailure(ffi::Error::new(ffi::DuckDBError), Some(e.to_string()))
        })?);
    }
    let mut name_ptrs: Vec<*const c_char> = names.iter().map(|c| c.as_ptr()).collect();

    let mut schema = CArrowSchema::empty();
    // SAFETY: `types`/`name_ptrs` are valid arrays of `column_count`; `out_schema` points
    // to a correctly-laid-out C Data Interface struct that DuckDB fills.
    let schema_err = unsafe {
        ffi::duckdb_to_arrow_schema(
            arrow_options,
            types.as_mut_ptr(),
            name_ptrs.as_mut_ptr(),
            column_count,
            ptr::addr_of_mut!(schema).cast::<ffi::ArrowSchema>(),
        )
    };
    for mut logical_type in types {
        // SAFETY: each was returned by `duckdb_column_logical_type`; destroy once.
        unsafe { ffi::duckdb_destroy_logical_type(&mut logical_type) };
    }
    take_error(schema_err)?;
    let schema = ArrowSchemaHandle(schema);

    let mut arrays: Vec<ArrowArrayHandle> = Vec::new();
    let mut chunks: Vec<DataChunk> = Vec::new();
    while let Some(chunk) = DataChunk::from_result(result) {
        let chunk = chunk?;
        let mut array = CArrowArray::empty();
        // SAFETY: `*chunk` is a valid data chunk; `out_arrow_array` points to a
        // correctly-laid-out C Data Interface struct that DuckDB fills.
        let array_err = unsafe {
            ffi::duckdb_data_chunk_to_arrow(
                arrow_options,
                *chunk,
                ptr::addr_of_mut!(array).cast::<ffi::ArrowArray>(),
            )
        };
        take_error(array_err)?;
        arrays.push(ArrowArrayHandle(array));
        chunks.push(chunk);
    }

    Ok(ArrowResult { schema, arrays, _chunks: chunks })
}

// ---------------------------------------------------------------------------
// Import: Arrow C Data Interface -> DuckDB
// ---------------------------------------------------------------------------

/// An owned DuckDB "Arrow converted schema" (`duckdb_arrow_converted_schema`) — the
/// per-column type information DuckDB derives from an Arrow schema, needed to turn
/// Arrow arrays into DuckDB data chunks. Destroyed on drop.
///
/// Building block for the chunk-level import path used by the `polars` feature.
#[cfg(test)]
pub struct ArrowConvertedSchema(ffi::duckdb_arrow_converted_schema);

#[cfg(test)]
impl Drop for ArrowConvertedSchema {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: `self.0` was produced by `duckdb_schema_from_arrow` and is
            // destroyed exactly once (null guard prevents re-entry).
            unsafe { ffi::duckdb_destroy_arrow_converted_schema(&mut self.0) };
        }
    }
}

/// Transforms an Arrow C Data Interface schema into DuckDB's converted schema.
///
/// `schema_ptr` must point to a valid Arrow C Data Interface `ArrowSchema`. DuckDB
/// only reads it — the caller retains the `release` responsibility for that schema.
#[cfg(test)]
pub(crate) fn schema_from_arrow(
    con: ffi::duckdb_connection,
    schema_ptr: *mut ffi::ArrowSchema,
) -> Result<ArrowConvertedSchema> {
    let mut converted: ffi::duckdb_arrow_converted_schema = ptr::null_mut();
    // SAFETY: `con` is a live connection; `schema_ptr` points to a valid ArrowSchema;
    // `converted` receives an owned handle destroyed by `ArrowConvertedSchema`'s Drop.
    let err = unsafe { ffi::duckdb_schema_from_arrow(con, schema_ptr, &mut converted) };
    take_error(err)?;
    if converted.is_null() {
        return Err(Error::DuckDBFailure(
            ffi::Error::new(ffi::DuckDBError),
            Some("duckdb_schema_from_arrow returned a null converted schema".to_owned()),
        ));
    }
    Ok(ArrowConvertedSchema(converted))
}

/// Transforms one Arrow C Data Interface array (a chunk) into a DuckDB [`DataChunk`],
/// using the converted schema from [`schema_from_arrow`].
///
/// DuckDB takes ownership of the array's data, so on success this neutralizes the
/// handle's `release` callback (preventing a double free when the handle drops).
#[cfg(test)]
pub(crate) fn chunk_from_arrow(
    con: ffi::duckdb_connection,
    array: &mut ArrowArrayHandle,
    converted: &ArrowConvertedSchema,
) -> Result<DataChunk> {
    let mut out: ffi::duckdb_data_chunk = ptr::null_mut();
    let array_ptr = ptr::addr_of_mut!(array.0).cast::<ffi::ArrowArray>();
    // SAFETY: `con` is live; `array_ptr` is a valid ArrowArray; `converted.0` is a live
    // converted schema; `out` receives an owned chunk destroyed by `DataChunk`'s Drop.
    let err = unsafe { ffi::duckdb_data_chunk_from_arrow(con, array_ptr, converted.0, &mut out) };
    take_error(err)?;
    // DuckDB now owns the array's data; make sure our handle's Drop does not release it.
    array.0.release = None;
    DataChunk::new(out)
}

/// Registers an Arrow C Data Interface stream as a DuckDB view named `name`, queryable
/// for as long as the stream (and the returned guard) live.
///
/// `stream` must point to a valid Arrow C Data Interface `ArrowArrayStream` and must
/// outlive the returned [`ArrowView`]. DuckDB reads from the stream while the view is
/// queried.
///
/// Note: this uses DuckDB's `duckdb_arrow_scan`, which carries an upstream deprecation
/// notice; the chunk-level [`schema_from_arrow`]/[`chunk_from_arrow`] path is the
/// non-deprecated alternative.
pub(crate) fn arrow_scan(
    con: ffi::duckdb_connection,
    name: &str,
    stream: *mut c_void,
) -> Result<()> {
    let c_name = CString::new(name).map_err(|e| {
        Error::DuckDBFailure(ffi::Error::new(ffi::DuckDBError), Some(e.to_string()))
    })?;
    // SAFETY: `con` is live; `c_name` is a valid NUL-terminated string; `stream` is a
    // caller-owned Arrow C Data Interface stream reinterpreted as `duckdb_arrow_stream`
    // (the deprecated scan API treats the handle as an `ArrowArrayStream*`).
    let state =
        unsafe { ffi::duckdb_arrow_scan(con, c_name.as_ptr(), stream as ffi::duckdb_arrow_stream) };
    if state != ffi::DuckDBSuccess {
        return Err(Error::DuckDBFailure(
            ffi::Error::new(ffi::DuckDBError),
            Some(format!("duckdb_arrow_scan failed to register view '{name}'")),
        ));
    }
    Ok(())
}

/// A DuckDB view backed by a registered Arrow stream (see
/// [`Connection::register_arrow`](crate::connection::Connection::register_arrow)).
///
/// Dropping the guard runs `DROP VIEW IF EXISTS` for the view. The guard borrows the
/// connection so it cannot outlive it; the caller is responsible for keeping the
/// backing Arrow stream alive for at least as long as this guard.
pub struct ArrowView<'c> {
    conn: &'c crate::connection::Connection,
    name: String,
}

impl<'c> ArrowView<'c> {
    pub(crate) fn new(
        conn: &'c crate::connection::Connection,
        name: String,
    ) -> Self {
        ArrowView { conn, name }
    }

    /// The registered view's name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for ArrowView<'_> {
    fn drop(&mut self) {
        // Escape embedded quotes so an odd view name can't break the identifier.
        let sql = format!("DROP VIEW IF EXISTS \"{}\"", self.name.replace('"', "\"\""));
        let Ok(c_sql) = CString::new(sql) else { return };
        // SAFETY: the borrowed connection is alive for the guard's lifetime; `res` is a
        // stack `duckdb_result` that DuckDB fills and we destroy unconditionally.
        unsafe {
            let mut res: ffi::duckdb_result = std::mem::zeroed();
            ffi::duckdb_query(self.conn.raw_con(), c_sql.as_ptr(), &mut res);
            ffi::duckdb_destroy_result(&mut res);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{chunk_from_arrow, schema_from_arrow};
    use crate::connection::Connection;

    #[test]
    fn exports_schema_and_arrays() {
        let mut conn = Connection::open_in_memory().unwrap();
        let arrow =
            conn.query_arrow("SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4", &mut []).unwrap();
        assert_eq!(arrow.schema().n_children(), 2, "two columns");
        assert_eq!(arrow.row_count(), 2, "two rows across chunks");
        assert!(!arrow.arrays().is_empty());
        // Dropping `arrow` releases the C Data Interface handles + chunks without crashing.
    }

    #[test]
    fn empty_result_exports_zero_rows() {
        let mut conn = Connection::open_in_memory().unwrap();
        let arrow = conn.query_arrow("SELECT 1 AS a WHERE 1 = 0", &mut []).unwrap();
        assert_eq!(arrow.schema().n_children(), 1);
        assert_eq!(arrow.row_count(), 0);
    }

    #[test]
    fn import_round_trips_exported_schema_and_arrays() {
        // Export a result to the Arrow C Data Interface, then feed the schema + arrays
        // back through the import path (schema_from_arrow + chunk_from_arrow) to prove
        // the conversion FFI works without pulling in an Arrow library.
        let mut conn = Connection::open_in_memory().unwrap();
        let arrow =
            conn.query_arrow("SELECT i AS a, i * 2 AS b FROM range(5) t(i)", &mut []).unwrap();
        let exported_rows = arrow.row_count();
        assert_eq!(exported_rows, 5);

        let (mut schema, mut arrays, _chunks) = arrow.into_parts();
        let con = conn.raw_con();

        // `con` is live; `schema` is a valid exported Arrow schema.
        let converted =
            schema_from_arrow(con, schema.as_mut_ptr().cast::<crate::ffi::ArrowSchema>()).unwrap();

        let mut imported_rows: u64 = 0;
        for array in &mut arrays {
            // `con` is live; `array` is a valid exported Arrow array; `converted` matches
            // the schema. `chunk_from_arrow` neutralizes the array's release on success.
            let chunk = chunk_from_arrow(con, array, &converted).unwrap();
            imported_rows += chunk.row_count();
        }

        assert_eq!(
            imported_rows, exported_rows as u64,
            "every exported row must survive the Arrow round-trip"
        );
    }
}
