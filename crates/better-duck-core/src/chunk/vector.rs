//! Read and write views over a single column of a [`DataChunkHandle`](super::DataChunkHandle).
//!
//! This is a deliberate divergence from the `duckdb` crate's design: its
//! `flat_vector(&self)` accessor lets safe code obtain two overlapping
//! `&mut [T]` views of the same column, which is documented there as a known
//! aliasing hole. Splitting read (`VectorRef`, built from `&self`, freely
//! aliasable) from write (`VectorMut`, built from `&mut self`, exclusive by the
//! borrow checker) closes that hole structurally instead of by convention.

use std::{ffi::c_char, marker::PhantomData, ptr, slice, str};

use crate::{
    error::{DuckDBConversionError, Error, Result},
    ffi::{
        duckdb_destroy_logical_type, duckdb_get_type_id, duckdb_string_t, duckdb_string_t_data,
        duckdb_string_t_length, duckdb_validity_row_is_valid, duckdb_validity_set_row_invalid,
        duckdb_validity_set_row_valid, duckdb_validity_set_row_validity, duckdb_vector,
        duckdb_vector_assign_string_element_len, duckdb_vector_ensure_validity_writable,
        duckdb_vector_get_column_type, duckdb_vector_get_data, duckdb_vector_get_validity,
        DUCKDB_TYPE, DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
        DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE, DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
        DUCKDB_TYPE_DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT,
        DUCKDB_TYPE_DUCKDB_TYPE_TINYINT, DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER, DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT,
        DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT,
    },
    types::{
        numeric::{hugeint_from_i128, i128_from_hugeint, u128_from_uhugeint, uhugeint_from_u128},
        value::DuckValue,
    },
};

use super::UdfResult;

mod private {
    /// Prevents [`super::FixedWidth`] from being implemented outside this crate.
    pub trait Sealed {}
}

/// A Rust type with a fixed-width DuckDB physical representation, safe to
/// reinterpret as a packed slice via [`VectorRef::as_slice`].
///
/// Sealed, and deliberately narrower than [`ScalarArg`]/[`ScalarRet`]:
/// `String`/`&str` are variable-length (no fixed stride to slice over), and
/// `i128`/`u128` are excluded even though they're fixed-width, because
/// DuckDB's physical HUGEINT/UHUGEINT storage is a `duckdb_hugeint`/
/// `duckdb_uhugeint` two-field struct — see the comment on the `i128`/`u128`
/// [`ScalarArg`] impls below for why this crate never assumes that memcpys
/// to a native `i128`/`u128`, even though the bytes happen to match on a
/// little-endian target.
pub trait FixedWidth: private::Sealed + Copy {
    /// The `DUCKDB_TYPE` this Rust type corresponds to.
    const DUCKDB_TYPE_ID: DUCKDB_TYPE;
}

macro_rules! impl_fixed_width {
    ($($rust_type:ty => $tag:expr),+ $(,)?) => {
        $(
            impl private::Sealed for $rust_type {}
            impl FixedWidth for $rust_type {
                const DUCKDB_TYPE_ID: DUCKDB_TYPE = $tag;
            }
        )+
    };
}

impl_fixed_width!(
    bool => DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
    i8 => DUCKDB_TYPE_DUCKDB_TYPE_TINYINT,
    i16 => DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT,
    i32 => DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
    i64 => DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
    u8 => DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT,
    u16 => DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT,
    u32 => DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER,
    u64 => DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT,
    f32 => DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
    f64 => DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
);

/// A read-only view of one column of a data chunk.
///
/// Built from `&DataChunkHandle`, so multiple `VectorRef`s over different (or
/// the same) columns may coexist freely — they only ever read.
pub struct VectorRef<'a> {
    ptr: duckdb_vector,
    _marker: PhantomData<&'a ()>,
}

impl<'a> VectorRef<'a> {
    /// Wraps a raw vector pointer as a read-only view.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid `duckdb_vector` that stays allocated and is not
    /// mutated through any other handle for the lifetime `'a`.
    pub(crate) unsafe fn new(ptr: duckdb_vector) -> Self {
        Self { ptr, _marker: PhantomData }
    }

    /// Returns `true` if the value at `row` is `NULL`.
    pub fn is_null(
        &self,
        row: usize,
    ) -> bool {
        // SAFETY: `self.ptr` is a valid vector for the lifetime of `self`.
        let validity = unsafe { duckdb_vector_get_validity(self.ptr) };
        // SAFETY: `row` is caller-guaranteed to be within the chunk's row count
        // (enforced by `DataChunkHandle`, the only safe way to obtain a
        // `VectorRef`). A null `validity` means "no NULLs in this vector", which
        // `duckdb_validity_row_is_valid` handles by returning `true`.
        !unsafe { duckdb_validity_row_is_valid(validity, row as u64) }
    }

    /// Reads the value at `row` as `T`.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying value cannot be converted to `T`.
    pub fn get<T: ScalarArg<'a>>(
        &self,
        row: usize,
    ) -> UdfResult<T> {
        T::read(self, row)
    }

    /// Reinterprets the first `len` rows of this column as a packed
    /// `&[T]` slice — a zero-conversion read, unlike
    /// [`get`](Self::get)/[`as_duck_value`](Self::as_duck_value), which decode
    /// one value at a time. `len` is typically the enclosing chunk's
    /// [`len`](super::DataChunkHandle::len).
    ///
    /// NULL rows are *not* filtered or masked here — a NULL row's slot holds
    /// whatever bytes DuckDB left there, which callers must treat as
    /// meaningless unless they've separately checked [`validity`](Self::validity).
    ///
    /// # Errors
    ///
    /// Returns an error if this column's DuckDB type does not match `T`.
    pub fn as_slice<T: FixedWidth>(
        &self,
        len: usize,
    ) -> Result<&'a [T]> {
        // SAFETY: `self.ptr` is a valid vector; `duckdb_vector_get_column_type`
        // always succeeds for a non-null vector and returns an owned logical type.
        let mut lt = unsafe { duckdb_vector_get_column_type(self.ptr) };
        // SAFETY: `lt` was just obtained above and is a valid logical type.
        let found = unsafe { duckdb_get_type_id(lt) };
        // SAFETY: `lt` was allocated above; destroy exactly once.
        unsafe { duckdb_destroy_logical_type(&mut lt) };
        if found != T::DUCKDB_TYPE_ID {
            return Err(Error::ConversionError(DuckDBConversionError::TypeMismatch {
                expected: T::DUCKDB_TYPE_ID,
                found,
            }));
        }
        // SAFETY: `self.ptr` is a valid vector for lifetime `'a`, holding at
        // least `len` rows (caller-guaranteed, per this type's own doc
        // contract) of physical type `T` (just confirmed above). The pointer
        // is not tied to `&self`'s borrow: it is valid for `'a`, matching
        // `ScalarArg::read`'s existing `&'v str`/`String` convention.
        let data = unsafe { duckdb_vector_get_data(self.ptr) as *const T };
        // SAFETY: `data` is non-null (a valid vector's data pointer is never
        // null) and points to a packed array of at least `len` `T`s for
        // lifetime `'a`, per the same reasoning as above.
        Ok(unsafe { slice::from_raw_parts(data, len) })
    }

    /// Returns the raw validity bitmap for the first `len` rows — one bit per
    /// row (1 = valid, 0 = NULL), packed into `u64` words, row `r` at bit
    /// `r % 64` of word `r / 64`. `None` means no NULLs are present in this
    /// vector at all (DuckDB's own "no validity mask allocated" shortcut —
    /// every row is implicitly valid).
    ///
    /// For checking one row, prefer [`is_null`](Self::is_null); this is for
    /// callers doing their own bulk NULL handling alongside
    /// [`as_slice`](Self::as_slice).
    pub fn validity(
        &self,
        len: usize,
    ) -> Option<&'a [u64]> {
        // SAFETY: `self.ptr` is a valid vector for lifetime `'a`.
        let validity = unsafe { duckdb_vector_get_validity(self.ptr) };
        if validity.is_null() {
            return None;
        }
        let word_count = len.div_ceil(64);
        // SAFETY: `validity` is non-null (checked above) and, per the C API,
        // points to at least `ceil(row_count / 64)` `u64` words for lifetime
        // `'a`; `len` is caller-guaranteed to be within the chunk's row count.
        Some(unsafe { slice::from_raw_parts(validity, word_count) })
    }

    /// Reads the value at `row` as a dynamically-typed [`DuckValue`], reusing
    /// the same decoder used for ordinary query results.
    ///
    /// # Errors
    ///
    /// Returns an error if the value cannot be decoded.
    pub fn as_duck_value(
        &self,
        row: usize,
    ) -> Result<DuckValue> {
        // SAFETY: `self.ptr` is a valid vector; `duckdb_vector_get_column_type`
        // always succeeds for a non-null vector and returns an owned logical type.
        let mut lt = unsafe { duckdb_vector_get_column_type(self.ptr) };
        // SAFETY: `lt` was just obtained above and is a valid logical type.
        let type_id = unsafe { duckdb_get_type_id(lt) };
        // SAFETY: `lt` was allocated above; destroy exactly once.
        unsafe { duckdb_destroy_logical_type(&mut lt) };
        DuckValue::from_duckdb_vec(self.ptr, type_id, row as u64).map_err(Error::ConversionError)
    }

    /// The raw pointer backing this view, for use by [`ScalarArg`] impls.
    pub(crate) fn raw(&self) -> duckdb_vector {
        self.ptr
    }
}

/// An exclusive, writable view of one column of a data chunk.
///
/// Built from `&mut DataChunkHandle`, so the borrow checker guarantees no other
/// `VectorRef`/`VectorMut` over the same column coexists with it.
pub struct VectorMut<'a> {
    ptr: duckdb_vector,
    _marker: PhantomData<&'a mut ()>,
}

impl<'a> VectorMut<'a> {
    /// Wraps a raw vector pointer as an exclusive, writable view.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid `duckdb_vector` that stays allocated for the
    /// lifetime `'a`, and no other live handle may read or write through it for
    /// that same lifetime.
    pub(crate) unsafe fn new(ptr: duckdb_vector) -> Self {
        Self { ptr, _marker: PhantomData }
    }

    /// Marks the value at `row` as `NULL`.
    pub fn set_null(
        &mut self,
        row: usize,
    ) {
        // SAFETY: `self.ptr` is a valid, exclusively-held vector.
        unsafe { duckdb_vector_ensure_validity_writable(self.ptr) };
        // SAFETY: the call above guarantees `duckdb_vector_get_validity` now
        // returns a valid, writable, non-null validity mask.
        let validity = unsafe { duckdb_vector_get_validity(self.ptr) };
        // SAFETY: `validity` is non-null (guaranteed above); `row` is
        // caller-guaranteed to be within the chunk's row count.
        unsafe { duckdb_validity_set_row_invalid(validity, row as u64) };
    }

    /// Marks the value at `row` as valid (not `NULL`) — the inverse of
    /// [`set_null`](Self::set_null), for un-marking a row on a reused chunk
    /// (see [`DataChunkHandle::reset`](super::DataChunkHandle::reset)) whose
    /// validity mask may still carry a previous batch's NULL marks.
    pub fn set_row_valid(
        &mut self,
        row: usize,
    ) {
        // SAFETY: `self.ptr` is a valid, exclusively-held vector.
        unsafe { duckdb_vector_ensure_validity_writable(self.ptr) };
        // SAFETY: the call above guarantees this now returns a valid,
        // writable, non-null validity mask.
        let validity = unsafe { duckdb_vector_get_validity(self.ptr) };
        // SAFETY: `validity` is non-null (guaranteed above); `row` is
        // caller-guaranteed to be within the chunk's row count.
        unsafe { duckdb_validity_set_row_valid(validity, row as u64) };
    }

    /// Marks the value at `row` as valid or `NULL`, per `valid` — the general
    /// form of [`set_null`](Self::set_null)/[`set_row_valid`](Self::set_row_valid).
    pub fn set_row_validity(
        &mut self,
        row: usize,
        valid: bool,
    ) {
        // SAFETY: `self.ptr` is a valid, exclusively-held vector.
        unsafe { duckdb_vector_ensure_validity_writable(self.ptr) };
        // SAFETY: the call above guarantees this now returns a valid,
        // writable, non-null validity mask.
        let validity = unsafe { duckdb_vector_get_validity(self.ptr) };
        // SAFETY: `validity` is non-null (guaranteed above); `row` is
        // caller-guaranteed to be within the chunk's row count.
        unsafe { duckdb_validity_set_row_validity(validity, row as u64, valid) };
    }

    /// Writes `value` at `row`.
    ///
    /// # Errors
    ///
    /// Returns an error if `T`'s conversion to a DuckDB value fails.
    pub fn set<T: ScalarRet>(
        &mut self,
        row: usize,
        value: T,
    ) -> UdfResult<()> {
        value.write(self, row)
    }

    /// The raw pointer backing this view, for use by [`ScalarRet`] impls.
    pub(crate) fn raw(&mut self) -> duckdb_vector {
        self.ptr
    }
}

/// A Rust type readable from one row of a [`VectorRef`].
pub trait ScalarArg<'v>: Sized {
    /// Reads the value at `row` from `v`.
    ///
    /// # Errors
    ///
    /// Returns an error if the value cannot be converted to `Self`.
    fn read(
        v: &VectorRef<'v>,
        row: usize,
    ) -> UdfResult<Self>;
}

/// A Rust type writable into one row of a [`VectorMut`].
pub trait ScalarRet {
    /// Writes `self` at `row` in `v`.
    ///
    /// # Errors
    ///
    /// Returns an error if `self` cannot be converted to a DuckDB value.
    fn write(
        self,
        v: &mut VectorMut<'_>,
        row: usize,
    ) -> UdfResult<()>;
}

// Fixed-width scalars: bool + all non-128-bit integers + floats.
//
// Physical storage for these types is a packed array of the natural C
// representation, matching the existing `simple_type_conversion!` macro's read
// path in `types/value.rs` — a direct pointer cast at the row offset.
macro_rules! impl_scalar_fixed {
    ($($rust_type:ty),+ $(,)?) => {
        $(
            impl<'v> ScalarArg<'v> for $rust_type {
                fn read(v: &VectorRef<'v>, row: usize) -> UdfResult<Self> {
                    // SAFETY: `v.raw()` is a valid vector of matching physical type for
                    // the lifetime of `v`; `row` is caller-guaranteed in range.
                    let data = unsafe { duckdb_vector_get_data(v.raw()) as *const $rust_type };
                    // SAFETY: `data` points to a packed array of at least the chunk's row
                    // count entries; `row` is in range.
                    Ok(unsafe { *data.add(row) })
                }
            }
            impl ScalarRet for $rust_type {
                fn write(self, v: &mut VectorMut<'_>, row: usize) -> UdfResult<()> {
                    // SAFETY: `v.raw()` is a valid, exclusively-held vector of matching
                    // physical type; `row` is caller-guaranteed in range.
                    let data = unsafe { duckdb_vector_get_data(v.raw()) as *mut $rust_type };
                    // SAFETY: `data` points to a packed array of at least the chunk's row
                    // count entries; `row` is in range; writing a fresh value here does not
                    // drop anything in place.
                    unsafe { ptr::write(data.add(row), self) };
                    Ok(())
                }
            }
        )+
    };
}

impl_scalar_fixed!(bool, i8, i16, i32, i64, u8, u16, u32, u64, f32, f64);

// 128-bit integers: never memcpy a native i128/u128 — DuckDB's physical HUGEINT
// storage is a `duckdb_hugeint { lower: u64, upper: i64 }` pair (UHUGEINT
// likewise unsigned), and constructing it explicitly is layout-independent.
impl<'v> ScalarArg<'v> for i128 {
    fn read(
        v: &VectorRef<'v>,
        row: usize,
    ) -> UdfResult<Self> {
        // SAFETY: `v.raw()` is a valid HUGEINT vector for the lifetime of `v`.
        let data = unsafe { duckdb_vector_get_data(v.raw()) as *const crate::ffi::duckdb_hugeint };
        // SAFETY: `data` points to a packed array of `duckdb_hugeint`; `row` is in range.
        Ok(i128_from_hugeint(unsafe { *data.add(row) }))
    }
}
impl ScalarRet for i128 {
    fn write(
        self,
        v: &mut VectorMut<'_>,
        row: usize,
    ) -> UdfResult<()> {
        // SAFETY: `v.raw()` is a valid, exclusively-held HUGEINT vector.
        let data = unsafe { duckdb_vector_get_data(v.raw()) as *mut crate::ffi::duckdb_hugeint };
        // SAFETY: `data` points to a packed array of `duckdb_hugeint`; `row` is in range.
        unsafe { ptr::write(data.add(row), hugeint_from_i128(self)) };
        Ok(())
    }
}
impl<'v> ScalarArg<'v> for u128 {
    fn read(
        v: &VectorRef<'v>,
        row: usize,
    ) -> UdfResult<Self> {
        // SAFETY: `v.raw()` is a valid UHUGEINT vector for the lifetime of `v`.
        let data = unsafe { duckdb_vector_get_data(v.raw()) as *const crate::ffi::duckdb_uhugeint };
        // SAFETY: `data` points to a packed array of `duckdb_uhugeint`; `row` is in range.
        Ok(u128_from_uhugeint(unsafe { *data.add(row) }))
    }
}
impl ScalarRet for u128 {
    fn write(
        self,
        v: &mut VectorMut<'_>,
        row: usize,
    ) -> UdfResult<()> {
        // SAFETY: `v.raw()` is a valid, exclusively-held UHUGEINT vector.
        let data = unsafe { duckdb_vector_get_data(v.raw()) as *mut crate::ffi::duckdb_uhugeint };
        // SAFETY: `data` points to a packed array of `duckdb_uhugeint`; `row` is in range.
        unsafe { ptr::write(data.add(row), uhugeint_from_u128(self)) };
        Ok(())
    }
}

/// Reads the `duckdb_string_t` at `row` directly from the vector's own data
/// buffer (not a local copy), so the returned pointer — inline or heap — stays
/// valid for the vector's lifetime `'v`.
fn read_str_bytes<'v>(
    v: &VectorRef<'v>,
    row: usize,
) -> &'v [u8] {
    // SAFETY: `v.raw()` is a valid VARCHAR/BLOB vector for lifetime `'v`.
    let data = unsafe { duckdb_vector_get_data(v.raw()) as *mut duckdb_string_t };
    // SAFETY: `data` points to a packed array of `duckdb_string_t`; `row` is in range.
    let str_ptr = unsafe { data.add(row) };
    // SAFETY: `str_ptr` points directly into the vector's own storage (inline
    // bytes live in the struct itself; out-of-line bytes are DuckDB-owned), so
    // the returned pointer is valid for `'v` either way.
    let c_ptr = unsafe { duckdb_string_t_data(str_ptr) };
    // SAFETY: `str_ptr` is valid to read (see above).
    let len = unsafe { duckdb_string_t_length(*str_ptr) } as usize;
    // SAFETY: `c_ptr` is valid for `len` bytes for lifetime `'v`, per the above.
    unsafe { slice::from_raw_parts(c_ptr.cast::<u8>(), len) }
}

fn write_str_bytes(
    v: &mut VectorMut<'_>,
    row: usize,
    bytes: &[u8],
) {
    // SAFETY: `v.raw()` is a valid, exclusively-held VARCHAR/BLOB vector;
    // `bytes.as_ptr()`/`bytes.len()` describe a valid byte range for the call.
    unsafe {
        duckdb_vector_assign_string_element_len(
            v.raw(),
            row as u64,
            bytes.as_ptr() as *const c_char,
            bytes.len() as u64,
        )
    };
}

impl<'v> ScalarArg<'v> for &'v str {
    fn read(
        v: &VectorRef<'v>,
        row: usize,
    ) -> UdfResult<Self> {
        str::from_utf8(read_str_bytes(v, row)).map_err(|e| Box::new(e) as _)
    }
}
impl<'v> ScalarArg<'v> for String {
    fn read(
        v: &VectorRef<'v>,
        row: usize,
    ) -> UdfResult<Self> {
        <&str as ScalarArg<'v>>::read(v, row).map(str::to_owned)
    }
}
impl ScalarRet for &str {
    fn write(
        self,
        v: &mut VectorMut<'_>,
        row: usize,
    ) -> UdfResult<()> {
        write_str_bytes(v, row, self.as_bytes());
        Ok(())
    }
}
impl ScalarRet for String {
    fn write(
        self,
        v: &mut VectorMut<'_>,
        row: usize,
    ) -> UdfResult<()> {
        write_str_bytes(v, row, self.as_bytes());
        Ok(())
    }
}

impl<'v, T: ScalarArg<'v>> ScalarArg<'v> for Option<T> {
    fn read(
        v: &VectorRef<'v>,
        row: usize,
    ) -> UdfResult<Self> {
        if v.is_null(row) {
            Ok(None)
        } else {
            T::read(v, row).map(Some)
        }
    }
}
impl<T: ScalarRet> ScalarRet for Option<T> {
    fn write(
        self,
        v: &mut VectorMut<'_>,
        row: usize,
    ) -> UdfResult<()> {
        match self {
            Some(value) => value.write(v, row),
            None => {
                v.set_null(row);
                Ok(())
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::data_chunk::DataChunkHandle;
    use crate::chunk::logical_type::LogicalType;

    #[test]
    fn i32_round_trips() {
        let types = [LogicalType::of::<i32>().unwrap()];
        let mut chunk = DataChunkHandle::new(&types).unwrap();
        {
            let mut vec = chunk.vector_mut(0).unwrap();
            vec.set(0, 42i32).unwrap();
        }
        let vec = chunk.vector(0).unwrap();
        let got: i32 = vec.get(0).unwrap();
        assert_eq!(got, 42);
    }

    #[test]
    fn i128_round_trips_negative() {
        let types = [LogicalType::of::<i128>().unwrap()];
        let mut chunk = DataChunkHandle::new(&types).unwrap();
        let value: i128 = -170_141_183_460_469_231_731_687_303_715_884_105_000;
        {
            let mut vec = chunk.vector_mut(0).unwrap();
            vec.set(0, value).unwrap();
        }
        let vec = chunk.vector(0).unwrap();
        let got: i128 = vec.get(0).unwrap();
        assert_eq!(got, value);
    }

    #[test]
    fn short_string_round_trips() {
        let types = [LogicalType::of::<String>().unwrap()];
        let mut chunk = DataChunkHandle::new(&types).unwrap();
        {
            let mut vec = chunk.vector_mut(0).unwrap();
            vec.set(0, "hi").unwrap();
        }
        let vec = chunk.vector(0).unwrap();
        let got: String = vec.get(0).unwrap();
        assert_eq!(got, "hi");
        let got_ref: &str = vec.get(0).unwrap();
        assert_eq!(got_ref, "hi");
    }

    #[test]
    fn long_string_round_trips() {
        let types = [LogicalType::of::<String>().unwrap()];
        let mut chunk = DataChunkHandle::new(&types).unwrap();
        let long = "x".repeat(64);
        {
            let mut vec = chunk.vector_mut(0).unwrap();
            vec.set(0, long.as_str()).unwrap();
        }
        let vec = chunk.vector(0).unwrap();
        let got: String = vec.get(0).unwrap();
        assert_eq!(got, long);
    }

    #[test]
    fn null_round_trips_via_option() {
        let types = [LogicalType::of::<i32>().unwrap()];
        let mut chunk = DataChunkHandle::new(&types).unwrap();
        {
            let mut vec = chunk.vector_mut(0).unwrap();
            vec.set(0, None::<i32>).unwrap();
        }
        let vec = chunk.vector(0).unwrap();
        assert!(vec.is_null(0));
        let got: Option<i32> = vec.get(0).unwrap();
        assert_eq!(got, None);
    }

    #[test]
    fn some_round_trips_via_option() {
        let types = [LogicalType::of::<i32>().unwrap()];
        let mut chunk = DataChunkHandle::new(&types).unwrap();
        {
            let mut vec = chunk.vector_mut(0).unwrap();
            vec.set(0, Some(7i32)).unwrap();
        }
        let vec = chunk.vector(0).unwrap();
        assert!(!vec.is_null(0));
        let got: Option<i32> = vec.get(0).unwrap();
        assert_eq!(got, Some(7));
    }

    #[test]
    fn as_slice_reads_every_fixed_width_type() {
        macro_rules! check {
            ($ty:ty, $val:expr) => {{
                let types = [LogicalType::of::<$ty>().unwrap()];
                let mut chunk = DataChunkHandle::new(&types).unwrap();
                {
                    let mut vec = chunk.vector_mut(0).unwrap();
                    vec.set(0, $val).unwrap();
                    vec.set(1, $val).unwrap();
                }
                let vec = chunk.vector(0).unwrap();
                let slice: &[$ty] = vec.as_slice(2).unwrap();
                assert_eq!(slice, &[$val, $val]);
            }};
        }
        check!(bool, true);
        check!(i8, -12i8);
        check!(i16, -1234i16);
        check!(i32, -123_456i32);
        check!(i64, -123_456_789i64);
        check!(u8, 200u8);
        check!(u16, 40_000u16);
        check!(u32, 3_000_000_000u32);
        check!(u64, 10_000_000_000u64);
        check!(f32, 1.5f32);
        check!(f64, 2.5f64);
    }

    #[test]
    fn as_slice_on_type_mismatch_errors() {
        let types = [LogicalType::of::<i32>().unwrap()];
        let chunk = DataChunkHandle::new(&types).unwrap();
        let vec = chunk.vector(0).unwrap();
        assert!(vec.as_slice::<i64>(1).is_err());
    }

    #[test]
    fn validity_is_none_when_no_nulls_present() {
        let types = [LogicalType::of::<i32>().unwrap()];
        let mut chunk = DataChunkHandle::new(&types).unwrap();
        {
            let mut vec = chunk.vector_mut(0).unwrap();
            vec.set(0, 1i32).unwrap();
        }
        let vec = chunk.vector(0).unwrap();
        assert!(vec.validity(1).is_none());
    }

    #[test]
    fn validity_reflects_null_rows_once_any_row_is_marked_null() {
        let types = [LogicalType::of::<i32>().unwrap()];
        let mut chunk = DataChunkHandle::new(&types).unwrap();
        {
            let mut vec = chunk.vector_mut(0).unwrap();
            vec.set(0, 1i32).unwrap();
            vec.set_null(1);
            vec.set(2, 3i32).unwrap();
        }
        let vec = chunk.vector(0).unwrap();
        let mask = vec.validity(3).expect("a NULL row was set, so a mask must exist");
        let is_valid = |row: usize| (mask[row / 64] >> (row % 64)) & 1 == 1;
        assert!(is_valid(0));
        assert!(!is_valid(1));
        assert!(is_valid(2));
    }

    #[test]
    fn set_row_valid_un_marks_a_previously_null_row() {
        let types = [LogicalType::of::<i32>().unwrap()];
        let mut chunk = DataChunkHandle::new(&types).unwrap();
        {
            let mut vec = chunk.vector_mut(0).unwrap();
            vec.set_null(0);
        }
        assert!(chunk.vector(0).unwrap().is_null(0));
        {
            let mut vec = chunk.vector_mut(0).unwrap();
            vec.set_row_valid(0);
        }
        assert!(!chunk.vector(0).unwrap().is_null(0));
    }

    #[test]
    fn set_row_validity_toggles_both_directions() {
        let types = [LogicalType::of::<i32>().unwrap()];
        let mut chunk = DataChunkHandle::new(&types).unwrap();
        {
            let mut vec = chunk.vector_mut(0).unwrap();
            vec.set_row_validity(0, false);
        }
        assert!(chunk.vector(0).unwrap().is_null(0));
        {
            let mut vec = chunk.vector_mut(0).unwrap();
            vec.set_row_validity(0, true);
        }
        assert!(!chunk.vector(0).unwrap().is_null(0));
    }
}
