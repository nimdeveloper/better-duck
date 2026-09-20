//! MAP read/write helpers + [`AppendAble`] impl for `HashMap<DuckValue, DuckValue>`.
//!
//! DuckDB `MAP` is physically stored as `LIST<STRUCT(key, value)>`.  The read path
//! walks the flat STRUCT child vectors using the same validity + recursion pattern as
//! LIST/ARRAY.  The write path builds a `duckdb_value` via `duckdb_create_map_value`.
//!
//! Unlike the previous string-keyed representation, MAP keys are now real `DuckValue`
//! values, preserving the full DuckDB key type.
// FFI pointer arguments are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use crate::types::value_ref::DuckValueRef;
use crate::{
    error::{DuckDBConversionError, Error, Result},
    ffi::{
        duckdb_create_map_type, duckdb_create_map_value, duckdb_destroy_logical_type,
        duckdb_destroy_value, duckdb_get_type_id, duckdb_list_entry, duckdb_list_vector_get_child,
        duckdb_logical_type, duckdb_struct_vector_get_child, duckdb_value, duckdb_vector,
        duckdb_vector_get_column_type, duckdb_vector_get_data, idx_t,
    },
    types::appendable::AppendAble,
};

use super::{value::DuckValue, DuckLogicalType};

// Read path

/// Read a DuckDB `MAP` vector column at `row_idx`.
///
/// MAP is physically `LIST<STRUCT(key, value)>`.  Keys are returned as real
/// [`DuckValue`] instances — no stringification.
///
/// # Safety
/// `val` must be a valid `duckdb_vector` of MAP type; `row_idx` must be within
/// `[0, chunk_size)`.
pub(crate) fn read_map(
    val: duckdb_vector,
    row_idx: u64,
) -> Result<DuckValue, DuckDBConversionError> {
    // SAFETY: MAP data layout is identical to LIST: each row slot holds a
    // `duckdb_list_entry { offset, length }`.
    let data_ptr = unsafe { duckdb_vector_get_data(val) as *const duckdb_list_entry };
    // SAFETY: `row_idx` is within [0, chunk_size).
    let entry: duckdb_list_entry = unsafe { *data_ptr.add(row_idx as usize) };

    // `entries_vec` is the flat STRUCT(key, value) child vector.
    // SAFETY: `val` is a valid MAP/LIST vector.
    let entries_vec = unsafe { duckdb_list_vector_get_child(val) };
    // SAFETY: `entries_vec` is a valid STRUCT vector; child 0 = keys.
    let key_vec = unsafe { duckdb_struct_vector_get_child(entries_vec, 0) };
    // SAFETY: `entries_vec` is a valid STRUCT vector; child 1 = values.
    let val_vec = unsafe { duckdb_struct_vector_get_child(entries_vec, 1) };

    // SAFETY: `key_vec` is a valid vector.
    let mut key_lt = unsafe { duckdb_vector_get_column_type(key_vec) };
    // SAFETY: `key_lt` was returned by `duckdb_vector_get_column_type`.
    let key_tid = unsafe { duckdb_get_type_id(key_lt) };
    // SAFETY: `key_lt` was allocated by `duckdb_vector_get_column_type` above.
    unsafe { duckdb_destroy_logical_type(&mut key_lt) };

    // SAFETY: `val_vec` is a valid vector.
    let mut vlt = unsafe { duckdb_vector_get_column_type(val_vec) };
    // SAFETY: `vlt` was returned by `duckdb_vector_get_column_type`.
    let val_tid = unsafe { duckdb_get_type_id(vlt) };
    // SAFETY: `vlt` was allocated by `duckdb_vector_get_column_type` above.
    unsafe { duckdb_destroy_logical_type(&mut vlt) };

    let mut pairs: HashMap<DuckValue, DuckValue> = HashMap::with_capacity(entry.length as usize);
    let mut read_err: Option<DuckDBConversionError> = None;

    for j in entry.offset..entry.offset + entry.length {
        let k = match DuckValue::from_duckdb_vec(key_vec, key_tid, j) {
            Ok(v) => v,
            Err(e) => {
                read_err = Some(e);
                break;
            },
        };
        let v = match DuckValue::from_duckdb_vec(val_vec, val_tid, j) {
            Ok(v) => v,
            Err(e) => {
                read_err = Some(e);
                break;
            },
        };
        pairs.insert(k, v);
    }

    match read_err {
        Some(e) => Err(e),
        None => Ok(DuckValue::Map(pairs)),
    }
}

// Write path

/// Build a `duckdb_value` of type `MAP` from a `HashMap<DuckValue, DuckValue>`.
///
/// Returns an error for empty maps (value type cannot be inferred).
/// The caller is responsible for destroying the returned value.
pub(crate) fn map_to_duck(
    m: &HashMap<DuckValue, DuckValue>
) -> Result<duckdb_value, DuckDBConversionError> {
    let n = m.len();
    if n == 0 {
        return Err(DuckDBConversionError::ConversionError(
            "cannot convert empty Map to duckdb_value: value type unknown".into(),
        ));
    }
    let pairs: Vec<(&DuckValue, &DuckValue)> = m.iter().collect();
    let mut key_lt = DuckValue::logical_type_of(pairs[0].0)?; // mut needed for duckdb_destroy_logical_type
    let mut val_lt = match DuckValue::logical_type_of(pairs[0].1) {
        Ok(lt) => lt,
        Err(e) => {
            // SAFETY: `key_lt` was allocated above.
            unsafe { duckdb_destroy_logical_type(&mut key_lt) };
            return Err(e);
        },
    };
    // SAFETY: both types are valid; `duckdb_create_map_type` copies them.
    let mut map_lt = unsafe { duckdb_create_map_type(key_lt, val_lt) };
    // SAFETY: `key_lt` was allocated by `logical_type_of` above; destroy once.
    unsafe { duckdb_destroy_logical_type(&mut key_lt) };
    // SAFETY: `val_lt` was allocated by `logical_type_of` above; destroy once.
    unsafe { duckdb_destroy_logical_type(&mut val_lt) };

    let mut key_dvs: Vec<duckdb_value> = Vec::with_capacity(n);
    let mut val_dvs: Vec<duckdb_value> = Vec::with_capacity(n);
    let mut err: Option<DuckDBConversionError> = None;
    for (k, v) in &pairs {
        match k.to_duck() {
            Ok(kv) => key_dvs.push(kv),
            Err(e) => {
                err = Some(e);
                break;
            },
        }
        match v.to_duck() {
            Ok(vv) => val_dvs.push(vv),
            Err(e) => {
                err = Some(e);
                break;
            },
        }
    }
    if let Some(e) = err {
        for mut kv in key_dvs {
            // SAFETY: each `kv` was created by `to_duck()` above; destroy once.
            unsafe { duckdb_destroy_value(&mut kv) };
        }
        for mut vv in val_dvs {
            // SAFETY: each `vv` was created by `to_duck()` above; destroy once.
            unsafe { duckdb_destroy_value(&mut vv) };
        }
        // SAFETY: `map_lt` was allocated above; destroy once.
        unsafe { duckdb_destroy_logical_type(&mut map_lt) };
        return Err(e);
    }
    // SAFETY: `map_lt` valid; key/val arrays have `n` elements each.
    let result = unsafe {
        duckdb_create_map_value(map_lt, key_dvs.as_mut_ptr(), val_dvs.as_mut_ptr(), n as idx_t)
    };
    // SAFETY: `map_lt` was allocated above; destroy once.
    unsafe { duckdb_destroy_logical_type(&mut map_lt) };
    for mut kv in key_dvs {
        // SAFETY: each `kv` was created by `to_duck()` above; destroy once.
        unsafe { duckdb_destroy_value(&mut kv) };
    }
    for mut vv in val_dvs {
        // SAFETY: each `vv` was created by `to_duck()` above; destroy once.
        unsafe { duckdb_destroy_value(&mut vv) };
    }
    Ok(result)
}

// Logical-type path

/// Return a `duckdb_logical_type` for a MAP with key/value types inferred from the
/// first entry.
pub(crate) fn map_logical_type(
    m: &HashMap<DuckValue, DuckValue>
) -> Result<duckdb_logical_type, DuckDBConversionError> {
    if m.is_empty() {
        return Err(DuckDBConversionError::ConversionError(
            "cannot determine value type of empty Map".into(),
        ));
    }
    let (first_k, first_v) = m.iter().next().unwrap();
    let mut key_lt = DuckValue::logical_type_of(first_k)?;
    let mut val_lt = match DuckValue::logical_type_of(first_v) {
        Ok(lt) => lt,
        Err(e) => {
            // SAFETY: `key_lt` was allocated by `logical_type_of` above; destroy once.
            unsafe { duckdb_destroy_logical_type(&mut key_lt) };
            return Err(e);
        },
    };
    // SAFETY: both types are valid; `duckdb_create_map_type` copies them.
    let lt = unsafe { duckdb_create_map_type(key_lt, val_lt) };
    // SAFETY: `key_lt` was allocated by `logical_type_of` above; destroy once.
    unsafe { duckdb_destroy_logical_type(&mut key_lt) };
    // SAFETY: `val_lt` was allocated by `logical_type_of` above; destroy once.
    unsafe { duckdb_destroy_logical_type(&mut val_lt) };
    Ok(lt)
}

// AppendAble impl
//
// Unlike `map_to_duck` above (which infers key/value types from the first entry and
// therefore rejects empty maps), this uses `K`/`V::duck_logical_type()` — a property
// of the Rust types, not of any particular value — so it works for empty maps too.
//
// This generic impl covers `HashMap<DuckValue, DuckValue>` too (`DuckValue` does not
// implement `DuckLogicalType`, but a caller with an actual `HashMap<DuckValue,
// DuckValue>` can bind `DuckValue::Map(m)` directly, which goes through the
// value-based `map_to_duck` path above instead).
//
// A plain `HashMap<String, DuckValue>` therefore means MAP here; wrap it in
// `DuckStruct` (see `duck_struct.rs`) to mean STRUCT instead — mirroring how `Blob`
// disambiguates `Vec<u8>` from a generic `LIST`.

/// Builds a `duckdb_value` of type `MAP` from `m`, using `K`/`V`'s static logical
/// types for the key/value types. Works even when `m` is empty.
fn build_typed_map_value<K, V>(m: &HashMap<K, V>) -> Result<duckdb_value, DuckDBConversionError>
where
    K: Into<DuckValue> + Clone + DuckLogicalType,
    V: Into<DuckValue> + Clone + DuckLogicalType,
{
    let mut key_lt = K::duck_logical_type()?;
    let mut val_lt = match V::duck_logical_type() {
        Ok(lt) => lt,
        Err(e) => {
            // SAFETY: `key_lt` was allocated by `K::duck_logical_type()` above.
            unsafe { duckdb_destroy_logical_type(&mut key_lt) };
            return Err(e);
        },
    };
    // SAFETY: both types are valid; `duckdb_create_map_type` copies them.
    let mut map_lt = unsafe { duckdb_create_map_type(key_lt, val_lt) };
    // SAFETY: `key_lt` was allocated above; destroy once.
    unsafe { duckdb_destroy_logical_type(&mut key_lt) };
    // SAFETY: `val_lt` was allocated above; destroy once.
    unsafe { duckdb_destroy_logical_type(&mut val_lt) };

    let n = m.len();
    let mut key_dvs: Vec<duckdb_value> = Vec::with_capacity(n);
    let mut val_dvs: Vec<duckdb_value> = Vec::with_capacity(n);
    let mut err: Option<DuckDBConversionError> = None;
    for (k, v) in m {
        match k.clone().into().to_duck() {
            Ok(kv) => key_dvs.push(kv),
            Err(e) => {
                err = Some(e);
                break;
            },
        }
        match v.clone().into().to_duck() {
            Ok(vv) => val_dvs.push(vv),
            Err(e) => {
                err = Some(e);
                break;
            },
        }
    }
    if let Some(e) = err {
        for mut kv in key_dvs {
            // SAFETY: each `kv` was created by `to_duck()` above; destroy once.
            unsafe { duckdb_destroy_value(&mut kv) };
        }
        for mut vv in val_dvs {
            // SAFETY: each `vv` was created by `to_duck()` above; destroy once.
            unsafe { duckdb_destroy_value(&mut vv) };
        }
        // SAFETY: `map_lt` was allocated above; destroy once.
        unsafe { duckdb_destroy_logical_type(&mut map_lt) };
        return Err(e);
    }
    // SAFETY: `map_lt` valid; key/val arrays have `n` elements each.
    let result = unsafe {
        duckdb_create_map_value(map_lt, key_dvs.as_mut_ptr(), val_dvs.as_mut_ptr(), n as idx_t)
    };
    // SAFETY: `map_lt` was allocated above; destroy once.
    unsafe { duckdb_destroy_logical_type(&mut map_lt) };
    for mut kv in key_dvs {
        // SAFETY: each `kv` was created by `to_duck()` above; destroy once.
        unsafe { duckdb_destroy_value(&mut kv) };
    }
    for mut vv in val_dvs {
        // SAFETY: each `vv` was created by `to_duck()` above; destroy once.
        unsafe { duckdb_destroy_value(&mut vv) };
    }
    Ok(result)
}

/// Bind/append a `HashMap<K, V>` as a DuckDB `MAP`, for any `K`/`V` with a fixed
/// DuckDB type (see [`DuckLogicalType`]). Works for empty maps too.
impl<K, V> AppendAble for HashMap<K, V>
where
    K: Into<DuckValue> + Clone + DuckLogicalType,
    V: Into<DuckValue> + Clone + DuckLogicalType,
{
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> Result<()> {
        let dv = build_typed_map_value(self).map_err(Error::ConversionError)?;
        // SAFETY: `stmt`/`idx` are valid; `dv` is an owned value bound and destroyed here.
        unsafe { crate::types::appendable::bind_owned_value(stmt, idx, dv) }
    }

    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> Result<()> {
        let dv = build_typed_map_value(self).map_err(Error::ConversionError)?;
        // SAFETY: `appender` is valid; `dv` is an owned value appended and destroyed here.
        unsafe { crate::types::appendable::append_owned_value(appender, dv) }
    }
}

// From<T> for DuckValue — Map

impl From<HashMap<DuckValue, DuckValue>> for DuckValue {
    fn from(h: HashMap<DuckValue, DuckValue>) -> Self {
        DuckValue::Map(h)
    }
}

/// Converts a `HashMap<String, DuckValue>` into `DuckValue::Map` (keys become
/// `DuckValue::Text`).
impl From<HashMap<String, DuckValue>> for DuckValue {
    fn from(h: HashMap<String, DuckValue>) -> Self {
        DuckValue::Map(h.into_iter().map(|(k, v)| (DuckValue::Text(k), v)).collect())
    }
}

/// Converts a `Vec<(String, DuckValue)>` into `DuckValue::Map` (keys become
/// `DuckValue::Text`).
impl From<Vec<(String, DuckValue)>> for DuckValue {
    fn from(v: Vec<(String, DuckValue)>) -> Self {
        DuckValue::Map(v.into_iter().map(|(k, v)| (DuckValue::Text(k), v)).collect())
    }
}

/// Order-independent hash for map/struct entries: XOR of per-entry hashes.
///
/// We hash each `(k, v)` pair with a fresh `DefaultHasher`, then XOR-fold the
/// results so that insertion order does not affect the combined hash.
pub(super) fn map_entries_hash<H: Hasher, K: Hash, V: Hash>(
    entries: impl Iterator<Item = (K, V)>,
    len: usize,
    state: &mut H,
) {
    len.hash(state);
    let xor_fold: u64 = entries
        .map(|(k, v)| {
            let mut h = DefaultHasher::new();
            k.hash(&mut h);
            v.hash(&mut h);
            h.finish()
        })
        .fold(0u64, |acc, x| acc ^ x);
    xor_fold.hash(state);
}

/// Order-independent hash for map/struct entries.
pub(super) fn map_entries_hash_ref<'a, H: Hasher>(
    entries: impl Iterator<Item = (&'a DuckValueRef<'a>, &'a DuckValueRef<'a>)>,
    len: usize,
    state: &mut H,
) where
    DuckValueRef<'a>: Hash,
{
    len.hash(state);
    let xor_fold: u64 = entries
        .map(|(k, v)| {
            let mut h = DefaultHasher::new();
            k.hash(&mut h);
            v.hash(&mut h);
            h.finish()
        })
        .fold(0u64, |acc, x| acc ^ x);
    xor_fold.hash(state);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        connection::Connection,
        ffi::{duckdb_destroy_logical_type, duckdb_destroy_value, duckdb_get_type_id},
    };

    #[test]
    fn empty_hashmap_appends_and_reads_back() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (v MAP(INTEGER, VARCHAR))").unwrap();
        let mut map: HashMap<i32, String> = HashMap::new();
        conn.execute_with("INSERT INTO t VALUES ($1)", &mut [&mut map]).unwrap();
        let mut result = conn.execute("SELECT v FROM t").unwrap();
        let row = result.next().unwrap().unwrap();
        match row.get("v").unwrap() {
            DuckValue::Map(m) => assert!(m.is_empty()),
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn nonempty_hashmap_appends_and_reads_back() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (v MAP(INTEGER, VARCHAR))").unwrap();
        let mut map: HashMap<i32, String> = HashMap::from([(1, "one".to_string())]);
        conn.execute_with("INSERT INTO t VALUES ($1)", &mut [&mut map]).unwrap();
        let mut result = conn.execute("SELECT v FROM t").unwrap();
        let row = result.next().unwrap().unwrap();
        match row.get("v").unwrap() {
            DuckValue::Map(m) => {
                assert_eq!(m.get(&DuckValue::Int(1)), Some(&DuckValue::text("one")));
            },
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn duck_value_map_creates_value_and_logical_type() {
        let value = DuckValue::Map(HashMap::from([
            (DuckValue::Int(1), DuckValue::text("one")),
            (DuckValue::Int(2), DuckValue::text("two")),
        ]));
        let mut raw = value.to_duck().unwrap();
        // SAFETY: `raw` was created by `DuckValue::to_duck` and is destroyed once.
        unsafe { duckdb_destroy_value(&mut raw) };

        let mut logical_type = DuckValue::logical_type_of(&value).unwrap();
        assert_eq!(
            // SAFETY: `logical_type` is valid until it is destroyed below.
            unsafe { duckdb_get_type_id(logical_type) },
            crate::ffi::DUCKDB_TYPE_DUCKDB_TYPE_MAP
        );
        // SAFETY: `logical_type` was created by `logical_type_of` and is destroyed once.
        unsafe { duckdb_destroy_logical_type(&mut logical_type) };
    }

    #[test]
    fn empty_duck_value_map_reports_conversion_errors() {
        let value = DuckValue::Map(HashMap::new());
        assert!(matches!(value.to_duck(), Err(DuckDBConversionError::ConversionError(_))));
        assert!(matches!(
            DuckValue::logical_type_of(&value),
            Err(DuckDBConversionError::ConversionError(_))
        ));
    }

    #[test]
    fn map_reads_arbitrary_keys_null_values_and_row_offsets() {
        let mut conn = Connection::open_in_memory().unwrap();
        let mut result = conn
            .execute(
                "SELECT * FROM (VALUES \
                 (map([1, 2], ['one', NULL])), \
                 (map([3], ['three']))) AS t(v)",
            )
            .unwrap();

        let first = result.next().unwrap().unwrap();
        assert_eq!(
            first.get("v"),
            Some(&DuckValue::Map(HashMap::from([
                (DuckValue::Int(1), DuckValue::text("one")),
                (DuckValue::Int(2), DuckValue::Null),
            ])))
        );
        let second = result.next().unwrap().unwrap();
        assert_eq!(
            second.get("v"),
            Some(&DuckValue::Map(HashMap::from([(DuckValue::Int(3), DuckValue::text("three"),)])))
        );
    }

    #[test]
    fn map_conversions_preserve_keys_and_values() {
        let direct = HashMap::from([(DuckValue::Int(1), DuckValue::Boolean(true))]);
        assert_eq!(DuckValue::from(direct.clone()), DuckValue::Map(direct));

        let string_map = HashMap::from([("name".to_owned(), DuckValue::Int(7))]);
        let expected =
            DuckValue::Map(HashMap::from([(DuckValue::text("name"), DuckValue::Int(7))]));
        assert_eq!(DuckValue::from(string_map), expected);
        assert_eq!(DuckValue::from(vec![("name".to_owned(), DuckValue::Int(7))]), expected);
    }
}
