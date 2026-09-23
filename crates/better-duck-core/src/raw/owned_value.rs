//! RAII wrapper for a standalone, owned `duckdb_value`, with introspection.
//!
//! Most values in this driver are read straight out of result vectors into
//! [`DuckValue`], but DuckDB also has a *standalone value* object
//! (`duckdb_value`) — the currency of the scalar-value C API. [`OwnedValue`] owns
//! one (destroying it exactly once on drop) and exposes the introspection the C API
//! offers on any value: whether it is SQL `NULL`, its SQL string rendering, and —
//! for a `STRUCT` value — its child fields by index (each returned child is itself
//! an owned `OwnedValue`, so nothing is leaked or double-freed).
//!
//! Obtain one with [`DuckValue::to_owned_value`](crate::types::value::DuckValue::to_owned_value).
// FFI pointer args are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::ffi::{c_void, CStr};

use crate::{
    error::{DuckDBConversionError, Result},
    ffi::{
        duckdb_destroy_value, duckdb_free, duckdb_get_date, duckdb_get_hugeint,
        duckdb_get_interval, duckdb_get_struct_child, duckdb_get_time, duckdb_get_time_ns,
        duckdb_get_time_tz, duckdb_get_timestamp, duckdb_get_timestamp_ms, duckdb_get_timestamp_ns,
        duckdb_get_timestamp_s, duckdb_get_timestamp_tz, duckdb_get_type_id, duckdb_get_uhugeint,
        duckdb_get_uuid, duckdb_get_value_type, duckdb_is_null_value, duckdb_type, duckdb_value,
        duckdb_value_to_string, idx_t, DUCKDB_TYPE_DUCKDB_TYPE_DATE,
        DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT, DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL,
        DUCKDB_TYPE_DUCKDB_TYPE_TIME, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS,
        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ,
        DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS, DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ,
        DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT, DUCKDB_TYPE_DUCKDB_TYPE_UUID,
    },
    types::numeric::{i128_from_hugeint, u128_from_uhugeint},
    types::uuid::DuckUuid,
};

/// A broken-down DuckDB `INTERVAL` (months, days, microseconds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntervalParts {
    /// Whole months.
    pub months: i32,
    /// Whole days.
    pub days: i32,
    /// Sub-day microseconds.
    pub micros: i64,
}

/// An owned standalone `duckdb_value` (destroyed once on drop) with introspection.
pub struct OwnedValue {
    value: duckdb_value,
}

impl OwnedValue {
    /// Takes ownership of a raw `duckdb_value`, or `None` if null.
    ///
    /// # Safety
    ///
    /// `value` must be a live `duckdb_value` whose ownership is transferred here
    /// (destroyed on drop).
    pub(crate) unsafe fn from_raw(value: duckdb_value) -> Option<OwnedValue> {
        if value.is_null() {
            return None;
        }
        Some(OwnedValue { value })
    }

    /// Whether the value's type is SQL `NULL`.
    #[must_use]
    pub fn is_null(&self) -> bool {
        // SAFETY: `self.value` is a valid, non-null duckdb_value owned by `self`.
        unsafe { duckdb_is_null_value(self.value) }
    }

    /// The raw handle, borrowed for the duration of `&self` (e.g. to make a vector
    /// reference this value). The caller must not destroy it.
    #[allow(dead_code)]
    pub(crate) fn raw(&self) -> duckdb_value {
        self.value
    }

    /// The value's SQL string rendering (e.g. `42`, `'text'`), or `None` if DuckDB
    /// produces none.
    #[must_use]
    pub fn to_sql_string(&self) -> Option<String> {
        // SAFETY: `self.value` is valid; `duckdb_value_to_string` returns a heap
        // `char*` (or null) that must be freed with `duckdb_free`.
        let ptr = unsafe { duckdb_value_to_string(self.value) };
        if ptr.is_null() {
            return None;
        }
        // SAFETY: `ptr` is a valid, non-null, null-terminated C string.
        let s = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned();
        // SAFETY: `ptr` was allocated by DuckDB and ownership transferred to us.
        unsafe { duckdb_free(ptr as *mut c_void) };
        Some(s)
    }

    /// The child field at `index` of a `STRUCT` value, as an owned [`OwnedValue`],
    /// or `None` if out of range (or the value is not a struct).
    #[must_use]
    pub fn struct_child(
        &self,
        index: u64,
    ) -> Option<OwnedValue> {
        // SAFETY: `self.value` is valid; `duckdb_get_struct_child` returns a newly
        // allocated child value (destroyed by the returned `OwnedValue`) or null.
        unsafe { OwnedValue::from_raw(duckdb_get_struct_child(self.value, index as idx_t)) }
    }

    /// The value's top-level `duckdb_type` id.
    #[must_use]
    fn type_id(&self) -> duckdb_type {
        // SAFETY: `self.value` is valid; `duckdb_get_value_type` returns a *borrowed*
        // logical type owned by the value — read only, never destroyed.
        let lt = unsafe { duckdb_get_value_type(self.value) };
        // SAFETY: `lt` is a valid (borrowed) logical type.
        unsafe { duckdb_get_type_id(lt) }
    }

    /// Errors unless the value's type id is `expected`.
    fn expect_type(
        &self,
        expected: duckdb_type,
        what: &str,
    ) -> Result<(), DuckDBConversionError> {
        let actual = self.type_id();
        if actual == expected {
            Ok(())
        } else {
            Err(DuckDBConversionError::ConversionError(format!(
                "value is type id {actual}, not {what} (id {expected})"
            )))
        }
    }

    /// The `HUGEINT` value as an `i128`.
    ///
    /// # Errors
    /// Errors if the value is not a `HUGEINT`.
    pub fn get_hugeint(&self) -> Result<i128, DuckDBConversionError> {
        self.expect_type(DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT, "HUGEINT")?;
        // SAFETY: type checked as HUGEINT above; `self.value` is valid.
        Ok(i128_from_hugeint(unsafe { duckdb_get_hugeint(self.value) }))
    }

    /// The `UHUGEINT` value as a `u128`.
    ///
    /// # Errors
    /// Errors if the value is not a `UHUGEINT`.
    pub fn get_uhugeint(&self) -> Result<u128, DuckDBConversionError> {
        self.expect_type(DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT, "UHUGEINT")?;
        // SAFETY: type checked as UHUGEINT above; `self.value` is valid.
        Ok(u128_from_uhugeint(unsafe { duckdb_get_uhugeint(self.value) }))
    }

    /// The `DATE` value as a day count since the epoch (1970-01-01).
    ///
    /// # Errors
    /// Errors if the value is not a `DATE`.
    pub fn get_date_days(&self) -> Result<i32, DuckDBConversionError> {
        self.expect_type(DUCKDB_TYPE_DUCKDB_TYPE_DATE, "DATE")?;
        // SAFETY: type checked as DATE above; `self.value` is valid.
        Ok(unsafe { duckdb_get_date(self.value) }.days)
    }

    /// The `TIME` value as microseconds since midnight.
    ///
    /// # Errors
    /// Errors if the value is not a `TIME`.
    pub fn get_time_micros(&self) -> Result<i64, DuckDBConversionError> {
        self.expect_type(DUCKDB_TYPE_DUCKDB_TYPE_TIME, "TIME")?;
        // SAFETY: type checked as TIME above; `self.value` is valid.
        Ok(unsafe { duckdb_get_time(self.value) }.micros)
    }

    /// The `TIME_NS` value as nanoseconds since midnight.
    ///
    /// # Errors
    /// Errors if the value is not a `TIME_NS`.
    pub fn get_time_ns_nanos(&self) -> Result<i64, DuckDBConversionError> {
        self.expect_type(DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS, "TIME_NS")?;
        // SAFETY: type checked as TIME_NS above; `self.value` is valid.
        Ok(unsafe { duckdb_get_time_ns(self.value) }.nanos)
    }

    /// The `TIME WITH TIME ZONE` value as its packed 64-bit representation
    /// (micros + offset bits, per DuckDB's `duckdb_time_tz` layout).
    ///
    /// # Errors
    /// Errors if the value is not a `TIME_TZ`.
    pub fn get_time_tz_bits(&self) -> Result<u64, DuckDBConversionError> {
        self.expect_type(DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ, "TIME_TZ")?;
        // SAFETY: type checked as TIME_TZ above; `self.value` is valid.
        Ok(unsafe { duckdb_get_time_tz(self.value) }.bits)
    }

    /// The `TIMESTAMP` value as microseconds since the epoch.
    ///
    /// # Errors
    /// Errors if the value is not a `TIMESTAMP`.
    pub fn get_timestamp_micros(&self) -> Result<i64, DuckDBConversionError> {
        self.expect_type(DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP, "TIMESTAMP")?;
        // SAFETY: type checked as TIMESTAMP above; `self.value` is valid.
        Ok(unsafe { duckdb_get_timestamp(self.value) }.micros)
    }

    /// The `TIMESTAMP WITH TIME ZONE` value as microseconds since the epoch (UTC).
    ///
    /// # Errors
    /// Errors if the value is not a `TIMESTAMP_TZ`.
    pub fn get_timestamp_tz_micros(&self) -> Result<i64, DuckDBConversionError> {
        self.expect_type(DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ, "TIMESTAMP_TZ")?;
        // SAFETY: type checked as TIMESTAMP_TZ above; `self.value` is valid.
        Ok(unsafe { duckdb_get_timestamp_tz(self.value) }.micros)
    }

    /// The `TIMESTAMP_S` value as seconds since the epoch.
    ///
    /// # Errors
    /// Errors if the value is not a `TIMESTAMP_S`.
    pub fn get_timestamp_seconds(&self) -> Result<i64, DuckDBConversionError> {
        self.expect_type(DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S, "TIMESTAMP_S")?;
        // SAFETY: type checked as TIMESTAMP_S above; `self.value` is valid.
        Ok(unsafe { duckdb_get_timestamp_s(self.value) }.seconds)
    }

    /// The `TIMESTAMP_MS` value as milliseconds since the epoch.
    ///
    /// # Errors
    /// Errors if the value is not a `TIMESTAMP_MS`.
    pub fn get_timestamp_millis(&self) -> Result<i64, DuckDBConversionError> {
        self.expect_type(DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS, "TIMESTAMP_MS")?;
        // SAFETY: type checked as TIMESTAMP_MS above; `self.value` is valid.
        Ok(unsafe { duckdb_get_timestamp_ms(self.value) }.millis)
    }

    /// The `TIMESTAMP_NS` value as nanoseconds since the epoch.
    ///
    /// # Errors
    /// Errors if the value is not a `TIMESTAMP_NS`.
    pub fn get_timestamp_nanos(&self) -> Result<i64, DuckDBConversionError> {
        self.expect_type(DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS, "TIMESTAMP_NS")?;
        // SAFETY: type checked as TIMESTAMP_NS above; `self.value` is valid.
        Ok(unsafe { duckdb_get_timestamp_ns(self.value) }.nanos)
    }

    /// The `INTERVAL` value as its (months, days, micros) parts.
    ///
    /// # Errors
    /// Errors if the value is not an `INTERVAL`.
    pub fn get_interval(&self) -> Result<IntervalParts, DuckDBConversionError> {
        self.expect_type(DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL, "INTERVAL")?;
        // SAFETY: type checked as INTERVAL above; `self.value` is valid.
        let iv = unsafe { duckdb_get_interval(self.value) };
        Ok(IntervalParts { months: iv.months, days: iv.days, micros: iv.micros })
    }

    /// The `UUID` value.
    ///
    /// # Errors
    /// Errors if the value is not a `UUID`.
    pub fn get_uuid(&self) -> Result<DuckUuid, DuckDBConversionError> {
        self.expect_type(DUCKDB_TYPE_DUCKDB_TYPE_UUID, "UUID")?;
        // SAFETY: type checked as UUID above; `self.value` is valid. The value API
        // returns the logical uhugeint (sign-bit flip already undone), so build the
        // `DuckUuid` directly from its two halves.
        let raw = unsafe { duckdb_get_uuid(self.value) };
        Ok(DuckUuid(((raw.upper as u128) << 64) | (raw.lower as u128)))
    }
}

impl Drop for OwnedValue {
    fn drop(&mut self) {
        if !self.value.is_null() {
            // SAFETY: `self.value` is a valid, non-null duckdb_value owned by `self`
            // and not yet destroyed; destroyed exactly once here.
            unsafe { duckdb_destroy_value(&mut self.value) };
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::value::DuckValue;
    use std::collections::HashMap;

    #[test]
    fn null_and_scalar_values_report_correctly() {
        assert!(DuckValue::Null.to_owned_value().unwrap().is_null());
        let int = DuckValue::Int(42).to_owned_value().unwrap();
        assert!(!int.is_null());
        assert_eq!(int.to_sql_string().as_deref(), Some("42"));
    }

    #[test]
    fn text_value_renders_as_sql_string() {
        let v = DuckValue::text("hi").to_owned_value().unwrap();
        // DuckDB renders a VARCHAR value quoted.
        assert_eq!(v.to_sql_string().as_deref(), Some("'hi'"));
    }

    #[test]
    fn struct_children_are_owned_and_introspectable() {
        // A single-field struct so child index 0 is deterministic.
        let s = DuckValue::Struct(HashMap::from([("n".to_owned(), DuckValue::Int(7))]));
        let owned = s.to_owned_value().unwrap();
        let child = owned.struct_child(0).expect("struct child 0");
        assert!(!child.is_null());
        assert_eq!(child.to_sql_string().as_deref(), Some("7"));
        // Out-of-range child index yields None.
        assert!(owned.struct_child(5).is_none());
    }

    #[test]
    fn typed_scalar_getters_read_matching_values_and_reject_mismatches() {
        use crate::types::uuid::DuckUuid;

        // HUGEINT / UHUGEINT.
        assert_eq!(DuckValue::HugeInt(-170).to_owned_value().unwrap().get_hugeint().unwrap(), -170);
        assert_eq!(DuckValue::UHugeInt(340).to_owned_value().unwrap().get_uhugeint().unwrap(), 340);

        // UUID round-trips through the value API.
        let uuid = DuckUuid(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);
        assert_eq!(DuckValue::Uuid(uuid).to_owned_value().unwrap().get_uuid().unwrap(), uuid);

        // Exact type check: a HUGEINT getter on a UHUGEINT value errors.
        assert!(DuckValue::UHugeInt(1).to_owned_value().unwrap().get_hugeint().is_err());
        // ...and on an integer value too.
        assert!(DuckValue::Int(1).to_owned_value().unwrap().get_timestamp_micros().is_err());
    }

    #[cfg(feature = "chrono")]
    #[test]
    fn interval_getter_reads_parts() {
        // 2 pure days → (0 months, 0 days-field, 2*86400*1e6 micros) — DuckDB keeps a
        // day-precision interval in the micros field when built from a µs Duration.
        let iv = DuckValue::Interval(chrono::Duration::microseconds(2 * 86_400 * 1_000_000))
            .to_owned_value()
            .unwrap()
            .get_interval()
            .unwrap();
        assert_eq!((iv.months, iv.days), (0, 0));
        assert_eq!(iv.micros, 2 * 86_400 * 1_000_000);
    }

    #[cfg(feature = "chrono")]
    #[test]
    fn temporal_getters_read_each_resolution() {
        use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

        // DATE → day count (2024-03-15).
        let date = DuckValue::Date(NaiveDate::from_ymd_opt(2024, 3, 15).unwrap());
        let days = date.to_owned_value().unwrap().get_date_days().unwrap();
        assert_eq!(
            days,
            (NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()
                - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32
        );

        // TIME → micros since midnight (01:02:03).
        let time = DuckValue::Time(NaiveTime::from_hms_opt(1, 2, 3).unwrap());
        assert_eq!(
            time.to_owned_value().unwrap().get_time_micros().unwrap(),
            (3600 + 2 * 60 + 3) * 1_000_000
        );

        // TIMESTAMP → micros since epoch.
        let ts = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 1).unwrap(),
        );
        assert_eq!(
            DuckValue::Timestamp(ts).to_owned_value().unwrap().get_timestamp_micros().unwrap(),
            1_000_000
        );
    }
}
