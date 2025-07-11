use super::*;

use crate::ffi::{
    duckdb_create_interval, duckdb_create_timestamp, duckdb_get_interval, duckdb_get_timestamp,
    duckdb_interval, duckdb_timestamp,
};
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

impl DuckDialect for StdDuration {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        unsafe {
            let interval = duckdb_get_interval(value);
            // DuckDB interval: months, days, micros
            let total_days = interval.months as u64 * 30 + interval.days as u64;
            let total_micros = total_days * 86_400_000_000 + interval.micros as u64;
            Ok(StdDuration::from_micros(total_micros))
        }
        // match type_ {
        //     DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => ...,
        //     _ => Err(DuckDBConversionError::ConversionError("Invalid duration".to_string())),
        // }
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        // This is a simplification: only micros, no months/days
        let micros = self.as_micros();
        let interval = duckdb_interval { months: 0, days: 0, micros: micros as i64 };
        Ok(unsafe { duckdb_create_interval(interval) })
    }
}

impl DuckDialect for SystemTime {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // if type_ != DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP {
        //     return Err(DuckDBConversionError::TypeMismatch {
        //         expected: DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
        //         found: type_,
        //     });
        // }
        let raw_ts = unsafe { duckdb_get_timestamp(value) };
        let micros = raw_ts.micros;
        let secs = micros / 1_000_000;
        let sub_micros = (micros % 1_000_000) as u32;
        Ok(UNIX_EPOCH + StdDuration::new(secs as u64, sub_micros * 1000))
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let duration = self
            .duration_since(UNIX_EPOCH)
            .map_err(|e| DuckDBConversionError::ConversionError(e.to_string()))?;
        let micros = duration.as_secs() as i64 * 1_000_000 + (duration.subsec_micros() as i64);
        let raw_ts = duckdb_timestamp { micros };
        Ok(unsafe { duckdb_create_timestamp(raw_ts) })
    }
}
