// Chrono implementations (feature-gated)
use super::*;
use crate::error::DuckDBConversionError;
use crate::ffi::{
    duckdb_create_date, duckdb_create_interval, duckdb_create_time, duckdb_create_timestamp,
    duckdb_date, duckdb_from_date, duckdb_get_date, duckdb_get_interval, duckdb_get_time,
    duckdb_get_timestamp, duckdb_interval, duckdb_time, duckdb_timestamp,
};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

impl DuckDialect for Duration {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        unsafe {
            let interval = duckdb_get_interval(value);
            // DuckDB interval: months, days, micros
            let total_days = interval.months as i64 * 30 + interval.days as i64;
            let total_micros = total_days * 86_400_000_000 + interval.micros as i64;
            Ok(Duration::microseconds(total_micros))
        }
        // match type_ {
        //     DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => ...,
        //     _ => Err(DuckDBConversionError::ConversionError("Invalid duration".to_string())),
        // }
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        // This is a simplification: only micros, no months/days
        let micros = self.num_microseconds().unwrap_or(0);
        let interval = duckdb_interval { months: 0, days: 0, micros: micros as i64 };
        Ok(unsafe { duckdb_create_interval(interval) })
    }
}

impl DuckDialect for NaiveDate {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        unsafe {
            let val = duckdb_get_date(value);
            let val = duckdb_from_date(val);
            // val.0: days since 1970-01-01
            NaiveDate::from_ymd_opt(val.year, val.month as u32, val.day as u32)
                .ok_or_else(|| DuckDBConversionError::ConversionError("Invalid date".to_string()))
        }
        // match type_ {
        //     DUCKDB_TYPE_DUCKDB_TYPE_DATE => ...,
        //     _ => Err(DuckDBConversionError::TypeMismatch {
        //         expected: DUCKDB_TYPE_DUCKDB_TYPE_DATE,
        //         found: type_,
        //     }),
        // }
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let days = self.num_days_from_ce() - 719163;
        let raw_date = duckdb_date { days };
        Ok(unsafe { duckdb_create_date(raw_date) })
    }
}

impl DuckDialect for NaiveTime {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // if type_ != DUCKDB_TYPE_DUCKDB_TYPE_TIME {
        //     return Err(DuckDBConversionError::TypeMismatch {
        //         expected: DUCKDB_TYPE_DUCKDB_TYPE_TIME,
        //         found: type_,
        //     });
        // }
        let raw_time = unsafe { duckdb_get_time(value) };
        // raw_time.micros: microseconds since midnight
        NaiveTime::from_num_seconds_from_midnight_opt(
            (raw_time.micros / 1_000_000) as u32,
            ((raw_time.micros % 1_000_000) * 1000) as u32,
        )
        .ok_or_else(|| DuckDBConversionError::ConversionError("Invalid time".to_string()))
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let micros = (self.num_seconds_from_midnight() as i64) * 1_000_000
            + (self.nanosecond() as i64) / 1000;
        let raw_time = duckdb_time { micros };
        Ok(unsafe { duckdb_create_time(raw_time) })
    }
}

impl DuckDialect for NaiveDateTime {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // if type_ != DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP {
        //     return Err(DuckDBConversionError::TypeMismatch {
        //         expected: DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
        //         found: type_,
        //     });
        // }
        let raw_ts = unsafe { duckdb_get_timestamp(value) };
        let micros = raw_ts.micros;
        let days_since_epoch = micros / 86_400_000_000;
        let date = NaiveDate::from_num_days_from_ce_opt(days_since_epoch as i32 + 719163)
            .ok_or_else(|| DuckDBConversionError::ConversionError("Invalid date".to_string()))?;
        let micros_of_day = micros % 86_400_000_000;
        let secs = (micros_of_day / 1_000_000) as u32;
        let nsecs = ((micros_of_day % 1_000_000) * 1000) as u32;
        let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nsecs)
            .ok_or_else(|| DuckDBConversionError::ConversionError("Invalid time".to_string()))?;
        Ok(NaiveDateTime::new(date, time))
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let micros = self.and_utc().timestamp() * 1_000_000
            + (self.and_utc().timestamp_subsec_micros() as i64);
        let raw_ts = duckdb_timestamp { micros };
        Ok(unsafe { duckdb_create_timestamp(raw_ts) })
    }
}

#[cfg(test)]
mod test_chrono_conversion {
    use crate::ffi::duckdb_destroy_value;

    #[test]
    fn test_duration_conversion() {
        use super::*;
        // Test conversion from Rust Duration to DuckDB and back
        let duration = Duration::new(3661, 0).unwrap(); // 1 hour, 1 minute, 1 second
        let mut duck_value = duration.to_duck().unwrap();
        let converted_duration = Duration::from_duck(duck_value).unwrap();
        assert_eq!(duration, converted_duration);
        unsafe { duckdb_destroy_value(&mut duck_value) }; // Clean up DuckDB value
    }
    #[test]
    fn test_date_conversion() {
        use super::*;
        // Test conversion from NaiveDate to DuckDB and back
        let date = NaiveDate::from_ymd_opt(2023, 10, 1).unwrap();
        let mut duck_value = date.to_duck().unwrap();
        let converted_date = NaiveDate::from_duck(duck_value).unwrap();
        assert_eq!(date, converted_date);
        unsafe { duckdb_destroy_value(&mut duck_value) }; // Clean up DuckDB value
    }
    #[test]
    fn test_time_conversion() {
        use super::*;
        // Test conversion from NaiveTime to DuckDB and back
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let mut duck_value = time.to_duck().unwrap();
        let converted_time = NaiveTime::from_duck(duck_value).unwrap();
        assert_eq!(time, converted_time);
        unsafe { duckdb_destroy_value(&mut duck_value) }; // Clean up DuckDB value
    }
    #[test]
    fn test_datetime_conversion() {
        use super::*;
        // Test conversion from NaiveDateTime to DuckDB and back
        let datetime = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2023, 10, 1).unwrap(),
            NaiveTime::from_hms_opt(12, 30, 45).unwrap(),
        );
        let mut duck_value = datetime.to_duck().unwrap();
        let converted_datetime = NaiveDateTime::from_duck(duck_value).unwrap();
        assert_eq!(datetime, converted_datetime);
        unsafe { duckdb_destroy_value(&mut duck_value) }; // Clean up DuckDB value
    }
}
