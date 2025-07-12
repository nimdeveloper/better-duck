use std::ffi::{c_char, CString};
use std::ptr;

use crate::error::Result;
use crate::ffi::{
    duckdb_appender, duckdb_appender_begin_row, duckdb_appender_close, duckdb_appender_create,
    duckdb_appender_destroy, duckdb_appender_end_row, duckdb_appender_flush,
};
use crate::helpers::duck_result::result_from_duckdb_appender;
use crate::raw::connection::RawConnection;
use crate::types::appendable::AppendAble;

pub struct Appender {
    _con: RawConnection,
    inn: duckdb_appender,
}
impl Appender {
    pub fn new(
        con: RawConnection,
        table: &str,
        schema: &str,
    ) -> Result<Appender> {
        let mut appender: duckdb_appender = ptr::null_mut();
        let c_table = CString::new(table).unwrap();
        let c_schema = CString::new(schema).unwrap();
        let res = unsafe {
            duckdb_appender_create(
                con.con,
                c_schema.as_ptr() as *const c_char,
                c_table.as_ptr() as *const c_char,
                &mut appender,
            )
        };
        result_from_duckdb_appender(res, &mut appender)
            .map(|_| Appender { _con: con, inn: appender })
    }

    #[allow(dead_code)]
    pub fn append<T: AppendAble>(
        &mut self,
        row: &mut T,
    ) -> Result<()> {
        let _ = unsafe { duckdb_appender_begin_row(self.inn) };
        row.appender_append(self.inn)?;
        let rc = unsafe { duckdb_appender_end_row(self.inn) };
        result_from_duckdb_appender(rc, &mut self.inn)
    }

    /// Flush data into DB
    #[allow(dead_code)]
    pub fn save(&mut self) -> Result<()> {
        unsafe { self.flush() }
    }

    /// Flush data into DB
    unsafe fn flush(&mut self) -> Result<()> {
        let res = duckdb_appender_flush(self.inn);
        duckdb_appender_destroy(&mut self.inn);
        result_from_duckdb_appender(res, &mut self.inn)
    }
}

impl Drop for Appender {
    fn drop(&mut self) {
        if !self.inn.is_null() {
            unsafe {
                self.flush().unwrap(); // can't safely handle failures here
                duckdb_appender_close(self.inn);
                duckdb_appender_destroy(&mut self.inn);
            }
        }
    }
}

#[cfg(test)]
mod appender_tests {
    use crate::{
        ffi::{duckdb_append_int32, duckdb_append_varchar, duckdb_bind_int32, duckdb_bind_varchar},
        types::value::DuckValue,
    };

    use super::*;
    use crate::{config::Config, error::DuckDBConversionError, helpers::path::path_to_cstring};

    #[derive(Debug)]
    struct Row(i32, &'static str);

    impl AppendAble for Row {
        fn appender_append(
            &mut self,
            appender: duckdb_appender,
        ) -> crate::error::Result<()> {
            unsafe {
                duckdb_append_int32(appender, self.0);
                let st = CString::new(self.1)
                    .map_err(|e| DuckDBConversionError::ConversionError(e.to_string()))
                    .unwrap();
                duckdb_append_varchar(appender, st.as_ptr());
            }
            Ok(())
        }
        fn stmt_append(
            &mut self,
            idx: u64,
            stmt: crate::ffi::duckdb_prepared_statement,
        ) -> Result<()> {
            unsafe {
                duckdb_bind_int32(stmt, idx, self.0);
                let st = CString::new(self.1)
                    .map_err(|e| DuckDBConversionError::ConversionError(e.to_string()))
                    .unwrap();
                duckdb_bind_varchar(stmt, idx + 1, st.as_ptr());
            }
            Ok(())
        }
    }

    fn get_test_connection() -> RawConnection {
        // This should be replaced with a real connection setup for integration tests
        // For now, this is a placeholder and will not work unless implemented
        // Setup: create a connection (assuming test DB in memory)
        let c_path = path_to_cstring(":memory:".as_ref()).unwrap();
        let config = Config::default().with("duckdb_api", "rust").unwrap();
        RawConnection::open_with_flags(&c_path, config).unwrap()
    }

    #[test]
    fn test_appender_create_and_drop() {
        let mut con = get_test_connection();

        // Create a table for testing
        let create_sql = "CREATE TABLE test_appender (id INTEGER, name VARCHAR)";
        con.execute(create_sql).unwrap();

        // Test: create appender
        let appender = Appender::new(con.clone(), "test_appender", "main");
        assert!(appender.is_ok());
        // Drop happens automatically, should not panic
    }

    #[test]
    fn test_appender_append_and_flush() {
        let mut con = get_test_connection();

        // Make sure table exists
        con.execute("CREATE TABLE test_append (id INTEGER, name VARCHAR)").unwrap();

        // panic!("TES");
        let mut appender = Appender::new(con.clone(), "test_append", "main").unwrap();
        let mut row = Row(1, "Alice");
        let mut row2 = Row(2, "Sara");
        let mut row3 = Row(3, "Charlie");

        appender.append(&mut row).unwrap();
        appender.append(&mut row2).unwrap();
        appender.append(&mut row3).unwrap();
        appender.save().unwrap();

        // Verify: query the table and check the inserted data

        // Check non-existent row
        let mut stmt = con.prepare("SELECT id,name FROM test_append WHERE id=123").unwrap();
        let mut rows = stmt.fetch().unwrap().unwrap();
        assert!(rows.next().is_none(), "Row with id=123 should not exist");

        // Verify all rows
        let mut stmt = con.prepare("SELECT id,name FROM test_append").unwrap();

        let rows = stmt.fetch().unwrap();
        assert!(!rows.is_none());
        let rows = rows.unwrap();
        for row in rows {
            assert!(row.is_ok());
            let row = row.unwrap();
            let id = match row.get("id").unwrap() {
                DuckValue::Int(id) => *id,
                other => panic!("Expected Int for 'id', got {:?}", other),
            };
            assert!([1, 2, 3].contains(&id), "Row with id={} should exist", id);
            let name = match row.get("name").unwrap() {
                DuckValue::Text(name) => name.as_str(),
                other => panic!("Expected Str for 'name', got {:?}", other),
            };
            match id {
                1 => assert_eq!(name, "Alice"),
                2 => assert_eq!(name, "Sara"),
                3 => assert_eq!(name, "Charlie"),
                _ => panic!("Unexpected row id: {}", id),
            }
        }
    }

    #[test]
    fn test_appender_error_on_invalid_table() {
        // Setup: create a connection (assuming test DB in memory)
        let c_path = path_to_cstring(":memory:".as_ref()).unwrap();
        let config = Config::default().with("duckdb_api", "rust").unwrap();
        let con = RawConnection::open_with_flags(&c_path, config).unwrap();

        // Try to create appender for non-existent table
        let appender = Appender::new(con, "nonexistent_table", "main");
        assert!(appender.is_err());
    }
}
