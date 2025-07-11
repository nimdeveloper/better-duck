use crate::ffi::{duckdb_appender, duckdb_prepared_statement};

use crate::error::Result;

pub trait AppendAble {
    /// Appends a value to the statement.
    ///
    /// # Arguments
    ///
    /// * `idx` - The index at which to append the value (0-based).
    /// * `stmt` - The prepared statement to append the value to.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the value was successfully appended, or an error if the operation failed.
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: duckdb_prepared_statement,
    ) -> Result<()> {
        unimplemented!(
            "Statement bind is not implemented for this type: {}",
            std::any::type_name::<Self>()
        );
    }
    /// Appends a value to the appender.
    ///
    /// # Arguments
    ///
    /// * `appender` - The appender to append the value to.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the value was successfully appended, or an error if the operation failed.
    fn appender_append(
        &mut self,
        appender: duckdb_appender,
    ) -> Result<()> {
        unimplemented!(
            "Appender append is not implemented for this type: {}",
            std::any::type_name::<Self>()
        );
    }
}
