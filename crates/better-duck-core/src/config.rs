use crate::{
    error::{Error, Result},
    ffi,
};
use std::{default::Default, ffi::CString, os::raw::c_char, ptr};

use strum::{Display, EnumString};

/// duckdb access mode, default is Automatic
#[derive(Debug, Eq, PartialEq, EnumString, Display)]
pub enum AccessMode {
    /// Access mode of the database AUTOMATIC
    #[strum(to_string = "AUTOMATIC")]
    Automatic,
    /// Access mode of the database READ_ONLY
    #[strum(to_string = "READ_ONLY")]
    ReadOnly,
    /// Access mode of the database READ_WRITE
    #[strum(to_string = "READ_WRITE")]
    ReadWrite,
}

/// duckdb default order, default is Asc
#[derive(Debug, Eq, PartialEq, EnumString, Display)]
pub enum DefaultOrder {
    /// The order type, ASC
    #[strum(to_string = "ASC")]
    Asc,
    /// The order type, DESC
    #[strum(to_string = "DESC")]
    Desc,
}

/// duckdb default null order, default is nulls first
#[derive(Debug, Eq, PartialEq, EnumString, Display)]
pub enum DefaultNullOrder {
    /// Null ordering, NullsFirst
    #[strum(to_string = "NULLS_FIRST")]
    NullsFirst,
    /// Null ordering, NullsLast
    #[strum(to_string = "NULLS_LAST")]
    NullsLast,
}

/// duckdb configuration
/// Refer to <https://github.com/duckdb/duckdb/blob/master/src/main/config.cpp>
#[derive(Default)]
pub struct Config {
    config: Option<ffi::duckdb_config>,
}

impl Config {
    pub(crate) fn duckdb_config(&self) -> ffi::duckdb_config {
        self.config.unwrap_or(std::ptr::null_mut() as ffi::duckdb_config)
    }

    /// enable autoload extensions
    #[allow(unused)]
    pub fn enable_autoload_extension(
        mut self,
        enabled: bool,
    ) -> Result<Config> {
        self.set("autoinstall_known_extensions", &(enabled as i32).to_string())?;
        self.set("autoload_known_extensions", &(enabled as i32).to_string())?;
        Ok(self)
    }

    /// Access mode of the database ([AUTOMATIC], READ_ONLY or READ_WRITE)
    #[allow(unused)]
    pub fn access_mode(
        mut self,
        mode: AccessMode,
    ) -> Result<Config> {
        self.set("access_mode", &mode.to_string())?;
        Ok(self)
    }

    /// Metadata from DuckDB callers
    #[allow(unused)]
    pub fn custom_user_agent(
        mut self,
        custom_user_agent: &str,
    ) -> Result<Config> {
        self.set("custom_user_agent", custom_user_agent)?;
        Ok(self)
    }

    /// The order type used when none is specified ([ASC] or DESC)
    #[allow(unused)]
    pub fn default_order(
        mut self,
        order: DefaultOrder,
    ) -> Result<Config> {
        self.set("default_order", &order.to_string())?;
        Ok(self)
    }

    /// Null ordering used when none is specified ([NULLS_FIRST] or NULLS_LAST)
    #[allow(unused)]
    pub fn default_null_order(
        mut self,
        null_order: DefaultNullOrder,
    ) -> Result<Config> {
        self.set("default_null_order", &null_order.to_string())?;
        Ok(self)
    }

    /// Allow the database to access external state (through e.g. COPY TO/FROM, CSV readers, pandas replacement scans, etc)
    #[allow(unused)]
    pub fn enable_external_access(
        mut self,
        enabled: bool,
    ) -> Result<Config> {
        self.set("enable_external_access", &enabled.to_string())?;
        Ok(self)
    }

    /// Whether or not object cache is used to cache e.g. Parquet metadata
    #[allow(unused)]
    pub fn enable_object_cache(
        mut self,
        enabled: bool,
    ) -> Result<Config> {
        self.set("enable_object_cache", &enabled.to_string())?;
        Ok(self)
    }

    /// Allow to load third-party duckdb extensions.
    #[allow(unused)]
    pub fn allow_unsigned_extensions(mut self) -> Result<Config> {
        self.set("allow_unsigned_extensions", "true")?;
        Ok(self)
    }

    /// The maximum memory of the system (e.g. 1GB)
    #[allow(unused)]
    pub fn max_memory(
        mut self,
        memory: &str,
    ) -> Result<Config> {
        self.set("max_memory", memory)?;
        Ok(self)
    }

    /// The number of total threads used by the system
    #[allow(unused)]
    pub fn threads(
        mut self,
        thread_num: i64,
    ) -> Result<Config> {
        self.set("threads", &thread_num.to_string())?;
        Ok(self)
    }

    /// Add any setting to the config. DuckDB will return an error if the setting is unknown or
    /// otherwise invalid.
    pub fn with(
        mut self,
        key: impl AsRef<str>,
        value: impl AsRef<str>,
    ) -> Result<Config> {
        self.set(key.as_ref(), value.as_ref())?;
        Ok(self)
    }

    fn set(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<()> {
        if self.config.is_none() {
            let mut config: ffi::duckdb_config = ptr::null_mut();
            let state = unsafe { ffi::duckdb_create_config(&mut config) };
            assert_eq!(state, ffi::DuckDBSuccess);
            self.config = Some(config);
        }

        let c_key = CString::new(key).unwrap();
        let c_value = CString::new(value).unwrap();
        let state = unsafe {
            ffi::duckdb_set_config(
                self.config.unwrap(),
                c_key.as_ptr() as *const c_char,
                c_value.as_ptr() as *const c_char,
            )
        };
        if state != ffi::DuckDBSuccess {
            return Err(Error::DuckDBFailure(
                ffi::Error::new(state),
                Some(format!("set {key}:{value} error")),
            ));
        }
        Ok(())
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        if self.config.is_some() {
            unsafe { ffi::duckdb_destroy_config(&mut self.config.unwrap()) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Dummy error module for testing if not present
    #[allow(dead_code)]
    mod error {
        use std::fmt;

        #[derive(Debug)]
        pub enum Error {
            DuckDBFailure(super::ffi::Error, Option<String>),
        }
        pub type Result<T> = std::result::Result<T, Error>;
        impl fmt::Display for Error {
            fn fmt(
                &self,
                f: &mut fmt::Formatter<'_>,
            ) -> fmt::Result {
                write!(f, "{:?}", self)
            }
        }
        impl std::error::Error for Error {}
    }

    #[test]
    fn test_access_mode_enum() {
        assert_eq!(AccessMode::Automatic.to_string(), "AUTOMATIC");
        assert_eq!(AccessMode::ReadOnly.to_string(), "READ_ONLY");
        assert_eq!(AccessMode::ReadWrite.to_string(), "READ_WRITE");
        assert_eq!("AUTOMATIC".parse::<AccessMode>().unwrap(), AccessMode::Automatic);
        assert_eq!("READ_ONLY".parse::<AccessMode>().unwrap(), AccessMode::ReadOnly);
        assert_eq!("READ_WRITE".parse::<AccessMode>().unwrap(), AccessMode::ReadWrite);
    }

    #[test]
    fn test_default_order_enum() {
        assert_eq!(DefaultOrder::Asc.to_string(), "ASC");
        assert_eq!(DefaultOrder::Desc.to_string(), "DESC");
        assert_eq!("ASC".parse::<DefaultOrder>().unwrap(), DefaultOrder::Asc);
        assert_eq!("DESC".parse::<DefaultOrder>().unwrap(), DefaultOrder::Desc);
    }

    #[test]
    fn test_default_null_order_enum() {
        assert_eq!(DefaultNullOrder::NullsFirst.to_string(), "NULLS_FIRST");
        assert_eq!(DefaultNullOrder::NullsLast.to_string(), "NULLS_LAST");
        assert_eq!(
            "NULLS_FIRST".parse::<DefaultNullOrder>().unwrap(),
            DefaultNullOrder::NullsFirst
        );
        assert_eq!("NULLS_LAST".parse::<DefaultNullOrder>().unwrap(), DefaultNullOrder::NullsLast);
    }

    #[test]
    fn test_enable_autoload_extension() {
        let config = Config::default().enable_autoload_extension(true);
        assert!(config.is_ok());
        let config = Config::default().enable_autoload_extension(false);
        assert!(config.is_ok());
    }

    #[test]
    fn test_access_mode_method() {
        let config = Config::default().access_mode(AccessMode::ReadOnly);
        assert!(config.is_ok());
    }

    #[test]
    fn test_custom_user_agent() {
        let config = Config::default().custom_user_agent("my-agent/1.0");
        assert!(config.is_ok());
    }

    #[test]
    fn test_default_order_method() {
        let config = Config::default().default_order(DefaultOrder::Desc);
        assert!(config.is_ok());
    }

    #[test]
    fn test_default_null_order_method() {
        let config = Config::default().default_null_order(DefaultNullOrder::NullsLast);
        assert!(config.is_ok());
    }

    #[test]
    fn test_enable_external_access() {
        let config = Config::default().enable_external_access(true);
        assert!(config.is_ok());
    }

    #[test]
    fn test_enable_object_cache() {
        let config = Config::default().enable_object_cache(true);
        assert!(config.is_ok());
    }

    #[test]
    fn test_allow_unsigned_extensions() {
        let config = Config::default().allow_unsigned_extensions();
        assert!(config.is_ok());
    }

    #[test]
    fn test_max_memory() {
        let config = Config::default().max_memory("512MB");
        assert!(config.is_ok());
    }

    #[test]
    fn test_threads() {
        let config = Config::default().threads(8);
        assert!(config.is_ok());
    }

    #[test]
    fn test_with() {
        let config = Config::default().with("some_key", "some_value");
        assert!(config.is_ok());
    }

    #[test]
    fn test_set_multiple_options() {
        let config = Config::default()
            .enable_autoload_extension(true)
            .and_then(|c| c.access_mode(AccessMode::ReadWrite))
            .and_then(|c| c.max_memory("1GB"))
            .and_then(|c| c.threads(4));
        assert!(config.is_ok());
    }

    #[test]
    fn test_config_drop() {
        // Just ensure drop does not panic
        let config = Config::default().enable_autoload_extension(true).unwrap();
        drop(config);
    }
}
