use crate::{
    error::{Error, Result},
    ffi,
};
use std::{
    default::Default,
    ffi::{CStr, CString},
    os::raw::c_char,
    ptr,
};

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

/// A DuckDB configuration flag's human-readable name and description, as reported
/// by [`Config::flag`] / [`Config::flags`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigFlag {
    /// The flag's name (e.g. `"access_mode"`), usable as a key for [`Config::with`].
    pub name: String,
    /// A human-readable description of what the flag controls.
    pub description: String,
}

/// The version string of the linked DuckDB library (e.g. `"v1.5.5"`).
///
/// Wraps `duckdb_library_version`, whose result is a static string owned by DuckDB
/// (never freed); the bytes are copied into an owned `String`.
#[must_use]
pub fn library_version() -> String {
    // SAFETY: `duckdb_library_version` returns a static, null-terminated string that
    // must NOT be freed; we only read and copy it.
    let ptr = unsafe { ffi::duckdb_library_version() };
    if ptr.is_null() {
        return String::new();
    }
    // SAFETY: `ptr` is a valid, non-null, null-terminated static C string.
    unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned()
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

    /// The number of configuration flags DuckDB recognises.
    ///
    /// Flags are addressable by index in `0..flag_count()` via [`Config::flag`].
    #[must_use]
    pub fn flag_count() -> usize {
        // SAFETY: `duckdb_config_count` takes no arguments and only reads a static table.
        unsafe { ffi::duckdb_config_count() }
    }

    /// The name and description of the configuration flag at `index`, or `None` if
    /// `index >= flag_count()`.
    ///
    /// Wraps `duckdb_get_config_flag`; the returned name/description are static
    /// strings owned by DuckDB (never freed) and are copied into the [`ConfigFlag`].
    #[must_use]
    pub fn flag(index: usize) -> Option<ConfigFlag> {
        let mut name: *const c_char = ptr::null();
        let mut description: *const c_char = ptr::null();
        // SAFETY: `name`/`description` are valid out-pointers. On success DuckDB writes
        // pointers to static strings (which must NOT be freed); an out-of-range index
        // returns `DuckDBError` and leaves them untouched.
        let state = unsafe { ffi::duckdb_get_config_flag(index, &mut name, &mut description) };
        if state != ffi::DuckDBSuccess || name.is_null() || description.is_null() {
            return None;
        }
        // SAFETY: `name` is a valid, non-null, null-terminated static C string owned
        // by DuckDB; we copy it out and never free it.
        let name = unsafe { CStr::from_ptr(name) }.to_string_lossy().into_owned();
        // SAFETY: `description` is likewise a valid static C string owned by DuckDB.
        let description = unsafe { CStr::from_ptr(description) }.to_string_lossy().into_owned();
        Some(ConfigFlag { name, description })
    }

    /// Iterates every configuration flag DuckDB recognises, in index order.
    pub fn flags() -> impl Iterator<Item = ConfigFlag> {
        (0..Self::flag_count()).filter_map(Self::flag)
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

    /// Access mode of the database (`AUTOMATIC`, `READ_ONLY`, or `READ_WRITE`)
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

    /// The order type used when none is specified (`ASC` or `DESC`)
    #[allow(unused)]
    pub fn default_order(
        mut self,
        order: DefaultOrder,
    ) -> Result<Config> {
        self.set("default_order", &order.to_string())?;
        Ok(self)
    }

    /// Null ordering used when none is specified (`NULLS_FIRST` or `NULLS_LAST`)
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
            // SAFETY: `config` is a valid output pointer; `duckdb_create_config` initializes it.
            let state = unsafe { ffi::duckdb_create_config(&mut config) };
            if state != ffi::DuckDBSuccess {
                return Err(Error::DuckDBFailure(
                    ffi::Error::new(state),
                    Some("failed to create duckdb_config".to_owned()),
                ));
            }
            self.config = Some(config);
        }

        let c_key = CString::new(key)?;
        let c_value = CString::new(value)?;
        // SAFETY: `self.config` is Some — either it was just initialized above or it was
        // already Some. `c_key` and `c_value` are valid null-terminated C strings that
        // outlive this call. `duckdb_set_config` does not retain the string pointers.
        let state = unsafe {
            ffi::duckdb_set_config(
                self.config.expect("config always initialized before this point"),
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

// SAFETY: `duckdb_config` is only mutated through `&mut self` methods (`set`), so no
// two threads can touch it concurrently even after a move. DuckDB does not associate
// the config handle with the thread that created it before it is consumed by
// `duckdb_open_ext`/`duckdb_connect`.
unsafe impl Send for Config {}

impl Drop for Config {
    fn drop(&mut self) {
        // SAFETY: `cfg` is a valid duckdb_config created in `set` and not yet destroyed.
        // `take()` sets `self.config` to None, so this runs at most once even if
        // `Drop` is called multiple times (which Rust prevents, but belt-and-suspenders).
        if let Some(mut cfg) = self.config.take() {
            // SAFETY: `cfg` is a valid duckdb_config created in `set` and not yet
            // destroyed. `take()` ensures this block runs at most once.
            unsafe { ffi::duckdb_destroy_config(&mut cfg) };
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

    #[test]
    fn test_rejects_interior_nul_in_key_or_value() {
        let key_error = Config::default().with("bad\0key", "value").err().unwrap();
        assert!(matches!(key_error, Error::NulError(_)));

        let value_error = Config::default().with("threads", "1\0extra").err().unwrap();
        assert!(matches!(value_error, Error::NulError(_)));
    }

    #[test]
    fn config_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Config>();
    }

    #[test]
    fn library_version_is_reported() {
        let version = super::library_version();
        assert!(!version.is_empty(), "library version must not be empty");
        // DuckDB reports versions like "v1.5.5"; at minimum it should contain a digit.
        assert!(version.chars().any(|c| c.is_ascii_digit()), "version {version:?} has no digit");
    }

    #[test]
    fn config_flags_are_discoverable() {
        let count = Config::flag_count();
        assert!(count > 0, "DuckDB must expose at least one config flag");

        // Index 0 is in range; one past the end is not.
        assert!(Config::flag(0).is_some());
        assert!(Config::flag(count).is_none(), "index == count must be out of range");

        // Iterating yields exactly `count` flags, each with a non-empty name.
        let flags: Vec<_> = Config::flags().collect();
        assert_eq!(flags.len(), count);
        assert!(flags.iter().all(|f| !f.name.is_empty()));

        // A stable, well-known flag is present and usable as a `with` key.
        let threads = flags.iter().find(|f| f.name == "threads").expect("`threads` flag missing");
        assert!(!threads.description.is_empty());
        assert!(Config::default().with(&threads.name, "2").is_ok());
    }
}
