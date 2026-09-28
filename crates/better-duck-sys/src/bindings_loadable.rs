//! Loadable-extension FFI surface — see xtask `generate_loadable_bindings`.
#![allow(non_camel_case_types, non_snake_case, dead_code)]
#![allow(rustdoc::broken_intra_doc_links, rustdoc::bare_urls)]
pub const DUCKDB_TYPE_DUCKDB_TYPE_INVALID: DUCKDB_TYPE = 0;
pub const DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN: DUCKDB_TYPE = 1;
pub const DUCKDB_TYPE_DUCKDB_TYPE_TINYINT: DUCKDB_TYPE = 2;
pub const DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT: DUCKDB_TYPE = 3;
pub const DUCKDB_TYPE_DUCKDB_TYPE_INTEGER: DUCKDB_TYPE = 4;
pub const DUCKDB_TYPE_DUCKDB_TYPE_BIGINT: DUCKDB_TYPE = 5;
pub const DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT: DUCKDB_TYPE = 6;
pub const DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT: DUCKDB_TYPE = 7;
pub const DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER: DUCKDB_TYPE = 8;
pub const DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT: DUCKDB_TYPE = 9;
pub const DUCKDB_TYPE_DUCKDB_TYPE_FLOAT: DUCKDB_TYPE = 10;
pub const DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE: DUCKDB_TYPE = 11;
pub const DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP: DUCKDB_TYPE = 12;
pub const DUCKDB_TYPE_DUCKDB_TYPE_DATE: DUCKDB_TYPE = 13;
pub const DUCKDB_TYPE_DUCKDB_TYPE_TIME: DUCKDB_TYPE = 14;
pub const DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL: DUCKDB_TYPE = 15;
pub const DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT: DUCKDB_TYPE = 16;
pub const DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT: DUCKDB_TYPE = 32;
pub const DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR: DUCKDB_TYPE = 17;
pub const DUCKDB_TYPE_DUCKDB_TYPE_BLOB: DUCKDB_TYPE = 18;
pub const DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL: DUCKDB_TYPE = 19;
pub const DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S: DUCKDB_TYPE = 20;
pub const DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS: DUCKDB_TYPE = 21;
pub const DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS: DUCKDB_TYPE = 22;
pub const DUCKDB_TYPE_DUCKDB_TYPE_ENUM: DUCKDB_TYPE = 23;
pub const DUCKDB_TYPE_DUCKDB_TYPE_LIST: DUCKDB_TYPE = 24;
pub const DUCKDB_TYPE_DUCKDB_TYPE_STRUCT: DUCKDB_TYPE = 25;
pub const DUCKDB_TYPE_DUCKDB_TYPE_MAP: DUCKDB_TYPE = 26;
pub const DUCKDB_TYPE_DUCKDB_TYPE_ARRAY: DUCKDB_TYPE = 33;
pub const DUCKDB_TYPE_DUCKDB_TYPE_UUID: DUCKDB_TYPE = 27;
pub const DUCKDB_TYPE_DUCKDB_TYPE_UNION: DUCKDB_TYPE = 28;
pub const DUCKDB_TYPE_DUCKDB_TYPE_BIT: DUCKDB_TYPE = 29;
pub const DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ: DUCKDB_TYPE = 30;
pub const DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ: DUCKDB_TYPE = 31;
pub const DUCKDB_TYPE_DUCKDB_TYPE_ANY: DUCKDB_TYPE = 34;
pub const DUCKDB_TYPE_DUCKDB_TYPE_BIGNUM: DUCKDB_TYPE = 35;
pub const DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL: DUCKDB_TYPE = 36;
pub const DUCKDB_TYPE_DUCKDB_TYPE_STRING_LITERAL: DUCKDB_TYPE = 37;
pub const DUCKDB_TYPE_DUCKDB_TYPE_INTEGER_LITERAL: DUCKDB_TYPE = 38;
pub const DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS: DUCKDB_TYPE = 39;
pub const DUCKDB_TYPE_DUCKDB_TYPE_GEOMETRY: DUCKDB_TYPE = 40;
pub const DUCKDB_TYPE_DUCKDB_TYPE_VARIANT: DUCKDB_TYPE = 41;
pub type DUCKDB_TYPE = ::std::os::raw::c_uint;
pub use self::DUCKDB_TYPE as duckdb_type;
pub const duckdb_state_DuckDBSuccess: duckdb_state = 0;
pub const duckdb_state_DuckDBError: duckdb_state = 1;
pub type duckdb_state = ::std::os::raw::c_uint;
pub const duckdb_pending_state_DUCKDB_PENDING_RESULT_READY: duckdb_pending_state = 0;
pub const duckdb_pending_state_DUCKDB_PENDING_RESULT_NOT_READY: duckdb_pending_state = 1;
pub const duckdb_pending_state_DUCKDB_PENDING_ERROR: duckdb_pending_state = 2;
pub const duckdb_pending_state_DUCKDB_PENDING_NO_TASKS_AVAILABLE: duckdb_pending_state = 3;
pub type duckdb_pending_state = ::std::os::raw::c_uint;
pub const duckdb_result_type_DUCKDB_RESULT_TYPE_INVALID: duckdb_result_type = 0;
pub const duckdb_result_type_DUCKDB_RESULT_TYPE_CHANGED_ROWS: duckdb_result_type = 1;
pub const duckdb_result_type_DUCKDB_RESULT_TYPE_NOTHING: duckdb_result_type = 2;
pub const duckdb_result_type_DUCKDB_RESULT_TYPE_QUERY_RESULT: duckdb_result_type = 3;
pub type duckdb_result_type = ::std::os::raw::c_uint;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_INVALID: duckdb_statement_type = 0;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_SELECT: duckdb_statement_type = 1;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_INSERT: duckdb_statement_type = 2;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_UPDATE: duckdb_statement_type = 3;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_EXPLAIN: duckdb_statement_type = 4;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_DELETE: duckdb_statement_type = 5;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_PREPARE: duckdb_statement_type = 6;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_CREATE: duckdb_statement_type = 7;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_EXECUTE: duckdb_statement_type = 8;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_ALTER: duckdb_statement_type = 9;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_TRANSACTION: duckdb_statement_type = 10;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_COPY: duckdb_statement_type = 11;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_ANALYZE: duckdb_statement_type = 12;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_VARIABLE_SET: duckdb_statement_type = 13;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_CREATE_FUNC: duckdb_statement_type = 14;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_DROP: duckdb_statement_type = 15;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_EXPORT: duckdb_statement_type = 16;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_PRAGMA: duckdb_statement_type = 17;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_VACUUM: duckdb_statement_type = 18;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_CALL: duckdb_statement_type = 19;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_SET: duckdb_statement_type = 20;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_LOAD: duckdb_statement_type = 21;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_RELATION: duckdb_statement_type = 22;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_EXTENSION: duckdb_statement_type = 23;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_LOGICAL_PLAN: duckdb_statement_type = 24;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_ATTACH: duckdb_statement_type = 25;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_DETACH: duckdb_statement_type = 26;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_MULTI: duckdb_statement_type = 27;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_COPY_DATABASE: duckdb_statement_type = 28;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_UPDATE_EXTENSIONS: duckdb_statement_type = 29;
pub const duckdb_statement_type_DUCKDB_STATEMENT_TYPE_MERGE_INTO: duckdb_statement_type = 30;
pub type duckdb_statement_type = ::std::os::raw::c_uint;
pub const duckdb_error_type_DUCKDB_ERROR_INVALID: duckdb_error_type = 0;
pub const duckdb_error_type_DUCKDB_ERROR_OUT_OF_RANGE: duckdb_error_type = 1;
pub const duckdb_error_type_DUCKDB_ERROR_CONVERSION: duckdb_error_type = 2;
pub const duckdb_error_type_DUCKDB_ERROR_UNKNOWN_TYPE: duckdb_error_type = 3;
pub const duckdb_error_type_DUCKDB_ERROR_DECIMAL: duckdb_error_type = 4;
pub const duckdb_error_type_DUCKDB_ERROR_MISMATCH_TYPE: duckdb_error_type = 5;
pub const duckdb_error_type_DUCKDB_ERROR_DIVIDE_BY_ZERO: duckdb_error_type = 6;
pub const duckdb_error_type_DUCKDB_ERROR_OBJECT_SIZE: duckdb_error_type = 7;
pub const duckdb_error_type_DUCKDB_ERROR_INVALID_TYPE: duckdb_error_type = 8;
pub const duckdb_error_type_DUCKDB_ERROR_SERIALIZATION: duckdb_error_type = 9;
pub const duckdb_error_type_DUCKDB_ERROR_TRANSACTION: duckdb_error_type = 10;
pub const duckdb_error_type_DUCKDB_ERROR_NOT_IMPLEMENTED: duckdb_error_type = 11;
pub const duckdb_error_type_DUCKDB_ERROR_EXPRESSION: duckdb_error_type = 12;
pub const duckdb_error_type_DUCKDB_ERROR_CATALOG: duckdb_error_type = 13;
pub const duckdb_error_type_DUCKDB_ERROR_PARSER: duckdb_error_type = 14;
pub const duckdb_error_type_DUCKDB_ERROR_PLANNER: duckdb_error_type = 15;
pub const duckdb_error_type_DUCKDB_ERROR_SCHEDULER: duckdb_error_type = 16;
pub const duckdb_error_type_DUCKDB_ERROR_EXECUTOR: duckdb_error_type = 17;
pub const duckdb_error_type_DUCKDB_ERROR_CONSTRAINT: duckdb_error_type = 18;
pub const duckdb_error_type_DUCKDB_ERROR_INDEX: duckdb_error_type = 19;
pub const duckdb_error_type_DUCKDB_ERROR_STAT: duckdb_error_type = 20;
pub const duckdb_error_type_DUCKDB_ERROR_CONNECTION: duckdb_error_type = 21;
pub const duckdb_error_type_DUCKDB_ERROR_SYNTAX: duckdb_error_type = 22;
pub const duckdb_error_type_DUCKDB_ERROR_SETTINGS: duckdb_error_type = 23;
pub const duckdb_error_type_DUCKDB_ERROR_BINDER: duckdb_error_type = 24;
pub const duckdb_error_type_DUCKDB_ERROR_NETWORK: duckdb_error_type = 25;
pub const duckdb_error_type_DUCKDB_ERROR_OPTIMIZER: duckdb_error_type = 26;
pub const duckdb_error_type_DUCKDB_ERROR_NULL_POINTER: duckdb_error_type = 27;
pub const duckdb_error_type_DUCKDB_ERROR_IO: duckdb_error_type = 28;
pub const duckdb_error_type_DUCKDB_ERROR_INTERRUPT: duckdb_error_type = 29;
pub const duckdb_error_type_DUCKDB_ERROR_FATAL: duckdb_error_type = 30;
pub const duckdb_error_type_DUCKDB_ERROR_INTERNAL: duckdb_error_type = 31;
pub const duckdb_error_type_DUCKDB_ERROR_INVALID_INPUT: duckdb_error_type = 32;
pub const duckdb_error_type_DUCKDB_ERROR_OUT_OF_MEMORY: duckdb_error_type = 33;
pub const duckdb_error_type_DUCKDB_ERROR_PERMISSION: duckdb_error_type = 34;
pub const duckdb_error_type_DUCKDB_ERROR_PARAMETER_NOT_RESOLVED: duckdb_error_type = 35;
pub const duckdb_error_type_DUCKDB_ERROR_PARAMETER_NOT_ALLOWED: duckdb_error_type = 36;
pub const duckdb_error_type_DUCKDB_ERROR_DEPENDENCY: duckdb_error_type = 37;
pub const duckdb_error_type_DUCKDB_ERROR_HTTP: duckdb_error_type = 38;
pub const duckdb_error_type_DUCKDB_ERROR_MISSING_EXTENSION: duckdb_error_type = 39;
pub const duckdb_error_type_DUCKDB_ERROR_AUTOLOAD: duckdb_error_type = 40;
pub const duckdb_error_type_DUCKDB_ERROR_SEQUENCE: duckdb_error_type = 41;
pub const duckdb_error_type_DUCKDB_INVALID_CONFIGURATION: duckdb_error_type = 42;
pub type duckdb_error_type = ::std::os::raw::c_uint;
pub const duckdb_cast_mode_DUCKDB_CAST_NORMAL: duckdb_cast_mode = 0;
pub const duckdb_cast_mode_DUCKDB_CAST_TRY: duckdb_cast_mode = 1;
pub type duckdb_cast_mode = ::std::os::raw::c_uint;
pub const duckdb_file_flag_DUCKDB_FILE_FLAG_INVALID: duckdb_file_flag = 0;
pub const duckdb_file_flag_DUCKDB_FILE_FLAG_READ: duckdb_file_flag = 1;
pub const duckdb_file_flag_DUCKDB_FILE_FLAG_WRITE: duckdb_file_flag = 2;
pub const duckdb_file_flag_DUCKDB_FILE_FLAG_CREATE: duckdb_file_flag = 3;
pub const duckdb_file_flag_DUCKDB_FILE_FLAG_CREATE_NEW: duckdb_file_flag = 4;
pub const duckdb_file_flag_DUCKDB_FILE_FLAG_APPEND: duckdb_file_flag = 5;
pub type duckdb_file_flag = ::std::os::raw::c_uint;
pub const duckdb_config_option_scope_DUCKDB_CONFIG_OPTION_SCOPE_INVALID:
    duckdb_config_option_scope = 0;
pub const duckdb_config_option_scope_DUCKDB_CONFIG_OPTION_SCOPE_LOCAL: duckdb_config_option_scope =
    1;
pub const duckdb_config_option_scope_DUCKDB_CONFIG_OPTION_SCOPE_SESSION:
    duckdb_config_option_scope = 2;
pub const duckdb_config_option_scope_DUCKDB_CONFIG_OPTION_SCOPE_GLOBAL: duckdb_config_option_scope =
    3;
pub type duckdb_config_option_scope = ::std::os::raw::c_uint;
pub const duckdb_catalog_entry_type_DUCKDB_CATALOG_ENTRY_TYPE_INVALID: duckdb_catalog_entry_type =
    0;
pub const duckdb_catalog_entry_type_DUCKDB_CATALOG_ENTRY_TYPE_TABLE: duckdb_catalog_entry_type = 1;
pub const duckdb_catalog_entry_type_DUCKDB_CATALOG_ENTRY_TYPE_SCHEMA: duckdb_catalog_entry_type = 2;
pub const duckdb_catalog_entry_type_DUCKDB_CATALOG_ENTRY_TYPE_VIEW: duckdb_catalog_entry_type = 3;
pub const duckdb_catalog_entry_type_DUCKDB_CATALOG_ENTRY_TYPE_INDEX: duckdb_catalog_entry_type = 4;
pub const duckdb_catalog_entry_type_DUCKDB_CATALOG_ENTRY_TYPE_PREPARED_STATEMENT:
    duckdb_catalog_entry_type = 5;
pub const duckdb_catalog_entry_type_DUCKDB_CATALOG_ENTRY_TYPE_SEQUENCE: duckdb_catalog_entry_type =
    6;
pub const duckdb_catalog_entry_type_DUCKDB_CATALOG_ENTRY_TYPE_COLLATION: duckdb_catalog_entry_type =
    7;
pub const duckdb_catalog_entry_type_DUCKDB_CATALOG_ENTRY_TYPE_TYPE: duckdb_catalog_entry_type = 8;
pub const duckdb_catalog_entry_type_DUCKDB_CATALOG_ENTRY_TYPE_DATABASE: duckdb_catalog_entry_type =
    9;
pub type duckdb_catalog_entry_type = ::std::os::raw::c_uint;
pub type idx_t = u64;
pub type sel_t = u32;
pub type duckdb_delete_callback_t =
    ::std::option::Option<unsafe extern "C" fn(data: *mut ::std::os::raw::c_void)>;
pub type duckdb_copy_callback_t = ::std::option::Option<
    unsafe extern "C" fn(data: *mut ::std::os::raw::c_void) -> *mut ::std::os::raw::c_void,
>;
pub type duckdb_task_state = *mut ::std::os::raw::c_void;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_date {
    pub days: i32,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_date_struct {
    pub year: i32,
    pub month: i8,
    pub day: i8,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_time {
    pub micros: i64,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_time_struct {
    pub hour: i8,
    pub min: i8,
    pub sec: i8,
    pub micros: i32,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_time_ns {
    pub nanos: i64,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_time_tz {
    pub bits: u64,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_time_tz_struct {
    pub time: duckdb_time_struct,
    pub offset: i32,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_timestamp {
    pub micros: i64,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_timestamp_struct {
    pub date: duckdb_date_struct,
    pub time: duckdb_time_struct,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_timestamp_s {
    pub seconds: i64,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_timestamp_ms {
    pub millis: i64,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_timestamp_ns {
    pub nanos: i64,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_hugeint {
    pub lower: u64,
    pub upper: i64,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_uhugeint {
    pub lower: u64,
    pub upper: u64,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_decimal {
    pub width: u8,
    pub scale: u8,
    pub value: duckdb_hugeint,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_query_progress_type {
    pub percentage: f64,
    pub rows_processed: u64,
    pub total_rows_to_process: u64,
}
#[repr(C)]
#[derive(Copy, Clone)]
pub struct duckdb_string_t {
    pub value: duckdb_string_t__bindgen_ty_1,
}
#[repr(C)]
#[derive(Copy, Clone)]
pub union duckdb_string_t__bindgen_ty_1 {
    pub pointer: duckdb_string_t__bindgen_ty_1__bindgen_ty_1,
    pub inlined: duckdb_string_t__bindgen_ty_1__bindgen_ty_2,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_string_t__bindgen_ty_1__bindgen_ty_1 {
    pub length: u32,
    pub prefix: [::std::os::raw::c_char; 4usize],
    pub ptr: *mut ::std::os::raw::c_char,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_string_t__bindgen_ty_1__bindgen_ty_2 {
    pub length: u32,
    pub inlined: [::std::os::raw::c_char; 12usize],
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_list_entry {
    pub offset: u64,
    pub length: u64,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_column {
    pub deprecated_data: *mut ::std::os::raw::c_void,
    pub deprecated_nullmask: *mut bool,
    pub deprecated_type: duckdb_type,
    pub deprecated_name: *mut ::std::os::raw::c_char,
    pub internal_data: *mut ::std::os::raw::c_void,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_vector {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_vector = *mut _duckdb_vector;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_selection_vector {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_selection_vector = *mut _duckdb_selection_vector;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_string {
    pub data: *mut ::std::os::raw::c_char,
    pub size: idx_t,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_blob {
    pub data: *mut ::std::os::raw::c_void,
    pub size: idx_t,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_bit {
    pub data: *mut u8,
    pub size: idx_t,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_bignum {
    pub data: *mut u8,
    pub size: idx_t,
    pub is_negative: bool,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_result {
    pub deprecated_column_count: idx_t,
    pub deprecated_row_count: idx_t,
    pub deprecated_rows_changed: idx_t,
    pub deprecated_columns: *mut duckdb_column,
    pub deprecated_error_message: *mut ::std::os::raw::c_char,
    pub internal_data: *mut ::std::os::raw::c_void,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_instance_cache {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_instance_cache = *mut _duckdb_instance_cache;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_database {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_database = *mut _duckdb_database;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_connection {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_connection = *mut _duckdb_connection;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_client_context {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_client_context = *mut _duckdb_client_context;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_prepared_statement {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_prepared_statement = *mut _duckdb_prepared_statement;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_extracted_statements {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_extracted_statements = *mut _duckdb_extracted_statements;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_pending_result {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_pending_result = *mut _duckdb_pending_result;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_appender {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_appender = *mut _duckdb_appender;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_table_description {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_table_description = *mut _duckdb_table_description;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_config {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_config = *mut _duckdb_config;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_config_option {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_config_option = *mut _duckdb_config_option;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_logical_type {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_logical_type = *mut _duckdb_logical_type;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_create_type_info {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_create_type_info = *mut _duckdb_create_type_info;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_data_chunk {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_data_chunk = *mut _duckdb_data_chunk;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_value {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_value = *mut _duckdb_value;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_profiling_info {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_profiling_info = *mut _duckdb_profiling_info;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_error_data {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_error_data = *mut _duckdb_error_data;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_expression {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_expression = *mut _duckdb_expression;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_extension_info {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_extension_info = *mut _duckdb_extension_info;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_function_info {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_function_info = *mut _duckdb_function_info;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_bind_info {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_bind_info = *mut _duckdb_bind_info;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_init_info {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_init_info = *mut _duckdb_init_info;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_scalar_function {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_scalar_function = *mut _duckdb_scalar_function;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_scalar_function_set {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_scalar_function_set = *mut _duckdb_scalar_function_set;
pub type duckdb_scalar_function_bind_t =
    ::std::option::Option<unsafe extern "C" fn(info: duckdb_bind_info)>;
pub type duckdb_scalar_function_init_t =
    ::std::option::Option<unsafe extern "C" fn(info: duckdb_init_info)>;
pub type duckdb_scalar_function_t = ::std::option::Option<
    unsafe extern "C" fn(
        info: duckdb_function_info,
        input: duckdb_data_chunk,
        output: duckdb_vector,
    ),
>;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_aggregate_function {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_aggregate_function = *mut _duckdb_aggregate_function;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_aggregate_function_set {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_aggregate_function_set = *mut _duckdb_aggregate_function_set;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_aggregate_state {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_aggregate_state = *mut _duckdb_aggregate_state;
pub type duckdb_aggregate_state_size =
    ::std::option::Option<unsafe extern "C" fn(info: duckdb_function_info) -> idx_t>;
pub type duckdb_aggregate_init_t = ::std::option::Option<
    unsafe extern "C" fn(info: duckdb_function_info, state: duckdb_aggregate_state),
>;
pub type duckdb_aggregate_destroy_t =
    ::std::option::Option<unsafe extern "C" fn(states: *mut duckdb_aggregate_state, count: idx_t)>;
pub type duckdb_aggregate_update_t = ::std::option::Option<
    unsafe extern "C" fn(
        info: duckdb_function_info,
        input: duckdb_data_chunk,
        states: *mut duckdb_aggregate_state,
    ),
>;
pub type duckdb_aggregate_combine_t = ::std::option::Option<
    unsafe extern "C" fn(
        info: duckdb_function_info,
        source: *mut duckdb_aggregate_state,
        target: *mut duckdb_aggregate_state,
        count: idx_t,
    ),
>;
pub type duckdb_aggregate_finalize_t = ::std::option::Option<
    unsafe extern "C" fn(
        info: duckdb_function_info,
        source: *mut duckdb_aggregate_state,
        result: duckdb_vector,
        count: idx_t,
        offset: idx_t,
    ),
>;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_table_function {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_table_function = *mut _duckdb_table_function;
pub type duckdb_table_function_bind_t =
    ::std::option::Option<unsafe extern "C" fn(info: duckdb_bind_info)>;
pub type duckdb_table_function_init_t =
    ::std::option::Option<unsafe extern "C" fn(info: duckdb_init_info)>;
pub type duckdb_table_function_t = ::std::option::Option<
    unsafe extern "C" fn(info: duckdb_function_info, output: duckdb_data_chunk),
>;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_copy_function {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_copy_function = *mut _duckdb_copy_function;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_copy_function_bind_info {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_copy_function_bind_info = *mut _duckdb_copy_function_bind_info;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_copy_function_global_init_info {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_copy_function_global_init_info = *mut _duckdb_copy_function_global_init_info;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_copy_function_sink_info {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_copy_function_sink_info = *mut _duckdb_copy_function_sink_info;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_copy_function_finalize_info {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_copy_function_finalize_info = *mut _duckdb_copy_function_finalize_info;
pub type duckdb_copy_function_bind_t =
    ::std::option::Option<unsafe extern "C" fn(info: duckdb_copy_function_bind_info)>;
pub type duckdb_copy_function_global_init_t =
    ::std::option::Option<unsafe extern "C" fn(info: duckdb_copy_function_global_init_info)>;
pub type duckdb_copy_function_sink_t = ::std::option::Option<
    unsafe extern "C" fn(info: duckdb_copy_function_sink_info, input: duckdb_data_chunk),
>;
pub type duckdb_copy_function_finalize_t =
    ::std::option::Option<unsafe extern "C" fn(info: duckdb_copy_function_finalize_info)>;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_cast_function {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_cast_function = *mut _duckdb_cast_function;
pub type duckdb_cast_function_t = ::std::option::Option<
    unsafe extern "C" fn(
        info: duckdb_function_info,
        count: idx_t,
        input: duckdb_vector,
        output: duckdb_vector,
    ) -> bool,
>;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_replacement_scan_info {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_replacement_scan_info = *mut _duckdb_replacement_scan_info;
pub type duckdb_replacement_callback_t = ::std::option::Option<
    unsafe extern "C" fn(
        info: duckdb_replacement_scan_info,
        table_name: *const ::std::os::raw::c_char,
        data: *mut ::std::os::raw::c_void,
    ),
>;
#[repr(C)]
#[derive(Debug)]
pub struct ArrowArray {
    _unused: [u8; 0],
}
#[repr(C)]
#[derive(Debug)]
pub struct ArrowSchema {
    _unused: [u8; 0],
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_arrow {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_arrow = *mut _duckdb_arrow;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_arrow_stream {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_arrow_stream = *mut _duckdb_arrow_stream;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_arrow_schema {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_arrow_schema = *mut _duckdb_arrow_schema;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_arrow_converted_schema {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_arrow_converted_schema = *mut _duckdb_arrow_converted_schema;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_arrow_array {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_arrow_array = *mut _duckdb_arrow_array;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_arrow_options {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_arrow_options = *mut _duckdb_arrow_options;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_file_open_options {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_file_open_options = *mut _duckdb_file_open_options;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_file_system {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_file_system = *mut _duckdb_file_system;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_file_handle {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_file_handle = *mut _duckdb_file_handle;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_catalog {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_catalog = *mut _duckdb_catalog;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_catalog_entry {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_catalog_entry = *mut _duckdb_catalog_entry;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct _duckdb_log_storage {
    pub internal_ptr: *mut ::std::os::raw::c_void,
}
pub type duckdb_log_storage = *mut _duckdb_log_storage;
pub type duckdb_logger_write_log_entry_t = ::std::option::Option<
    unsafe extern "C" fn(
        extra_data: *mut ::std::os::raw::c_void,
        timestamp: *mut duckdb_timestamp,
        level: *const ::std::os::raw::c_char,
        log_type: *const ::std::os::raw::c_char,
        log_message: *const ::std::os::raw::c_char,
    ),
>;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_extension_access {
    pub set_error: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_extension_info, error: *const ::std::os::raw::c_char),
    >,
    pub get_database: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_extension_info) -> *mut duckdb_database,
    >,
    pub get_api: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_extension_info,
            version: *const ::std::os::raw::c_char,
        ) -> *const ::std::os::raw::c_void,
    >,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct duckdb_ext_api_v1 {
    pub duckdb_open: ::std::option::Option<
        unsafe extern "C" fn(
            path: *const ::std::os::raw::c_char,
            out_database: *mut duckdb_database,
        ) -> duckdb_state,
    >,
    pub duckdb_open_ext: ::std::option::Option<
        unsafe extern "C" fn(
            path: *const ::std::os::raw::c_char,
            out_database: *mut duckdb_database,
            config: duckdb_config,
            out_error: *mut *mut ::std::os::raw::c_char,
        ) -> duckdb_state,
    >,
    pub duckdb_close: ::std::option::Option<unsafe extern "C" fn(database: *mut duckdb_database)>,
    pub duckdb_connect: ::std::option::Option<
        unsafe extern "C" fn(
            database: duckdb_database,
            out_connection: *mut duckdb_connection,
        ) -> duckdb_state,
    >,
    pub duckdb_interrupt:
        ::std::option::Option<unsafe extern "C" fn(connection: duckdb_connection)>,
    pub duckdb_query_progress: ::std::option::Option<
        unsafe extern "C" fn(connection: duckdb_connection) -> duckdb_query_progress_type,
    >,
    pub duckdb_disconnect:
        ::std::option::Option<unsafe extern "C" fn(connection: *mut duckdb_connection)>,
    pub duckdb_library_version:
        ::std::option::Option<unsafe extern "C" fn() -> *const ::std::os::raw::c_char>,
    pub duckdb_create_config:
        ::std::option::Option<unsafe extern "C" fn(out_config: *mut duckdb_config) -> duckdb_state>,
    pub duckdb_config_count: ::std::option::Option<unsafe extern "C" fn() -> usize>,
    pub duckdb_get_config_flag: ::std::option::Option<
        unsafe extern "C" fn(
            index: usize,
            out_name: *mut *const ::std::os::raw::c_char,
            out_description: *mut *const ::std::os::raw::c_char,
        ) -> duckdb_state,
    >,
    pub duckdb_set_config: ::std::option::Option<
        unsafe extern "C" fn(
            config: duckdb_config,
            name: *const ::std::os::raw::c_char,
            option: *const ::std::os::raw::c_char,
        ) -> duckdb_state,
    >,
    pub duckdb_destroy_config:
        ::std::option::Option<unsafe extern "C" fn(config: *mut duckdb_config)>,
    pub duckdb_query: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            query: *const ::std::os::raw::c_char,
            out_result: *mut duckdb_result,
        ) -> duckdb_state,
    >,
    pub duckdb_destroy_result:
        ::std::option::Option<unsafe extern "C" fn(result: *mut duckdb_result)>,
    pub duckdb_column_name: ::std::option::Option<
        unsafe extern "C" fn(
            result: *mut duckdb_result,
            col: idx_t,
        ) -> *const ::std::os::raw::c_char,
    >,
    pub duckdb_column_type: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t) -> duckdb_type,
    >,
    pub duckdb_result_statement_type:
        ::std::option::Option<unsafe extern "C" fn(result: duckdb_result) -> duckdb_statement_type>,
    pub duckdb_column_logical_type: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t) -> duckdb_logical_type,
    >,
    pub duckdb_column_count:
        ::std::option::Option<unsafe extern "C" fn(result: *mut duckdb_result) -> idx_t>,
    pub duckdb_rows_changed:
        ::std::option::Option<unsafe extern "C" fn(result: *mut duckdb_result) -> idx_t>,
    pub duckdb_result_error: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result) -> *const ::std::os::raw::c_char,
    >,
    pub duckdb_result_error_type: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result) -> duckdb_error_type,
    >,
    pub duckdb_result_return_type:
        ::std::option::Option<unsafe extern "C" fn(result: duckdb_result) -> duckdb_result_type>,
    pub duckdb_malloc:
        ::std::option::Option<unsafe extern "C" fn(size: usize) -> *mut ::std::os::raw::c_void>,
    pub duckdb_free: ::std::option::Option<unsafe extern "C" fn(ptr: *mut ::std::os::raw::c_void)>,
    pub duckdb_vector_size: ::std::option::Option<unsafe extern "C" fn() -> idx_t>,
    pub duckdb_string_is_inlined:
        ::std::option::Option<unsafe extern "C" fn(string: duckdb_string_t) -> bool>,
    pub duckdb_string_t_length:
        ::std::option::Option<unsafe extern "C" fn(string: duckdb_string_t) -> u32>,
    pub duckdb_string_t_data: ::std::option::Option<
        unsafe extern "C" fn(string: *mut duckdb_string_t) -> *const ::std::os::raw::c_char,
    >,
    pub duckdb_from_date:
        ::std::option::Option<unsafe extern "C" fn(date: duckdb_date) -> duckdb_date_struct>,
    pub duckdb_to_date:
        ::std::option::Option<unsafe extern "C" fn(date: duckdb_date_struct) -> duckdb_date>,
    pub duckdb_is_finite_date:
        ::std::option::Option<unsafe extern "C" fn(date: duckdb_date) -> bool>,
    pub duckdb_from_time:
        ::std::option::Option<unsafe extern "C" fn(time: duckdb_time) -> duckdb_time_struct>,
    pub duckdb_create_time_tz:
        ::std::option::Option<unsafe extern "C" fn(micros: i64, offset: i32) -> duckdb_time_tz>,
    pub duckdb_from_time_tz: ::std::option::Option<
        unsafe extern "C" fn(micros: duckdb_time_tz) -> duckdb_time_tz_struct,
    >,
    pub duckdb_to_time:
        ::std::option::Option<unsafe extern "C" fn(time: duckdb_time_struct) -> duckdb_time>,
    pub duckdb_from_timestamp: ::std::option::Option<
        unsafe extern "C" fn(ts: duckdb_timestamp) -> duckdb_timestamp_struct,
    >,
    pub duckdb_to_timestamp: ::std::option::Option<
        unsafe extern "C" fn(ts: duckdb_timestamp_struct) -> duckdb_timestamp,
    >,
    pub duckdb_is_finite_timestamp:
        ::std::option::Option<unsafe extern "C" fn(ts: duckdb_timestamp) -> bool>,
    pub duckdb_hugeint_to_double:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_hugeint) -> f64>,
    pub duckdb_double_to_hugeint:
        ::std::option::Option<unsafe extern "C" fn(val: f64) -> duckdb_hugeint>,
    pub duckdb_uhugeint_to_double:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_uhugeint) -> f64>,
    pub duckdb_double_to_uhugeint:
        ::std::option::Option<unsafe extern "C" fn(val: f64) -> duckdb_uhugeint>,
    pub duckdb_double_to_decimal: ::std::option::Option<
        unsafe extern "C" fn(val: f64, width: u8, scale: u8) -> duckdb_decimal,
    >,
    pub duckdb_decimal_to_double:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_decimal) -> f64>,
    pub duckdb_prepare: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            query: *const ::std::os::raw::c_char,
            out_prepared_statement: *mut duckdb_prepared_statement,
        ) -> duckdb_state,
    >,
    pub duckdb_destroy_prepare: ::std::option::Option<
        unsafe extern "C" fn(prepared_statement: *mut duckdb_prepared_statement),
    >,
    pub duckdb_prepare_error: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
        ) -> *const ::std::os::raw::c_char,
    >,
    pub duckdb_nparams: ::std::option::Option<
        unsafe extern "C" fn(prepared_statement: duckdb_prepared_statement) -> idx_t,
    >,
    pub duckdb_parameter_name: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            index: idx_t,
        ) -> *const ::std::os::raw::c_char,
    >,
    pub duckdb_param_type: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
        ) -> duckdb_type,
    >,
    pub duckdb_param_logical_type: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
        ) -> duckdb_logical_type,
    >,
    pub duckdb_clear_bindings: ::std::option::Option<
        unsafe extern "C" fn(prepared_statement: duckdb_prepared_statement) -> duckdb_state,
    >,
    pub duckdb_prepared_statement_type: ::std::option::Option<
        unsafe extern "C" fn(statement: duckdb_prepared_statement) -> duckdb_statement_type,
    >,
    pub duckdb_bind_value: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: duckdb_value,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_parameter_index: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx_out: *mut idx_t,
            name: *const ::std::os::raw::c_char,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_boolean: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: bool,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_int8: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: i8,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_int16: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: i16,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_int32: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: i32,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_int64: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: i64,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_hugeint: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: duckdb_hugeint,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_uhugeint: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: duckdb_uhugeint,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_decimal: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: duckdb_decimal,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_uint8: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: u8,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_uint16: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: u16,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_uint32: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: u32,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_uint64: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: u64,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_float: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: f32,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_double: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: f64,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_date: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: duckdb_date,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_time: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: duckdb_time,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_timestamp: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: duckdb_timestamp,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_timestamp_tz: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: duckdb_timestamp,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_interval: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: duckdb_interval,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_varchar: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: *const ::std::os::raw::c_char,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_varchar_length: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            val: *const ::std::os::raw::c_char,
            length: idx_t,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_blob: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
            data: *const ::std::os::raw::c_void,
            length: idx_t,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_null: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            param_idx: idx_t,
        ) -> duckdb_state,
    >,
    pub duckdb_execute_prepared: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            out_result: *mut duckdb_result,
        ) -> duckdb_state,
    >,
    pub duckdb_extract_statements: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            query: *const ::std::os::raw::c_char,
            out_extracted_statements: *mut duckdb_extracted_statements,
        ) -> idx_t,
    >,
    pub duckdb_prepare_extracted_statement: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            extracted_statements: duckdb_extracted_statements,
            index: idx_t,
            out_prepared_statement: *mut duckdb_prepared_statement,
        ) -> duckdb_state,
    >,
    pub duckdb_extract_statements_error: ::std::option::Option<
        unsafe extern "C" fn(
            extracted_statements: duckdb_extracted_statements,
        ) -> *const ::std::os::raw::c_char,
    >,
    pub duckdb_destroy_extracted: ::std::option::Option<
        unsafe extern "C" fn(extracted_statements: *mut duckdb_extracted_statements),
    >,
    pub duckdb_pending_prepared: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            out_result: *mut duckdb_pending_result,
        ) -> duckdb_state,
    >,
    pub duckdb_destroy_pending:
        ::std::option::Option<unsafe extern "C" fn(pending_result: *mut duckdb_pending_result)>,
    pub duckdb_pending_error: ::std::option::Option<
        unsafe extern "C" fn(
            pending_result: duckdb_pending_result,
        ) -> *const ::std::os::raw::c_char,
    >,
    pub duckdb_pending_execute_task: ::std::option::Option<
        unsafe extern "C" fn(pending_result: duckdb_pending_result) -> duckdb_pending_state,
    >,
    pub duckdb_pending_execute_check_state: ::std::option::Option<
        unsafe extern "C" fn(pending_result: duckdb_pending_result) -> duckdb_pending_state,
    >,
    pub duckdb_execute_pending: ::std::option::Option<
        unsafe extern "C" fn(
            pending_result: duckdb_pending_result,
            out_result: *mut duckdb_result,
        ) -> duckdb_state,
    >,
    pub duckdb_pending_execution_is_finished:
        ::std::option::Option<unsafe extern "C" fn(pending_state: duckdb_pending_state) -> bool>,
    pub duckdb_destroy_value: ::std::option::Option<unsafe extern "C" fn(value: *mut duckdb_value)>,
    pub duckdb_create_varchar: ::std::option::Option<
        unsafe extern "C" fn(text: *const ::std::os::raw::c_char) -> duckdb_value,
    >,
    pub duckdb_create_varchar_length: ::std::option::Option<
        unsafe extern "C" fn(text: *const ::std::os::raw::c_char, length: idx_t) -> duckdb_value,
    >,
    pub duckdb_create_bool:
        ::std::option::Option<unsafe extern "C" fn(input: bool) -> duckdb_value>,
    pub duckdb_create_int8: ::std::option::Option<unsafe extern "C" fn(input: i8) -> duckdb_value>,
    pub duckdb_create_uint8: ::std::option::Option<unsafe extern "C" fn(input: u8) -> duckdb_value>,
    pub duckdb_create_int16:
        ::std::option::Option<unsafe extern "C" fn(input: i16) -> duckdb_value>,
    pub duckdb_create_uint16:
        ::std::option::Option<unsafe extern "C" fn(input: u16) -> duckdb_value>,
    pub duckdb_create_int32:
        ::std::option::Option<unsafe extern "C" fn(input: i32) -> duckdb_value>,
    pub duckdb_create_uint32:
        ::std::option::Option<unsafe extern "C" fn(input: u32) -> duckdb_value>,
    pub duckdb_create_uint64:
        ::std::option::Option<unsafe extern "C" fn(input: u64) -> duckdb_value>,
    pub duckdb_create_int64: ::std::option::Option<unsafe extern "C" fn(val: i64) -> duckdb_value>,
    pub duckdb_create_hugeint:
        ::std::option::Option<unsafe extern "C" fn(input: duckdb_hugeint) -> duckdb_value>,
    pub duckdb_create_uhugeint:
        ::std::option::Option<unsafe extern "C" fn(input: duckdb_uhugeint) -> duckdb_value>,
    pub duckdb_create_float:
        ::std::option::Option<unsafe extern "C" fn(input: f32) -> duckdb_value>,
    pub duckdb_create_double:
        ::std::option::Option<unsafe extern "C" fn(input: f64) -> duckdb_value>,
    pub duckdb_create_date:
        ::std::option::Option<unsafe extern "C" fn(input: duckdb_date) -> duckdb_value>,
    pub duckdb_create_time:
        ::std::option::Option<unsafe extern "C" fn(input: duckdb_time) -> duckdb_value>,
    pub duckdb_create_time_tz_value:
        ::std::option::Option<unsafe extern "C" fn(value: duckdb_time_tz) -> duckdb_value>,
    pub duckdb_create_timestamp:
        ::std::option::Option<unsafe extern "C" fn(input: duckdb_timestamp) -> duckdb_value>,
    pub duckdb_create_interval:
        ::std::option::Option<unsafe extern "C" fn(input: duckdb_interval) -> duckdb_value>,
    pub duckdb_create_blob:
        ::std::option::Option<unsafe extern "C" fn(data: *const u8, length: idx_t) -> duckdb_value>,
    pub duckdb_create_bignum:
        ::std::option::Option<unsafe extern "C" fn(input: duckdb_bignum) -> duckdb_value>,
    pub duckdb_create_decimal:
        ::std::option::Option<unsafe extern "C" fn(input: duckdb_decimal) -> duckdb_value>,
    pub duckdb_create_bit:
        ::std::option::Option<unsafe extern "C" fn(input: duckdb_bit) -> duckdb_value>,
    pub duckdb_create_uuid:
        ::std::option::Option<unsafe extern "C" fn(input: duckdb_uhugeint) -> duckdb_value>,
    pub duckdb_get_bool: ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> bool>,
    pub duckdb_get_int8: ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> i8>,
    pub duckdb_get_uint8: ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> u8>,
    pub duckdb_get_int16: ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> i16>,
    pub duckdb_get_uint16: ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> u16>,
    pub duckdb_get_int32: ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> i32>,
    pub duckdb_get_uint32: ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> u32>,
    pub duckdb_get_int64: ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> i64>,
    pub duckdb_get_uint64: ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> u64>,
    pub duckdb_get_hugeint:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_hugeint>,
    pub duckdb_get_uhugeint:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_uhugeint>,
    pub duckdb_get_float: ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> f32>,
    pub duckdb_get_double: ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> f64>,
    pub duckdb_get_date:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_date>,
    pub duckdb_get_time:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_time>,
    pub duckdb_get_time_tz:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_time_tz>,
    pub duckdb_get_timestamp:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_timestamp>,
    pub duckdb_get_interval:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_interval>,
    pub duckdb_get_value_type:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_logical_type>,
    pub duckdb_get_blob:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_blob>,
    pub duckdb_get_bignum:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_bignum>,
    pub duckdb_get_decimal:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_decimal>,
    pub duckdb_get_bit:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_bit>,
    pub duckdb_get_uuid:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_uhugeint>,
    pub duckdb_get_varchar: ::std::option::Option<
        unsafe extern "C" fn(value: duckdb_value) -> *mut ::std::os::raw::c_char,
    >,
    pub duckdb_create_struct_value: ::std::option::Option<
        unsafe extern "C" fn(type_: duckdb_logical_type, values: *mut duckdb_value) -> duckdb_value,
    >,
    pub duckdb_create_list_value: ::std::option::Option<
        unsafe extern "C" fn(
            type_: duckdb_logical_type,
            values: *mut duckdb_value,
            value_count: idx_t,
        ) -> duckdb_value,
    >,
    pub duckdb_create_array_value: ::std::option::Option<
        unsafe extern "C" fn(
            type_: duckdb_logical_type,
            values: *mut duckdb_value,
            value_count: idx_t,
        ) -> duckdb_value,
    >,
    pub duckdb_get_map_size:
        ::std::option::Option<unsafe extern "C" fn(value: duckdb_value) -> idx_t>,
    pub duckdb_get_map_key: ::std::option::Option<
        unsafe extern "C" fn(value: duckdb_value, index: idx_t) -> duckdb_value,
    >,
    pub duckdb_get_map_value: ::std::option::Option<
        unsafe extern "C" fn(value: duckdb_value, index: idx_t) -> duckdb_value,
    >,
    pub duckdb_is_null_value:
        ::std::option::Option<unsafe extern "C" fn(value: duckdb_value) -> bool>,
    pub duckdb_create_null_value: ::std::option::Option<unsafe extern "C" fn() -> duckdb_value>,
    pub duckdb_get_list_size:
        ::std::option::Option<unsafe extern "C" fn(value: duckdb_value) -> idx_t>,
    pub duckdb_get_list_child: ::std::option::Option<
        unsafe extern "C" fn(value: duckdb_value, index: idx_t) -> duckdb_value,
    >,
    pub duckdb_create_enum_value: ::std::option::Option<
        unsafe extern "C" fn(type_: duckdb_logical_type, value: u64) -> duckdb_value,
    >,
    pub duckdb_get_enum_value:
        ::std::option::Option<unsafe extern "C" fn(value: duckdb_value) -> u64>,
    pub duckdb_get_struct_child: ::std::option::Option<
        unsafe extern "C" fn(value: duckdb_value, index: idx_t) -> duckdb_value,
    >,
    pub duckdb_create_logical_type:
        ::std::option::Option<unsafe extern "C" fn(type_: duckdb_type) -> duckdb_logical_type>,
    pub duckdb_logical_type_get_alias: ::std::option::Option<
        unsafe extern "C" fn(type_: duckdb_logical_type) -> *mut ::std::os::raw::c_char,
    >,
    pub duckdb_logical_type_set_alias: ::std::option::Option<
        unsafe extern "C" fn(type_: duckdb_logical_type, alias: *const ::std::os::raw::c_char),
    >,
    pub duckdb_create_list_type: ::std::option::Option<
        unsafe extern "C" fn(type_: duckdb_logical_type) -> duckdb_logical_type,
    >,
    pub duckdb_create_array_type: ::std::option::Option<
        unsafe extern "C" fn(type_: duckdb_logical_type, array_size: idx_t) -> duckdb_logical_type,
    >,
    pub duckdb_create_map_type: ::std::option::Option<
        unsafe extern "C" fn(
            key_type: duckdb_logical_type,
            value_type: duckdb_logical_type,
        ) -> duckdb_logical_type,
    >,
    pub duckdb_create_union_type: ::std::option::Option<
        unsafe extern "C" fn(
            member_types: *mut duckdb_logical_type,
            member_names: *mut *const ::std::os::raw::c_char,
            member_count: idx_t,
        ) -> duckdb_logical_type,
    >,
    pub duckdb_create_struct_type: ::std::option::Option<
        unsafe extern "C" fn(
            member_types: *mut duckdb_logical_type,
            member_names: *mut *const ::std::os::raw::c_char,
            member_count: idx_t,
        ) -> duckdb_logical_type,
    >,
    pub duckdb_create_enum_type: ::std::option::Option<
        unsafe extern "C" fn(
            member_names: *mut *const ::std::os::raw::c_char,
            member_count: idx_t,
        ) -> duckdb_logical_type,
    >,
    pub duckdb_create_decimal_type:
        ::std::option::Option<unsafe extern "C" fn(width: u8, scale: u8) -> duckdb_logical_type>,
    pub duckdb_get_type_id:
        ::std::option::Option<unsafe extern "C" fn(type_: duckdb_logical_type) -> duckdb_type>,
    pub duckdb_decimal_width:
        ::std::option::Option<unsafe extern "C" fn(type_: duckdb_logical_type) -> u8>,
    pub duckdb_decimal_scale:
        ::std::option::Option<unsafe extern "C" fn(type_: duckdb_logical_type) -> u8>,
    pub duckdb_decimal_internal_type:
        ::std::option::Option<unsafe extern "C" fn(type_: duckdb_logical_type) -> duckdb_type>,
    pub duckdb_enum_internal_type:
        ::std::option::Option<unsafe extern "C" fn(type_: duckdb_logical_type) -> duckdb_type>,
    pub duckdb_enum_dictionary_size:
        ::std::option::Option<unsafe extern "C" fn(type_: duckdb_logical_type) -> u32>,
    pub duckdb_enum_dictionary_value: ::std::option::Option<
        unsafe extern "C" fn(
            type_: duckdb_logical_type,
            index: idx_t,
        ) -> *mut ::std::os::raw::c_char,
    >,
    pub duckdb_list_type_child_type: ::std::option::Option<
        unsafe extern "C" fn(type_: duckdb_logical_type) -> duckdb_logical_type,
    >,
    pub duckdb_array_type_child_type: ::std::option::Option<
        unsafe extern "C" fn(type_: duckdb_logical_type) -> duckdb_logical_type,
    >,
    pub duckdb_array_type_array_size:
        ::std::option::Option<unsafe extern "C" fn(type_: duckdb_logical_type) -> idx_t>,
    pub duckdb_map_type_key_type: ::std::option::Option<
        unsafe extern "C" fn(type_: duckdb_logical_type) -> duckdb_logical_type,
    >,
    pub duckdb_map_type_value_type: ::std::option::Option<
        unsafe extern "C" fn(type_: duckdb_logical_type) -> duckdb_logical_type,
    >,
    pub duckdb_struct_type_child_count:
        ::std::option::Option<unsafe extern "C" fn(type_: duckdb_logical_type) -> idx_t>,
    pub duckdb_struct_type_child_name: ::std::option::Option<
        unsafe extern "C" fn(
            type_: duckdb_logical_type,
            index: idx_t,
        ) -> *mut ::std::os::raw::c_char,
    >,
    pub duckdb_struct_type_child_type: ::std::option::Option<
        unsafe extern "C" fn(type_: duckdb_logical_type, index: idx_t) -> duckdb_logical_type,
    >,
    pub duckdb_union_type_member_count:
        ::std::option::Option<unsafe extern "C" fn(type_: duckdb_logical_type) -> idx_t>,
    pub duckdb_union_type_member_name: ::std::option::Option<
        unsafe extern "C" fn(
            type_: duckdb_logical_type,
            index: idx_t,
        ) -> *mut ::std::os::raw::c_char,
    >,
    pub duckdb_union_type_member_type: ::std::option::Option<
        unsafe extern "C" fn(type_: duckdb_logical_type, index: idx_t) -> duckdb_logical_type,
    >,
    pub duckdb_destroy_logical_type:
        ::std::option::Option<unsafe extern "C" fn(type_: *mut duckdb_logical_type)>,
    pub duckdb_register_logical_type: ::std::option::Option<
        unsafe extern "C" fn(
            con: duckdb_connection,
            type_: duckdb_logical_type,
            info: duckdb_create_type_info,
        ) -> duckdb_state,
    >,
    pub duckdb_create_data_chunk: ::std::option::Option<
        unsafe extern "C" fn(
            types: *mut duckdb_logical_type,
            column_count: idx_t,
        ) -> duckdb_data_chunk,
    >,
    pub duckdb_destroy_data_chunk:
        ::std::option::Option<unsafe extern "C" fn(chunk: *mut duckdb_data_chunk)>,
    pub duckdb_data_chunk_reset:
        ::std::option::Option<unsafe extern "C" fn(chunk: duckdb_data_chunk)>,
    pub duckdb_data_chunk_get_column_count:
        ::std::option::Option<unsafe extern "C" fn(chunk: duckdb_data_chunk) -> idx_t>,
    pub duckdb_data_chunk_get_vector: ::std::option::Option<
        unsafe extern "C" fn(chunk: duckdb_data_chunk, col_idx: idx_t) -> duckdb_vector,
    >,
    pub duckdb_data_chunk_get_size:
        ::std::option::Option<unsafe extern "C" fn(chunk: duckdb_data_chunk) -> idx_t>,
    pub duckdb_data_chunk_set_size:
        ::std::option::Option<unsafe extern "C" fn(chunk: duckdb_data_chunk, size: idx_t)>,
    pub duckdb_vector_get_column_type:
        ::std::option::Option<unsafe extern "C" fn(vector: duckdb_vector) -> duckdb_logical_type>,
    pub duckdb_vector_get_data: ::std::option::Option<
        unsafe extern "C" fn(vector: duckdb_vector) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_vector_get_validity:
        ::std::option::Option<unsafe extern "C" fn(vector: duckdb_vector) -> *mut u64>,
    pub duckdb_vector_ensure_validity_writable:
        ::std::option::Option<unsafe extern "C" fn(vector: duckdb_vector)>,
    pub duckdb_vector_assign_string_element: ::std::option::Option<
        unsafe extern "C" fn(
            vector: duckdb_vector,
            index: idx_t,
            str_: *const ::std::os::raw::c_char,
        ),
    >,
    pub duckdb_vector_assign_string_element_len: ::std::option::Option<
        unsafe extern "C" fn(
            vector: duckdb_vector,
            index: idx_t,
            str_: *const ::std::os::raw::c_char,
            str_len: idx_t,
        ),
    >,
    pub duckdb_list_vector_get_child:
        ::std::option::Option<unsafe extern "C" fn(vector: duckdb_vector) -> duckdb_vector>,
    pub duckdb_list_vector_get_size:
        ::std::option::Option<unsafe extern "C" fn(vector: duckdb_vector) -> idx_t>,
    pub duckdb_list_vector_set_size: ::std::option::Option<
        unsafe extern "C" fn(vector: duckdb_vector, size: idx_t) -> duckdb_state,
    >,
    pub duckdb_list_vector_reserve: ::std::option::Option<
        unsafe extern "C" fn(vector: duckdb_vector, required_capacity: idx_t) -> duckdb_state,
    >,
    pub duckdb_struct_vector_get_child: ::std::option::Option<
        unsafe extern "C" fn(vector: duckdb_vector, index: idx_t) -> duckdb_vector,
    >,
    pub duckdb_array_vector_get_child:
        ::std::option::Option<unsafe extern "C" fn(vector: duckdb_vector) -> duckdb_vector>,
    pub duckdb_validity_row_is_valid:
        ::std::option::Option<unsafe extern "C" fn(validity: *mut u64, row: idx_t) -> bool>,
    pub duckdb_validity_set_row_validity:
        ::std::option::Option<unsafe extern "C" fn(validity: *mut u64, row: idx_t, valid: bool)>,
    pub duckdb_validity_set_row_invalid:
        ::std::option::Option<unsafe extern "C" fn(validity: *mut u64, row: idx_t)>,
    pub duckdb_validity_set_row_valid:
        ::std::option::Option<unsafe extern "C" fn(validity: *mut u64, row: idx_t)>,
    pub duckdb_create_scalar_function:
        ::std::option::Option<unsafe extern "C" fn() -> duckdb_scalar_function>,
    pub duckdb_destroy_scalar_function:
        ::std::option::Option<unsafe extern "C" fn(scalar_function: *mut duckdb_scalar_function)>,
    pub duckdb_scalar_function_set_name: ::std::option::Option<
        unsafe extern "C" fn(
            scalar_function: duckdb_scalar_function,
            name: *const ::std::os::raw::c_char,
        ),
    >,
    pub duckdb_scalar_function_set_varargs: ::std::option::Option<
        unsafe extern "C" fn(scalar_function: duckdb_scalar_function, type_: duckdb_logical_type),
    >,
    pub duckdb_scalar_function_set_special_handling:
        ::std::option::Option<unsafe extern "C" fn(scalar_function: duckdb_scalar_function)>,
    pub duckdb_scalar_function_set_volatile:
        ::std::option::Option<unsafe extern "C" fn(scalar_function: duckdb_scalar_function)>,
    pub duckdb_scalar_function_add_parameter: ::std::option::Option<
        unsafe extern "C" fn(scalar_function: duckdb_scalar_function, type_: duckdb_logical_type),
    >,
    pub duckdb_scalar_function_set_return_type: ::std::option::Option<
        unsafe extern "C" fn(scalar_function: duckdb_scalar_function, type_: duckdb_logical_type),
    >,
    pub duckdb_scalar_function_set_extra_info: ::std::option::Option<
        unsafe extern "C" fn(
            scalar_function: duckdb_scalar_function,
            extra_info: *mut ::std::os::raw::c_void,
            destroy: duckdb_delete_callback_t,
        ),
    >,
    pub duckdb_scalar_function_set_function: ::std::option::Option<
        unsafe extern "C" fn(
            scalar_function: duckdb_scalar_function,
            function: duckdb_scalar_function_t,
        ),
    >,
    pub duckdb_register_scalar_function: ::std::option::Option<
        unsafe extern "C" fn(
            con: duckdb_connection,
            scalar_function: duckdb_scalar_function,
        ) -> duckdb_state,
    >,
    pub duckdb_scalar_function_get_extra_info: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_function_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_scalar_function_set_error: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_function_info, error: *const ::std::os::raw::c_char),
    >,
    pub duckdb_create_scalar_function_set: ::std::option::Option<
        unsafe extern "C" fn(name: *const ::std::os::raw::c_char) -> duckdb_scalar_function_set,
    >,
    pub duckdb_destroy_scalar_function_set: ::std::option::Option<
        unsafe extern "C" fn(scalar_function_set: *mut duckdb_scalar_function_set),
    >,
    pub duckdb_add_scalar_function_to_set: ::std::option::Option<
        unsafe extern "C" fn(
            set: duckdb_scalar_function_set,
            function: duckdb_scalar_function,
        ) -> duckdb_state,
    >,
    pub duckdb_register_scalar_function_set: ::std::option::Option<
        unsafe extern "C" fn(
            con: duckdb_connection,
            set: duckdb_scalar_function_set,
        ) -> duckdb_state,
    >,
    pub duckdb_create_aggregate_function:
        ::std::option::Option<unsafe extern "C" fn() -> duckdb_aggregate_function>,
    pub duckdb_destroy_aggregate_function: ::std::option::Option<
        unsafe extern "C" fn(aggregate_function: *mut duckdb_aggregate_function),
    >,
    pub duckdb_aggregate_function_set_name: ::std::option::Option<
        unsafe extern "C" fn(
            aggregate_function: duckdb_aggregate_function,
            name: *const ::std::os::raw::c_char,
        ),
    >,
    pub duckdb_aggregate_function_add_parameter: ::std::option::Option<
        unsafe extern "C" fn(
            aggregate_function: duckdb_aggregate_function,
            type_: duckdb_logical_type,
        ),
    >,
    pub duckdb_aggregate_function_set_return_type: ::std::option::Option<
        unsafe extern "C" fn(
            aggregate_function: duckdb_aggregate_function,
            type_: duckdb_logical_type,
        ),
    >,
    pub duckdb_aggregate_function_set_functions: ::std::option::Option<
        unsafe extern "C" fn(
            aggregate_function: duckdb_aggregate_function,
            state_size: duckdb_aggregate_state_size,
            state_init: duckdb_aggregate_init_t,
            update: duckdb_aggregate_update_t,
            combine: duckdb_aggregate_combine_t,
            finalize: duckdb_aggregate_finalize_t,
        ),
    >,
    pub duckdb_aggregate_function_set_destructor: ::std::option::Option<
        unsafe extern "C" fn(
            aggregate_function: duckdb_aggregate_function,
            destroy: duckdb_aggregate_destroy_t,
        ),
    >,
    pub duckdb_register_aggregate_function: ::std::option::Option<
        unsafe extern "C" fn(
            con: duckdb_connection,
            aggregate_function: duckdb_aggregate_function,
        ) -> duckdb_state,
    >,
    pub duckdb_aggregate_function_set_special_handling:
        ::std::option::Option<unsafe extern "C" fn(aggregate_function: duckdb_aggregate_function)>,
    pub duckdb_aggregate_function_set_extra_info: ::std::option::Option<
        unsafe extern "C" fn(
            aggregate_function: duckdb_aggregate_function,
            extra_info: *mut ::std::os::raw::c_void,
            destroy: duckdb_delete_callback_t,
        ),
    >,
    pub duckdb_aggregate_function_get_extra_info: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_function_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_aggregate_function_set_error: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_function_info, error: *const ::std::os::raw::c_char),
    >,
    pub duckdb_create_aggregate_function_set: ::std::option::Option<
        unsafe extern "C" fn(name: *const ::std::os::raw::c_char) -> duckdb_aggregate_function_set,
    >,
    pub duckdb_destroy_aggregate_function_set: ::std::option::Option<
        unsafe extern "C" fn(aggregate_function_set: *mut duckdb_aggregate_function_set),
    >,
    pub duckdb_add_aggregate_function_to_set: ::std::option::Option<
        unsafe extern "C" fn(
            set: duckdb_aggregate_function_set,
            function: duckdb_aggregate_function,
        ) -> duckdb_state,
    >,
    pub duckdb_register_aggregate_function_set: ::std::option::Option<
        unsafe extern "C" fn(
            con: duckdb_connection,
            set: duckdb_aggregate_function_set,
        ) -> duckdb_state,
    >,
    pub duckdb_create_table_function:
        ::std::option::Option<unsafe extern "C" fn() -> duckdb_table_function>,
    pub duckdb_destroy_table_function:
        ::std::option::Option<unsafe extern "C" fn(table_function: *mut duckdb_table_function)>,
    pub duckdb_table_function_set_name: ::std::option::Option<
        unsafe extern "C" fn(
            table_function: duckdb_table_function,
            name: *const ::std::os::raw::c_char,
        ),
    >,
    pub duckdb_table_function_add_parameter: ::std::option::Option<
        unsafe extern "C" fn(table_function: duckdb_table_function, type_: duckdb_logical_type),
    >,
    pub duckdb_table_function_add_named_parameter: ::std::option::Option<
        unsafe extern "C" fn(
            table_function: duckdb_table_function,
            name: *const ::std::os::raw::c_char,
            type_: duckdb_logical_type,
        ),
    >,
    pub duckdb_table_function_set_extra_info: ::std::option::Option<
        unsafe extern "C" fn(
            table_function: duckdb_table_function,
            extra_info: *mut ::std::os::raw::c_void,
            destroy: duckdb_delete_callback_t,
        ),
    >,
    pub duckdb_table_function_set_bind: ::std::option::Option<
        unsafe extern "C" fn(
            table_function: duckdb_table_function,
            bind: duckdb_table_function_bind_t,
        ),
    >,
    pub duckdb_table_function_set_init: ::std::option::Option<
        unsafe extern "C" fn(
            table_function: duckdb_table_function,
            init: duckdb_table_function_init_t,
        ),
    >,
    pub duckdb_table_function_set_local_init: ::std::option::Option<
        unsafe extern "C" fn(
            table_function: duckdb_table_function,
            init: duckdb_table_function_init_t,
        ),
    >,
    pub duckdb_table_function_set_function: ::std::option::Option<
        unsafe extern "C" fn(
            table_function: duckdb_table_function,
            function: duckdb_table_function_t,
        ),
    >,
    pub duckdb_table_function_supports_projection_pushdown: ::std::option::Option<
        unsafe extern "C" fn(table_function: duckdb_table_function, pushdown: bool),
    >,
    pub duckdb_register_table_function: ::std::option::Option<
        unsafe extern "C" fn(
            con: duckdb_connection,
            function: duckdb_table_function,
        ) -> duckdb_state,
    >,
    pub duckdb_bind_get_extra_info: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_bind_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_bind_add_result_column: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_bind_info,
            name: *const ::std::os::raw::c_char,
            type_: duckdb_logical_type,
        ),
    >,
    pub duckdb_bind_get_parameter_count:
        ::std::option::Option<unsafe extern "C" fn(info: duckdb_bind_info) -> idx_t>,
    pub duckdb_bind_get_parameter: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_bind_info, index: idx_t) -> duckdb_value,
    >,
    pub duckdb_bind_get_named_parameter: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_bind_info,
            name: *const ::std::os::raw::c_char,
        ) -> duckdb_value,
    >,
    pub duckdb_bind_set_bind_data: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_bind_info,
            bind_data: *mut ::std::os::raw::c_void,
            destroy: duckdb_delete_callback_t,
        ),
    >,
    pub duckdb_bind_set_cardinality: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_bind_info, cardinality: idx_t, is_exact: bool),
    >,
    pub duckdb_bind_set_error: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_bind_info, error: *const ::std::os::raw::c_char),
    >,
    pub duckdb_init_get_extra_info: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_init_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_init_get_bind_data: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_init_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_init_set_init_data: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_init_info,
            init_data: *mut ::std::os::raw::c_void,
            destroy: duckdb_delete_callback_t,
        ),
    >,
    pub duckdb_init_get_column_count:
        ::std::option::Option<unsafe extern "C" fn(info: duckdb_init_info) -> idx_t>,
    pub duckdb_init_get_column_index: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_init_info, column_index: idx_t) -> idx_t,
    >,
    pub duckdb_init_set_max_threads:
        ::std::option::Option<unsafe extern "C" fn(info: duckdb_init_info, max_threads: idx_t)>,
    pub duckdb_init_set_error: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_init_info, error: *const ::std::os::raw::c_char),
    >,
    pub duckdb_function_get_extra_info: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_function_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_function_get_bind_data: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_function_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_function_get_init_data: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_function_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_function_get_local_init_data: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_function_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_function_set_error: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_function_info, error: *const ::std::os::raw::c_char),
    >,
    pub duckdb_add_replacement_scan: ::std::option::Option<
        unsafe extern "C" fn(
            db: duckdb_database,
            replacement: duckdb_replacement_callback_t,
            extra_data: *mut ::std::os::raw::c_void,
            delete_callback: duckdb_delete_callback_t,
        ),
    >,
    pub duckdb_replacement_scan_set_function_name: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_replacement_scan_info,
            function_name: *const ::std::os::raw::c_char,
        ),
    >,
    pub duckdb_replacement_scan_add_parameter: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_replacement_scan_info, parameter: duckdb_value),
    >,
    pub duckdb_replacement_scan_set_error: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_replacement_scan_info,
            error: *const ::std::os::raw::c_char,
        ),
    >,
    pub duckdb_profiling_info_get_metrics:
        ::std::option::Option<unsafe extern "C" fn(info: duckdb_profiling_info) -> duckdb_value>,
    pub duckdb_profiling_info_get_child_count:
        ::std::option::Option<unsafe extern "C" fn(info: duckdb_profiling_info) -> idx_t>,
    pub duckdb_profiling_info_get_child: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_profiling_info, index: idx_t) -> duckdb_profiling_info,
    >,
    pub duckdb_appender_create: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            schema: *const ::std::os::raw::c_char,
            table: *const ::std::os::raw::c_char,
            out_appender: *mut duckdb_appender,
        ) -> duckdb_state,
    >,
    pub duckdb_appender_create_ext: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            catalog: *const ::std::os::raw::c_char,
            schema: *const ::std::os::raw::c_char,
            table: *const ::std::os::raw::c_char,
            out_appender: *mut duckdb_appender,
        ) -> duckdb_state,
    >,
    pub duckdb_appender_column_count:
        ::std::option::Option<unsafe extern "C" fn(appender: duckdb_appender) -> idx_t>,
    pub duckdb_appender_column_type: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, col_idx: idx_t) -> duckdb_logical_type,
    >,
    pub duckdb_appender_error: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender) -> *const ::std::os::raw::c_char,
    >,
    pub duckdb_appender_flush:
        ::std::option::Option<unsafe extern "C" fn(appender: duckdb_appender) -> duckdb_state>,
    pub duckdb_appender_close:
        ::std::option::Option<unsafe extern "C" fn(appender: duckdb_appender) -> duckdb_state>,
    pub duckdb_appender_destroy:
        ::std::option::Option<unsafe extern "C" fn(appender: *mut duckdb_appender) -> duckdb_state>,
    pub duckdb_appender_add_column: ::std::option::Option<
        unsafe extern "C" fn(
            appender: duckdb_appender,
            name: *const ::std::os::raw::c_char,
        ) -> duckdb_state,
    >,
    pub duckdb_appender_clear_columns:
        ::std::option::Option<unsafe extern "C" fn(appender: duckdb_appender) -> duckdb_state>,
    pub duckdb_append_data_chunk: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, chunk: duckdb_data_chunk) -> duckdb_state,
    >,
    pub duckdb_table_description_create: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            schema: *const ::std::os::raw::c_char,
            table: *const ::std::os::raw::c_char,
            out: *mut duckdb_table_description,
        ) -> duckdb_state,
    >,
    pub duckdb_table_description_create_ext: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            catalog: *const ::std::os::raw::c_char,
            schema: *const ::std::os::raw::c_char,
            table: *const ::std::os::raw::c_char,
            out: *mut duckdb_table_description,
        ) -> duckdb_state,
    >,
    pub duckdb_table_description_destroy: ::std::option::Option<
        unsafe extern "C" fn(table_description: *mut duckdb_table_description),
    >,
    pub duckdb_table_description_error: ::std::option::Option<
        unsafe extern "C" fn(
            table_description: duckdb_table_description,
        ) -> *const ::std::os::raw::c_char,
    >,
    pub duckdb_column_has_default: ::std::option::Option<
        unsafe extern "C" fn(
            table_description: duckdb_table_description,
            index: idx_t,
            out: *mut bool,
        ) -> duckdb_state,
    >,
    pub duckdb_table_description_get_column_name: ::std::option::Option<
        unsafe extern "C" fn(
            table_description: duckdb_table_description,
            index: idx_t,
        ) -> *mut ::std::os::raw::c_char,
    >,
    pub duckdb_execute_tasks:
        ::std::option::Option<unsafe extern "C" fn(database: duckdb_database, max_tasks: idx_t)>,
    pub duckdb_create_task_state:
        ::std::option::Option<unsafe extern "C" fn(database: duckdb_database) -> duckdb_task_state>,
    pub duckdb_execute_tasks_state:
        ::std::option::Option<unsafe extern "C" fn(state: duckdb_task_state)>,
    pub duckdb_execute_n_tasks_state: ::std::option::Option<
        unsafe extern "C" fn(state: duckdb_task_state, max_tasks: idx_t) -> idx_t,
    >,
    pub duckdb_finish_execution:
        ::std::option::Option<unsafe extern "C" fn(state: duckdb_task_state)>,
    pub duckdb_task_state_is_finished:
        ::std::option::Option<unsafe extern "C" fn(state: duckdb_task_state) -> bool>,
    pub duckdb_destroy_task_state:
        ::std::option::Option<unsafe extern "C" fn(state: duckdb_task_state)>,
    pub duckdb_execution_is_finished:
        ::std::option::Option<unsafe extern "C" fn(con: duckdb_connection) -> bool>,
    pub duckdb_fetch_chunk:
        ::std::option::Option<unsafe extern "C" fn(result: duckdb_result) -> duckdb_data_chunk>,
    pub duckdb_create_cast_function:
        ::std::option::Option<unsafe extern "C" fn() -> duckdb_cast_function>,
    pub duckdb_cast_function_set_source_type: ::std::option::Option<
        unsafe extern "C" fn(cast_function: duckdb_cast_function, source_type: duckdb_logical_type),
    >,
    pub duckdb_cast_function_set_target_type: ::std::option::Option<
        unsafe extern "C" fn(cast_function: duckdb_cast_function, target_type: duckdb_logical_type),
    >,
    pub duckdb_cast_function_set_implicit_cast_cost:
        ::std::option::Option<unsafe extern "C" fn(cast_function: duckdb_cast_function, cost: i64)>,
    pub duckdb_cast_function_set_function: ::std::option::Option<
        unsafe extern "C" fn(cast_function: duckdb_cast_function, function: duckdb_cast_function_t),
    >,
    pub duckdb_cast_function_set_extra_info: ::std::option::Option<
        unsafe extern "C" fn(
            cast_function: duckdb_cast_function,
            extra_info: *mut ::std::os::raw::c_void,
            destroy: duckdb_delete_callback_t,
        ),
    >,
    pub duckdb_cast_function_get_extra_info: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_function_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_cast_function_get_cast_mode:
        ::std::option::Option<unsafe extern "C" fn(info: duckdb_function_info) -> duckdb_cast_mode>,
    pub duckdb_cast_function_set_error: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_function_info, error: *const ::std::os::raw::c_char),
    >,
    pub duckdb_cast_function_set_row_error: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_function_info,
            error: *const ::std::os::raw::c_char,
            row: idx_t,
            output: duckdb_vector,
        ),
    >,
    pub duckdb_register_cast_function: ::std::option::Option<
        unsafe extern "C" fn(
            con: duckdb_connection,
            cast_function: duckdb_cast_function,
        ) -> duckdb_state,
    >,
    pub duckdb_destroy_cast_function:
        ::std::option::Option<unsafe extern "C" fn(cast_function: *mut duckdb_cast_function)>,
    pub duckdb_is_finite_timestamp_s:
        ::std::option::Option<unsafe extern "C" fn(ts: duckdb_timestamp_s) -> bool>,
    pub duckdb_is_finite_timestamp_ms:
        ::std::option::Option<unsafe extern "C" fn(ts: duckdb_timestamp_ms) -> bool>,
    pub duckdb_is_finite_timestamp_ns:
        ::std::option::Option<unsafe extern "C" fn(ts: duckdb_timestamp_ns) -> bool>,
    pub duckdb_create_timestamp_tz:
        ::std::option::Option<unsafe extern "C" fn(input: duckdb_timestamp) -> duckdb_value>,
    pub duckdb_create_timestamp_s:
        ::std::option::Option<unsafe extern "C" fn(input: duckdb_timestamp_s) -> duckdb_value>,
    pub duckdb_create_timestamp_ms:
        ::std::option::Option<unsafe extern "C" fn(input: duckdb_timestamp_ms) -> duckdb_value>,
    pub duckdb_create_timestamp_ns:
        ::std::option::Option<unsafe extern "C" fn(input: duckdb_timestamp_ns) -> duckdb_value>,
    pub duckdb_get_timestamp_tz:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_timestamp>,
    pub duckdb_get_timestamp_s:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_timestamp_s>,
    pub duckdb_get_timestamp_ms:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_timestamp_ms>,
    pub duckdb_get_timestamp_ns:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_timestamp_ns>,
    pub duckdb_append_value: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: duckdb_value) -> duckdb_state,
    >,
    pub duckdb_get_profiling_info: ::std::option::Option<
        unsafe extern "C" fn(connection: duckdb_connection) -> duckdb_profiling_info,
    >,
    pub duckdb_profiling_info_get_value: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_profiling_info,
            key: *const ::std::os::raw::c_char,
        ) -> duckdb_value,
    >,
    pub duckdb_appender_begin_row:
        ::std::option::Option<unsafe extern "C" fn(appender: duckdb_appender) -> duckdb_state>,
    pub duckdb_appender_end_row:
        ::std::option::Option<unsafe extern "C" fn(appender: duckdb_appender) -> duckdb_state>,
    pub duckdb_append_default:
        ::std::option::Option<unsafe extern "C" fn(appender: duckdb_appender) -> duckdb_state>,
    pub duckdb_append_bool: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: bool) -> duckdb_state,
    >,
    pub duckdb_append_int8: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: i8) -> duckdb_state,
    >,
    pub duckdb_append_int16: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: i16) -> duckdb_state,
    >,
    pub duckdb_append_int32: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: i32) -> duckdb_state,
    >,
    pub duckdb_append_int64: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: i64) -> duckdb_state,
    >,
    pub duckdb_append_hugeint: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: duckdb_hugeint) -> duckdb_state,
    >,
    pub duckdb_append_uint8: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: u8) -> duckdb_state,
    >,
    pub duckdb_append_uint16: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: u16) -> duckdb_state,
    >,
    pub duckdb_append_uint32: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: u32) -> duckdb_state,
    >,
    pub duckdb_append_uint64: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: u64) -> duckdb_state,
    >,
    pub duckdb_append_uhugeint: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: duckdb_uhugeint) -> duckdb_state,
    >,
    pub duckdb_append_float: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: f32) -> duckdb_state,
    >,
    pub duckdb_append_double: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: f64) -> duckdb_state,
    >,
    pub duckdb_append_date: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: duckdb_date) -> duckdb_state,
    >,
    pub duckdb_append_time: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: duckdb_time) -> duckdb_state,
    >,
    pub duckdb_append_timestamp: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: duckdb_timestamp) -> duckdb_state,
    >,
    pub duckdb_append_interval: ::std::option::Option<
        unsafe extern "C" fn(appender: duckdb_appender, value: duckdb_interval) -> duckdb_state,
    >,
    pub duckdb_append_varchar: ::std::option::Option<
        unsafe extern "C" fn(
            appender: duckdb_appender,
            val: *const ::std::os::raw::c_char,
        ) -> duckdb_state,
    >,
    pub duckdb_append_varchar_length: ::std::option::Option<
        unsafe extern "C" fn(
            appender: duckdb_appender,
            val: *const ::std::os::raw::c_char,
            length: idx_t,
        ) -> duckdb_state,
    >,
    pub duckdb_append_blob: ::std::option::Option<
        unsafe extern "C" fn(
            appender: duckdb_appender,
            data: *const ::std::os::raw::c_void,
            length: idx_t,
        ) -> duckdb_state,
    >,
    pub duckdb_append_null:
        ::std::option::Option<unsafe extern "C" fn(appender: duckdb_appender) -> duckdb_state>,
    pub duckdb_row_count:
        ::std::option::Option<unsafe extern "C" fn(result: *mut duckdb_result) -> idx_t>,
    pub duckdb_column_data: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_nullmask_data: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t) -> *mut bool,
    >,
    pub duckdb_result_get_chunk: ::std::option::Option<
        unsafe extern "C" fn(result: duckdb_result, chunk_index: idx_t) -> duckdb_data_chunk,
    >,
    pub duckdb_result_is_streaming:
        ::std::option::Option<unsafe extern "C" fn(result: duckdb_result) -> bool>,
    pub duckdb_result_chunk_count:
        ::std::option::Option<unsafe extern "C" fn(result: duckdb_result) -> idx_t>,
    pub duckdb_value_boolean: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> bool,
    >,
    pub duckdb_value_int8: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> i8,
    >,
    pub duckdb_value_int16: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> i16,
    >,
    pub duckdb_value_int32: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> i32,
    >,
    pub duckdb_value_int64: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> i64,
    >,
    pub duckdb_value_hugeint: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> duckdb_hugeint,
    >,
    pub duckdb_value_uhugeint: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> duckdb_uhugeint,
    >,
    pub duckdb_value_decimal: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> duckdb_decimal,
    >,
    pub duckdb_value_uint8: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> u8,
    >,
    pub duckdb_value_uint16: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> u16,
    >,
    pub duckdb_value_uint32: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> u32,
    >,
    pub duckdb_value_uint64: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> u64,
    >,
    pub duckdb_value_float: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> f32,
    >,
    pub duckdb_value_double: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> f64,
    >,
    pub duckdb_value_date: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> duckdb_date,
    >,
    pub duckdb_value_time: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> duckdb_time,
    >,
    pub duckdb_value_timestamp: ::std::option::Option<
        unsafe extern "C" fn(
            result: *mut duckdb_result,
            col: idx_t,
            row: idx_t,
        ) -> duckdb_timestamp,
    >,
    pub duckdb_value_interval: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> duckdb_interval,
    >,
    pub duckdb_value_varchar: ::std::option::Option<
        unsafe extern "C" fn(
            result: *mut duckdb_result,
            col: idx_t,
            row: idx_t,
        ) -> *mut ::std::os::raw::c_char,
    >,
    pub duckdb_value_string: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> duckdb_string,
    >,
    pub duckdb_value_varchar_internal: ::std::option::Option<
        unsafe extern "C" fn(
            result: *mut duckdb_result,
            col: idx_t,
            row: idx_t,
        ) -> *mut ::std::os::raw::c_char,
    >,
    pub duckdb_value_string_internal: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> duckdb_string,
    >,
    pub duckdb_value_blob: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> duckdb_blob,
    >,
    pub duckdb_value_is_null: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result, col: idx_t, row: idx_t) -> bool,
    >,
    pub duckdb_execute_prepared_streaming: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            out_result: *mut duckdb_result,
        ) -> duckdb_state,
    >,
    pub duckdb_pending_prepared_streaming: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            out_result: *mut duckdb_pending_result,
        ) -> duckdb_state,
    >,
    pub duckdb_query_arrow: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            query: *const ::std::os::raw::c_char,
            out_result: *mut duckdb_arrow,
        ) -> duckdb_state,
    >,
    pub duckdb_query_arrow_schema: ::std::option::Option<
        unsafe extern "C" fn(
            result: duckdb_arrow,
            out_schema: *mut duckdb_arrow_schema,
        ) -> duckdb_state,
    >,
    pub duckdb_prepared_arrow_schema: ::std::option::Option<
        unsafe extern "C" fn(
            prepared: duckdb_prepared_statement,
            out_schema: *mut duckdb_arrow_schema,
        ) -> duckdb_state,
    >,
    pub duckdb_result_arrow_array: ::std::option::Option<
        unsafe extern "C" fn(
            result: duckdb_result,
            chunk: duckdb_data_chunk,
            out_array: *mut duckdb_arrow_array,
        ),
    >,
    pub duckdb_query_arrow_array: ::std::option::Option<
        unsafe extern "C" fn(
            result: duckdb_arrow,
            out_array: *mut duckdb_arrow_array,
        ) -> duckdb_state,
    >,
    pub duckdb_arrow_column_count:
        ::std::option::Option<unsafe extern "C" fn(result: duckdb_arrow) -> idx_t>,
    pub duckdb_arrow_row_count:
        ::std::option::Option<unsafe extern "C" fn(result: duckdb_arrow) -> idx_t>,
    pub duckdb_arrow_rows_changed:
        ::std::option::Option<unsafe extern "C" fn(result: duckdb_arrow) -> idx_t>,
    pub duckdb_query_arrow_error: ::std::option::Option<
        unsafe extern "C" fn(result: duckdb_arrow) -> *const ::std::os::raw::c_char,
    >,
    pub duckdb_destroy_arrow:
        ::std::option::Option<unsafe extern "C" fn(result: *mut duckdb_arrow)>,
    pub duckdb_destroy_arrow_stream:
        ::std::option::Option<unsafe extern "C" fn(stream_p: *mut duckdb_arrow_stream)>,
    pub duckdb_execute_prepared_arrow: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            out_result: *mut duckdb_arrow,
        ) -> duckdb_state,
    >,
    pub duckdb_arrow_scan: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            table_name: *const ::std::os::raw::c_char,
            arrow: duckdb_arrow_stream,
        ) -> duckdb_state,
    >,
    pub duckdb_arrow_array_scan: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            table_name: *const ::std::os::raw::c_char,
            arrow_schema: duckdb_arrow_schema,
            arrow_array: duckdb_arrow_array,
            out_stream: *mut duckdb_arrow_stream,
        ) -> duckdb_state,
    >,
    pub duckdb_stream_fetch_chunk:
        ::std::option::Option<unsafe extern "C" fn(result: duckdb_result) -> duckdb_data_chunk>,
    pub duckdb_create_instance_cache:
        ::std::option::Option<unsafe extern "C" fn() -> duckdb_instance_cache>,
    pub duckdb_get_or_create_from_cache: ::std::option::Option<
        unsafe extern "C" fn(
            instance_cache: duckdb_instance_cache,
            path: *const ::std::os::raw::c_char,
            out_database: *mut duckdb_database,
            config: duckdb_config,
            out_error: *mut *mut ::std::os::raw::c_char,
        ) -> duckdb_state,
    >,
    pub duckdb_destroy_instance_cache:
        ::std::option::Option<unsafe extern "C" fn(instance_cache: *mut duckdb_instance_cache)>,
    pub duckdb_append_default_to_chunk: ::std::option::Option<
        unsafe extern "C" fn(
            appender: duckdb_appender,
            chunk: duckdb_data_chunk,
            col: idx_t,
            row: idx_t,
        ) -> duckdb_state,
    >,
    pub duckdb_appender_error_data:
        ::std::option::Option<unsafe extern "C" fn(appender: duckdb_appender) -> duckdb_error_data>,
    pub duckdb_appender_create_query: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            query: *const ::std::os::raw::c_char,
            column_count: idx_t,
            types: *mut duckdb_logical_type,
            table_name: *const ::std::os::raw::c_char,
            column_names: *mut *const ::std::os::raw::c_char,
            out_appender: *mut duckdb_appender,
        ) -> duckdb_state,
    >,
    pub duckdb_appender_clear:
        ::std::option::Option<unsafe extern "C" fn(appender: duckdb_appender) -> duckdb_state>,
    pub duckdb_to_arrow_schema: ::std::option::Option<
        unsafe extern "C" fn(
            arrow_options: duckdb_arrow_options,
            types: *mut duckdb_logical_type,
            names: *mut *const ::std::os::raw::c_char,
            column_count: idx_t,
            out_schema: *mut ArrowSchema,
        ) -> duckdb_error_data,
    >,
    pub duckdb_data_chunk_to_arrow: ::std::option::Option<
        unsafe extern "C" fn(
            arrow_options: duckdb_arrow_options,
            chunk: duckdb_data_chunk,
            out_arrow_array: *mut ArrowArray,
        ) -> duckdb_error_data,
    >,
    pub duckdb_schema_from_arrow: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            schema: *mut ArrowSchema,
            out_types: *mut duckdb_arrow_converted_schema,
        ) -> duckdb_error_data,
    >,
    pub duckdb_data_chunk_from_arrow: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            arrow_array: *mut ArrowArray,
            converted_schema: duckdb_arrow_converted_schema,
            out_chunk: *mut duckdb_data_chunk,
        ) -> duckdb_error_data,
    >,
    pub duckdb_destroy_arrow_converted_schema: ::std::option::Option<
        unsafe extern "C" fn(arrow_converted_schema: *mut duckdb_arrow_converted_schema),
    >,
    pub duckdb_client_context_get_catalog: ::std::option::Option<
        unsafe extern "C" fn(
            context: duckdb_client_context,
            catalog_name: *const ::std::os::raw::c_char,
        ) -> duckdb_catalog,
    >,
    pub duckdb_catalog_get_type_name: ::std::option::Option<
        unsafe extern "C" fn(catalog: duckdb_catalog) -> *const ::std::os::raw::c_char,
    >,
    pub duckdb_catalog_get_entry: ::std::option::Option<
        unsafe extern "C" fn(
            catalog: duckdb_catalog,
            context: duckdb_client_context,
            entry_type: duckdb_catalog_entry_type,
            schema_name: *const ::std::os::raw::c_char,
            entry_name: *const ::std::os::raw::c_char,
        ) -> duckdb_catalog_entry,
    >,
    pub duckdb_destroy_catalog:
        ::std::option::Option<unsafe extern "C" fn(catalog: *mut duckdb_catalog)>,
    pub duckdb_catalog_entry_get_type: ::std::option::Option<
        unsafe extern "C" fn(entry: duckdb_catalog_entry) -> duckdb_catalog_entry_type,
    >,
    pub duckdb_catalog_entry_get_name: ::std::option::Option<
        unsafe extern "C" fn(entry: duckdb_catalog_entry) -> *const ::std::os::raw::c_char,
    >,
    pub duckdb_destroy_catalog_entry:
        ::std::option::Option<unsafe extern "C" fn(entry: *mut duckdb_catalog_entry)>,
    pub duckdb_create_config_option:
        ::std::option::Option<unsafe extern "C" fn() -> duckdb_config_option>,
    pub duckdb_destroy_config_option:
        ::std::option::Option<unsafe extern "C" fn(option: *mut duckdb_config_option)>,
    pub duckdb_config_option_set_name: ::std::option::Option<
        unsafe extern "C" fn(option: duckdb_config_option, name: *const ::std::os::raw::c_char),
    >,
    pub duckdb_config_option_set_type: ::std::option::Option<
        unsafe extern "C" fn(option: duckdb_config_option, type_: duckdb_logical_type),
    >,
    pub duckdb_config_option_set_default_value: ::std::option::Option<
        unsafe extern "C" fn(option: duckdb_config_option, default_value: duckdb_value),
    >,
    pub duckdb_config_option_set_default_scope: ::std::option::Option<
        unsafe extern "C" fn(
            option: duckdb_config_option,
            default_scope: duckdb_config_option_scope,
        ),
    >,
    pub duckdb_config_option_set_description: ::std::option::Option<
        unsafe extern "C" fn(
            option: duckdb_config_option,
            description: *const ::std::os::raw::c_char,
        ),
    >,
    pub duckdb_register_config_option: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            option: duckdb_config_option,
        ) -> duckdb_state,
    >,
    pub duckdb_client_context_get_config_option: ::std::option::Option<
        unsafe extern "C" fn(
            context: duckdb_client_context,
            name: *const ::std::os::raw::c_char,
            out_scope: *mut duckdb_config_option_scope,
        ) -> duckdb_value,
    >,
    pub duckdb_create_copy_function:
        ::std::option::Option<unsafe extern "C" fn() -> duckdb_copy_function>,
    pub duckdb_copy_function_set_name: ::std::option::Option<
        unsafe extern "C" fn(
            copy_function: duckdb_copy_function,
            name: *const ::std::os::raw::c_char,
        ),
    >,
    pub duckdb_copy_function_set_extra_info: ::std::option::Option<
        unsafe extern "C" fn(
            copy_function: duckdb_copy_function,
            extra_info: *mut ::std::os::raw::c_void,
            destructor: duckdb_delete_callback_t,
        ),
    >,
    pub duckdb_register_copy_function: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            copy_function: duckdb_copy_function,
        ) -> duckdb_state,
    >,
    pub duckdb_destroy_copy_function:
        ::std::option::Option<unsafe extern "C" fn(copy_function: *mut duckdb_copy_function)>,
    pub duckdb_copy_function_set_bind: ::std::option::Option<
        unsafe extern "C" fn(
            copy_function: duckdb_copy_function,
            bind: duckdb_copy_function_bind_t,
        ),
    >,
    pub duckdb_copy_function_bind_set_error: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_copy_function_bind_info,
            error: *const ::std::os::raw::c_char,
        ),
    >,
    pub duckdb_copy_function_bind_get_extra_info: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_copy_function_bind_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_copy_function_bind_get_client_context: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_copy_function_bind_info) -> duckdb_client_context,
    >,
    pub duckdb_copy_function_bind_get_column_count:
        ::std::option::Option<unsafe extern "C" fn(info: duckdb_copy_function_bind_info) -> idx_t>,
    pub duckdb_copy_function_bind_get_column_type: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_copy_function_bind_info,
            col_idx: idx_t,
        ) -> duckdb_logical_type,
    >,
    pub duckdb_copy_function_bind_get_options: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_copy_function_bind_info) -> duckdb_value,
    >,
    pub duckdb_copy_function_bind_set_bind_data: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_copy_function_bind_info,
            bind_data: *mut ::std::os::raw::c_void,
            destructor: duckdb_delete_callback_t,
        ),
    >,
    pub duckdb_copy_function_set_global_init: ::std::option::Option<
        unsafe extern "C" fn(
            copy_function: duckdb_copy_function,
            init: duckdb_copy_function_global_init_t,
        ),
    >,
    pub duckdb_copy_function_global_init_set_error: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_copy_function_global_init_info,
            error: *const ::std::os::raw::c_char,
        ),
    >,
    pub duckdb_copy_function_global_init_get_extra_info: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_copy_function_global_init_info,
        ) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_copy_function_global_init_get_client_context: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_copy_function_global_init_info) -> duckdb_client_context,
    >,
    pub duckdb_copy_function_global_init_get_bind_data: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_copy_function_global_init_info,
        ) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_copy_function_global_init_set_global_state: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_copy_function_global_init_info,
            global_state: *mut ::std::os::raw::c_void,
            destructor: duckdb_delete_callback_t,
        ),
    >,
    pub duckdb_copy_function_global_init_get_file_path: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_copy_function_global_init_info,
        ) -> *const ::std::os::raw::c_char,
    >,
    pub duckdb_copy_function_set_sink: ::std::option::Option<
        unsafe extern "C" fn(
            copy_function: duckdb_copy_function,
            function: duckdb_copy_function_sink_t,
        ),
    >,
    pub duckdb_copy_function_sink_set_error: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_copy_function_sink_info,
            error: *const ::std::os::raw::c_char,
        ),
    >,
    pub duckdb_copy_function_sink_get_extra_info: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_copy_function_sink_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_copy_function_sink_get_client_context: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_copy_function_sink_info) -> duckdb_client_context,
    >,
    pub duckdb_copy_function_sink_get_bind_data: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_copy_function_sink_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_copy_function_sink_get_global_state: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_copy_function_sink_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_copy_function_set_finalize: ::std::option::Option<
        unsafe extern "C" fn(
            copy_function: duckdb_copy_function,
            finalize: duckdb_copy_function_finalize_t,
        ),
    >,
    pub duckdb_copy_function_finalize_set_error: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_copy_function_finalize_info,
            error: *const ::std::os::raw::c_char,
        ),
    >,
    pub duckdb_copy_function_finalize_get_extra_info: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_copy_function_finalize_info,
        ) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_copy_function_finalize_get_client_context: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_copy_function_finalize_info) -> duckdb_client_context,
    >,
    pub duckdb_copy_function_finalize_get_bind_data: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_copy_function_finalize_info,
        ) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_copy_function_finalize_get_global_state: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_copy_function_finalize_info,
        ) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_copy_function_set_copy_from_function: ::std::option::Option<
        unsafe extern "C" fn(
            copy_function: duckdb_copy_function,
            table_function: duckdb_table_function,
        ),
    >,
    pub duckdb_table_function_bind_get_result_column_count:
        ::std::option::Option<unsafe extern "C" fn(info: duckdb_bind_info) -> idx_t>,
    pub duckdb_table_function_bind_get_result_column_name: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_bind_info,
            col_idx: idx_t,
        ) -> *const ::std::os::raw::c_char,
    >,
    pub duckdb_table_function_bind_get_result_column_type: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_bind_info, col_idx: idx_t) -> duckdb_logical_type,
    >,
    pub duckdb_create_error_data: ::std::option::Option<
        unsafe extern "C" fn(
            type_: duckdb_error_type,
            message: *const ::std::os::raw::c_char,
        ) -> duckdb_error_data,
    >,
    pub duckdb_destroy_error_data:
        ::std::option::Option<unsafe extern "C" fn(error_data: *mut duckdb_error_data)>,
    pub duckdb_error_data_error_type: ::std::option::Option<
        unsafe extern "C" fn(error_data: duckdb_error_data) -> duckdb_error_type,
    >,
    pub duckdb_error_data_message: ::std::option::Option<
        unsafe extern "C" fn(error_data: duckdb_error_data) -> *const ::std::os::raw::c_char,
    >,
    pub duckdb_error_data_has_error:
        ::std::option::Option<unsafe extern "C" fn(error_data: duckdb_error_data) -> bool>,
    pub duckdb_destroy_expression:
        ::std::option::Option<unsafe extern "C" fn(expr: *mut duckdb_expression)>,
    pub duckdb_expression_return_type:
        ::std::option::Option<unsafe extern "C" fn(expr: duckdb_expression) -> duckdb_logical_type>,
    pub duckdb_expression_is_foldable:
        ::std::option::Option<unsafe extern "C" fn(expr: duckdb_expression) -> bool>,
    pub duckdb_expression_fold: ::std::option::Option<
        unsafe extern "C" fn(
            context: duckdb_client_context,
            expr: duckdb_expression,
            out_value: *mut duckdb_value,
        ) -> duckdb_error_data,
    >,
    pub duckdb_client_context_get_file_system: ::std::option::Option<
        unsafe extern "C" fn(context: duckdb_client_context) -> duckdb_file_system,
    >,
    pub duckdb_destroy_file_system:
        ::std::option::Option<unsafe extern "C" fn(file_system: *mut duckdb_file_system)>,
    pub duckdb_file_system_open: ::std::option::Option<
        unsafe extern "C" fn(
            file_system: duckdb_file_system,
            path: *const ::std::os::raw::c_char,
            options: duckdb_file_open_options,
            out_file: *mut duckdb_file_handle,
        ) -> duckdb_state,
    >,
    pub duckdb_file_system_error_data: ::std::option::Option<
        unsafe extern "C" fn(file_system: duckdb_file_system) -> duckdb_error_data,
    >,
    pub duckdb_create_file_open_options:
        ::std::option::Option<unsafe extern "C" fn() -> duckdb_file_open_options>,
    pub duckdb_file_open_options_set_flag: ::std::option::Option<
        unsafe extern "C" fn(
            options: duckdb_file_open_options,
            flag: duckdb_file_flag,
            value: bool,
        ) -> duckdb_state,
    >,
    pub duckdb_destroy_file_open_options:
        ::std::option::Option<unsafe extern "C" fn(options: *mut duckdb_file_open_options)>,
    pub duckdb_destroy_file_handle:
        ::std::option::Option<unsafe extern "C" fn(file_handle: *mut duckdb_file_handle)>,
    pub duckdb_file_handle_error_data: ::std::option::Option<
        unsafe extern "C" fn(file_handle: duckdb_file_handle) -> duckdb_error_data,
    >,
    pub duckdb_file_handle_close: ::std::option::Option<
        unsafe extern "C" fn(file_handle: duckdb_file_handle) -> duckdb_state,
    >,
    pub duckdb_file_handle_read: ::std::option::Option<
        unsafe extern "C" fn(
            file_handle: duckdb_file_handle,
            buffer: *mut ::std::os::raw::c_void,
            size: i64,
        ) -> i64,
    >,
    pub duckdb_file_handle_write: ::std::option::Option<
        unsafe extern "C" fn(
            file_handle: duckdb_file_handle,
            buffer: *const ::std::os::raw::c_void,
            size: i64,
        ) -> i64,
    >,
    pub duckdb_file_handle_seek: ::std::option::Option<
        unsafe extern "C" fn(file_handle: duckdb_file_handle, position: i64) -> duckdb_state,
    >,
    pub duckdb_file_handle_tell:
        ::std::option::Option<unsafe extern "C" fn(file_handle: duckdb_file_handle) -> i64>,
    pub duckdb_file_handle_sync: ::std::option::Option<
        unsafe extern "C" fn(file_handle: duckdb_file_handle) -> duckdb_state,
    >,
    pub duckdb_file_handle_size:
        ::std::option::Option<unsafe extern "C" fn(file_handle: duckdb_file_handle) -> i64>,
    pub duckdb_geometry_type_get_crs: ::std::option::Option<
        unsafe extern "C" fn(type_: duckdb_logical_type) -> *mut ::std::os::raw::c_char,
    >,
    pub duckdb_create_log_storage:
        ::std::option::Option<unsafe extern "C" fn() -> duckdb_log_storage>,
    pub duckdb_destroy_log_storage:
        ::std::option::Option<unsafe extern "C" fn(log_storage: *mut duckdb_log_storage)>,
    pub duckdb_log_storage_set_write_log_entry: ::std::option::Option<
        unsafe extern "C" fn(
            log_storage: duckdb_log_storage,
            function: duckdb_logger_write_log_entry_t,
        ),
    >,
    pub duckdb_log_storage_set_extra_data: ::std::option::Option<
        unsafe extern "C" fn(
            log_storage: duckdb_log_storage,
            extra_data: *mut ::std::os::raw::c_void,
            delete_callback: duckdb_delete_callback_t,
        ),
    >,
    pub duckdb_log_storage_set_name: ::std::option::Option<
        unsafe extern "C" fn(log_storage: duckdb_log_storage, name: *const ::std::os::raw::c_char),
    >,
    pub duckdb_register_log_storage: ::std::option::Option<
        unsafe extern "C" fn(
            database: duckdb_database,
            log_storage: duckdb_log_storage,
        ) -> duckdb_state,
    >,
    pub duckdb_client_context_get_connection_id:
        ::std::option::Option<unsafe extern "C" fn(context: duckdb_client_context) -> idx_t>,
    pub duckdb_destroy_client_context:
        ::std::option::Option<unsafe extern "C" fn(context: *mut duckdb_client_context)>,
    pub duckdb_connection_get_client_context: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            out_context: *mut duckdb_client_context,
        ),
    >,
    pub duckdb_get_table_names: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            query: *const ::std::os::raw::c_char,
            qualified: bool,
        ) -> duckdb_value,
    >,
    pub duckdb_connection_get_arrow_options: ::std::option::Option<
        unsafe extern "C" fn(
            connection: duckdb_connection,
            out_arrow_options: *mut duckdb_arrow_options,
        ),
    >,
    pub duckdb_destroy_arrow_options:
        ::std::option::Option<unsafe extern "C" fn(arrow_options: *mut duckdb_arrow_options)>,
    pub duckdb_prepared_statement_column_count: ::std::option::Option<
        unsafe extern "C" fn(prepared_statement: duckdb_prepared_statement) -> idx_t,
    >,
    pub duckdb_prepared_statement_column_name: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            col_idx: idx_t,
        ) -> *const ::std::os::raw::c_char,
    >,
    pub duckdb_prepared_statement_column_logical_type: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            col_idx: idx_t,
        ) -> duckdb_logical_type,
    >,
    pub duckdb_prepared_statement_column_type: ::std::option::Option<
        unsafe extern "C" fn(
            prepared_statement: duckdb_prepared_statement,
            col_idx: idx_t,
        ) -> duckdb_type,
    >,
    pub duckdb_result_get_arrow_options: ::std::option::Option<
        unsafe extern "C" fn(result: *mut duckdb_result) -> duckdb_arrow_options,
    >,
    pub duckdb_scalar_function_set_bind: ::std::option::Option<
        unsafe extern "C" fn(
            scalar_function: duckdb_scalar_function,
            bind: duckdb_scalar_function_bind_t,
        ),
    >,
    pub duckdb_scalar_function_bind_set_error: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_bind_info, error: *const ::std::os::raw::c_char),
    >,
    pub duckdb_scalar_function_get_client_context: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_bind_info, out_context: *mut duckdb_client_context),
    >,
    pub duckdb_scalar_function_set_bind_data: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_bind_info,
            bind_data: *mut ::std::os::raw::c_void,
            destroy: duckdb_delete_callback_t,
        ),
    >,
    pub duckdb_scalar_function_get_bind_data: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_function_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_scalar_function_bind_get_extra_info: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_bind_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_scalar_function_bind_get_argument_count:
        ::std::option::Option<unsafe extern "C" fn(info: duckdb_bind_info) -> idx_t>,
    pub duckdb_scalar_function_bind_get_argument: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_bind_info, index: idx_t) -> duckdb_expression,
    >,
    pub duckdb_scalar_function_set_bind_data_copy: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_bind_info, copy: duckdb_copy_callback_t),
    >,
    pub duckdb_scalar_function_get_state: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_function_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_scalar_function_set_init: ::std::option::Option<
        unsafe extern "C" fn(
            scalar_function: duckdb_scalar_function,
            init: duckdb_scalar_function_init_t,
        ),
    >,
    pub duckdb_scalar_function_init_set_error: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_init_info, error: *const ::std::os::raw::c_char),
    >,
    pub duckdb_scalar_function_init_set_state: ::std::option::Option<
        unsafe extern "C" fn(
            info: duckdb_init_info,
            state: *mut ::std::os::raw::c_void,
            destroy: duckdb_delete_callback_t,
        ),
    >,
    pub duckdb_scalar_function_init_get_client_context: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_init_info, out_context: *mut duckdb_client_context),
    >,
    pub duckdb_scalar_function_init_get_bind_data: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_init_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_scalar_function_init_get_extra_info: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_init_info) -> *mut ::std::os::raw::c_void,
    >,
    pub duckdb_value_to_string: ::std::option::Option<
        unsafe extern "C" fn(value: duckdb_value) -> *mut ::std::os::raw::c_char,
    >,
    pub duckdb_valid_utf8_check: ::std::option::Option<
        unsafe extern "C" fn(str_: *const ::std::os::raw::c_char, len: idx_t) -> duckdb_error_data,
    >,
    pub duckdb_table_description_get_column_count: ::std::option::Option<
        unsafe extern "C" fn(table_description: duckdb_table_description) -> idx_t,
    >,
    pub duckdb_table_description_get_column_type: ::std::option::Option<
        unsafe extern "C" fn(
            table_description: duckdb_table_description,
            index: idx_t,
        ) -> duckdb_logical_type,
    >,
    pub duckdb_table_function_get_client_context: ::std::option::Option<
        unsafe extern "C" fn(info: duckdb_bind_info, out_context: *mut duckdb_client_context),
    >,
    pub duckdb_create_map_value: ::std::option::Option<
        unsafe extern "C" fn(
            map_type: duckdb_logical_type,
            keys: *mut duckdb_value,
            values: *mut duckdb_value,
            entry_count: idx_t,
        ) -> duckdb_value,
    >,
    pub duckdb_create_union_value: ::std::option::Option<
        unsafe extern "C" fn(
            union_type: duckdb_logical_type,
            tag_index: idx_t,
            value: duckdb_value,
        ) -> duckdb_value,
    >,
    pub duckdb_create_time_ns:
        ::std::option::Option<unsafe extern "C" fn(input: duckdb_time_ns) -> duckdb_value>,
    pub duckdb_get_time_ns:
        ::std::option::Option<unsafe extern "C" fn(val: duckdb_value) -> duckdb_time_ns>,
    pub duckdb_create_vector: ::std::option::Option<
        unsafe extern "C" fn(type_: duckdb_logical_type, capacity: idx_t) -> duckdb_vector,
    >,
    pub duckdb_destroy_vector:
        ::std::option::Option<unsafe extern "C" fn(vector: *mut duckdb_vector)>,
    pub duckdb_slice_vector: ::std::option::Option<
        unsafe extern "C" fn(vector: duckdb_vector, sel: duckdb_selection_vector, len: idx_t),
    >,
    pub duckdb_vector_reference_value:
        ::std::option::Option<unsafe extern "C" fn(vector: duckdb_vector, value: duckdb_value)>,
    pub duckdb_vector_reference_vector: ::std::option::Option<
        unsafe extern "C" fn(to_vector: duckdb_vector, from_vector: duckdb_vector),
    >,
    pub duckdb_create_selection_vector:
        ::std::option::Option<unsafe extern "C" fn(size: idx_t) -> duckdb_selection_vector>,
    pub duckdb_destroy_selection_vector:
        ::std::option::Option<unsafe extern "C" fn(sel: duckdb_selection_vector)>,
    pub duckdb_selection_vector_get_data_ptr:
        ::std::option::Option<unsafe extern "C" fn(sel: duckdb_selection_vector) -> *mut sel_t>,
    pub duckdb_vector_copy_sel: ::std::option::Option<
        unsafe extern "C" fn(
            src: duckdb_vector,
            dst: duckdb_vector,
            sel: duckdb_selection_vector,
            src_count: idx_t,
            src_offset: idx_t,
            dst_offset: idx_t,
        ),
    >,
    pub duckdb_unsafe_vector_assign_string_element_len: ::std::option::Option<
        unsafe extern "C" fn(
            vector: duckdb_vector,
            index: idx_t,
            str_: *const ::std::os::raw::c_char,
            str_len: idx_t,
        ),
    >,
}
static DUCKDB_OPEN: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_open(
    a0: *const ::std::os::raw::c_char,
    a1: *mut duckdb_database,
) -> duckdb_state {
    let p = DUCKDB_OPEN.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_open: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            *const ::std::os::raw::c_char,
            *mut duckdb_database,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_OPEN_EXT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_open_ext(
    a0: *const ::std::os::raw::c_char,
    a1: *mut duckdb_database,
    a2: duckdb_config,
    a3: *mut *mut ::std::os::raw::c_char,
) -> duckdb_state {
    let p = DUCKDB_OPEN_EXT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_open_ext: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            *const ::std::os::raw::c_char,
            *mut duckdb_database,
            duckdb_config,
            *mut *mut ::std::os::raw::c_char,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2, a3)
    }
}
static DUCKDB_CLOSE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_close(a0: *mut duckdb_database) {
    let p = DUCKDB_CLOSE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_close: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_database) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CONNECT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_connect(
    a0: duckdb_database,
    a1: *mut duckdb_connection,
) -> duckdb_state {
    let p = DUCKDB_CONNECT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_connect: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_database, *mut duckdb_connection) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_INTERRUPT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_interrupt(a0: duckdb_connection) {
    let p = DUCKDB_INTERRUPT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_interrupt: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_connection) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_QUERY_PROGRESS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_query_progress(a0: duckdb_connection) -> duckdb_query_progress_type {
    let p = DUCKDB_QUERY_PROGRESS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_query_progress: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_connection) -> duckdb_query_progress_type =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DISCONNECT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_disconnect(a0: *mut duckdb_connection) {
    let p = DUCKDB_DISCONNECT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_disconnect: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_connection) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_LIBRARY_VERSION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_library_version() -> *const ::std::os::raw::c_char {
    let p = DUCKDB_LIBRARY_VERSION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_library_version: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn() -> *const ::std::os::raw::c_char = ::std::mem::transmute(p);
        f()
    }
}
static DUCKDB_CREATE_CONFIG: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_config(a0: *mut duckdb_config) -> duckdb_state {
    let p = DUCKDB_CREATE_CONFIG.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_config: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_config) -> duckdb_state = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CONFIG_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_config_count() -> usize {
    let p = DUCKDB_CONFIG_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_config_count: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn() -> usize = ::std::mem::transmute(p);
        f()
    }
}
static DUCKDB_GET_CONFIG_FLAG: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_config_flag(
    a0: usize,
    a1: *mut *const ::std::os::raw::c_char,
    a2: *mut *const ::std::os::raw::c_char,
) -> duckdb_state {
    let p = DUCKDB_GET_CONFIG_FLAG.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_config_flag: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            usize,
            *mut *const ::std::os::raw::c_char,
            *mut *const ::std::os::raw::c_char,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_SET_CONFIG: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_set_config(
    a0: duckdb_config,
    a1: *const ::std::os::raw::c_char,
    a2: *const ::std::os::raw::c_char,
) -> duckdb_state {
    let p = DUCKDB_SET_CONFIG.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_set_config: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_config,
            *const ::std::os::raw::c_char,
            *const ::std::os::raw::c_char,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_DESTROY_CONFIG: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_config(a0: *mut duckdb_config) {
    let p = DUCKDB_DESTROY_CONFIG.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_config: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_config) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_QUERY: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_query(
    a0: duckdb_connection,
    a1: *const ::std::os::raw::c_char,
    a2: *mut duckdb_result,
) -> duckdb_state {
    let p = DUCKDB_QUERY.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_query: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            *const ::std::os::raw::c_char,
            *mut duckdb_result,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_DESTROY_RESULT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_result(a0: *mut duckdb_result) {
    let p = DUCKDB_DESTROY_RESULT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_result: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COLUMN_NAME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_column_name(
    a0: *mut duckdb_result,
    a1: idx_t,
) -> *const ::std::os::raw::c_char {
    let p = DUCKDB_COLUMN_NAME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_column_name: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t) -> *const ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_COLUMN_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_column_type(
    a0: *mut duckdb_result,
    a1: idx_t,
) -> duckdb_type {
    let p = DUCKDB_COLUMN_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_column_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t) -> duckdb_type =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_RESULT_STATEMENT_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_result_statement_type(a0: duckdb_result) -> duckdb_statement_type {
    let p = DUCKDB_RESULT_STATEMENT_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_result_statement_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_result) -> duckdb_statement_type =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COLUMN_LOGICAL_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_column_logical_type(
    a0: *mut duckdb_result,
    a1: idx_t,
) -> duckdb_logical_type {
    let p = DUCKDB_COLUMN_LOGICAL_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_column_logical_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_COLUMN_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_column_count(a0: *mut duckdb_result) -> idx_t {
    let p = DUCKDB_COLUMN_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_column_count: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_ROWS_CHANGED: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_rows_changed(a0: *mut duckdb_result) -> idx_t {
    let p = DUCKDB_ROWS_CHANGED.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_rows_changed: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_RESULT_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_result_error(a0: *mut duckdb_result) -> *const ::std::os::raw::c_char {
    let p = DUCKDB_RESULT_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_result_error: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result) -> *const ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_RESULT_ERROR_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_result_error_type(a0: *mut duckdb_result) -> duckdb_error_type {
    let p = DUCKDB_RESULT_ERROR_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_result_error_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result) -> duckdb_error_type =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_RESULT_RETURN_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_result_return_type(a0: duckdb_result) -> duckdb_result_type {
    let p = DUCKDB_RESULT_RETURN_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_result_return_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_result) -> duckdb_result_type = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_MALLOC: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_malloc(a0: usize) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_MALLOC.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_malloc: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(usize) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_FREE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_free(a0: *mut ::std::os::raw::c_void) {
    let p = DUCKDB_FREE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_free: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut ::std::os::raw::c_void) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_VECTOR_SIZE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_vector_size() -> idx_t {
    let p = DUCKDB_VECTOR_SIZE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_vector_size: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn() -> idx_t = ::std::mem::transmute(p);
        f()
    }
}
static DUCKDB_STRING_IS_INLINED: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_string_is_inlined(a0: duckdb_string_t) -> bool {
    let p = DUCKDB_STRING_IS_INLINED.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_string_is_inlined: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_string_t) -> bool = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_STRING_T_LENGTH: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_string_t_length(a0: duckdb_string_t) -> u32 {
    let p = DUCKDB_STRING_T_LENGTH.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_string_t_length: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_string_t) -> u32 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_STRING_T_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_string_t_data(a0: *mut duckdb_string_t) -> *const ::std::os::raw::c_char {
    let p = DUCKDB_STRING_T_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_string_t_data: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_string_t) -> *const ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_FROM_DATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_from_date(a0: duckdb_date) -> duckdb_date_struct {
    let p = DUCKDB_FROM_DATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_from_date: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_date) -> duckdb_date_struct = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_TO_DATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_to_date(a0: duckdb_date_struct) -> duckdb_date {
    let p = DUCKDB_TO_DATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_to_date: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_date_struct) -> duckdb_date = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_IS_FINITE_DATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_is_finite_date(a0: duckdb_date) -> bool {
    let p = DUCKDB_IS_FINITE_DATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_is_finite_date: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_date) -> bool = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_FROM_TIME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_from_time(a0: duckdb_time) -> duckdb_time_struct {
    let p = DUCKDB_FROM_TIME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_from_time: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_time) -> duckdb_time_struct = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_TIME_TZ: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_time_tz(
    a0: i64,
    a1: i32,
) -> duckdb_time_tz {
    let p = DUCKDB_CREATE_TIME_TZ.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_time_tz: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(i64, i32) -> duckdb_time_tz = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_FROM_TIME_TZ: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_from_time_tz(a0: duckdb_time_tz) -> duckdb_time_tz_struct {
    let p = DUCKDB_FROM_TIME_TZ.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_from_time_tz: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_time_tz) -> duckdb_time_tz_struct =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_TO_TIME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_to_time(a0: duckdb_time_struct) -> duckdb_time {
    let p = DUCKDB_TO_TIME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_to_time: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_time_struct) -> duckdb_time = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_FROM_TIMESTAMP: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_from_timestamp(a0: duckdb_timestamp) -> duckdb_timestamp_struct {
    let p = DUCKDB_FROM_TIMESTAMP.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_from_timestamp: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_timestamp) -> duckdb_timestamp_struct =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_TO_TIMESTAMP: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_to_timestamp(a0: duckdb_timestamp_struct) -> duckdb_timestamp {
    let p = DUCKDB_TO_TIMESTAMP.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_to_timestamp: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_timestamp_struct) -> duckdb_timestamp =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_IS_FINITE_TIMESTAMP: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_is_finite_timestamp(a0: duckdb_timestamp) -> bool {
    let p = DUCKDB_IS_FINITE_TIMESTAMP.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_is_finite_timestamp: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_timestamp) -> bool = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_HUGEINT_TO_DOUBLE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_hugeint_to_double(a0: duckdb_hugeint) -> f64 {
    let p = DUCKDB_HUGEINT_TO_DOUBLE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_hugeint_to_double: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_hugeint) -> f64 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DOUBLE_TO_HUGEINT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_double_to_hugeint(a0: f64) -> duckdb_hugeint {
    let p = DUCKDB_DOUBLE_TO_HUGEINT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_double_to_hugeint: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(f64) -> duckdb_hugeint = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_UHUGEINT_TO_DOUBLE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_uhugeint_to_double(a0: duckdb_uhugeint) -> f64 {
    let p = DUCKDB_UHUGEINT_TO_DOUBLE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_uhugeint_to_double: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_uhugeint) -> f64 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DOUBLE_TO_UHUGEINT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_double_to_uhugeint(a0: f64) -> duckdb_uhugeint {
    let p = DUCKDB_DOUBLE_TO_UHUGEINT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_double_to_uhugeint: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(f64) -> duckdb_uhugeint = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DOUBLE_TO_DECIMAL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_double_to_decimal(
    a0: f64,
    a1: u8,
    a2: u8,
) -> duckdb_decimal {
    let p = DUCKDB_DOUBLE_TO_DECIMAL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_double_to_decimal: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(f64, u8, u8) -> duckdb_decimal = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_DECIMAL_TO_DOUBLE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_decimal_to_double(a0: duckdb_decimal) -> f64 {
    let p = DUCKDB_DECIMAL_TO_DOUBLE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_decimal_to_double: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_decimal) -> f64 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_PREPARE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_prepare(
    a0: duckdb_connection,
    a1: *const ::std::os::raw::c_char,
    a2: *mut duckdb_prepared_statement,
) -> duckdb_state {
    let p = DUCKDB_PREPARE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_prepare: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            *const ::std::os::raw::c_char,
            *mut duckdb_prepared_statement,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_DESTROY_PREPARE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_prepare(a0: *mut duckdb_prepared_statement) {
    let p = DUCKDB_DESTROY_PREPARE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_prepare: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_prepared_statement) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_PREPARE_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_prepare_error(a0: duckdb_prepared_statement) -> *const ::std::os::raw::c_char {
    let p = DUCKDB_PREPARE_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_prepare_error: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement) -> *const ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_NPARAMS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_nparams(a0: duckdb_prepared_statement) -> idx_t {
    let p = DUCKDB_NPARAMS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_nparams: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_PARAMETER_NAME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_parameter_name(
    a0: duckdb_prepared_statement,
    a1: idx_t,
) -> *const ::std::os::raw::c_char {
    let p = DUCKDB_PARAMETER_NAME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_parameter_name: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_prepared_statement,
            idx_t,
        ) -> *const ::std::os::raw::c_char = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_PARAM_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_param_type(
    a0: duckdb_prepared_statement,
    a1: idx_t,
) -> duckdb_type {
    let p = DUCKDB_PARAM_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_param_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t) -> duckdb_type =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_PARAM_LOGICAL_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_param_logical_type(
    a0: duckdb_prepared_statement,
    a1: idx_t,
) -> duckdb_logical_type {
    let p = DUCKDB_PARAM_LOGICAL_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_param_logical_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CLEAR_BINDINGS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_clear_bindings(a0: duckdb_prepared_statement) -> duckdb_state {
    let p = DUCKDB_CLEAR_BINDINGS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_clear_bindings: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_PREPARED_STATEMENT_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_prepared_statement_type(
    a0: duckdb_prepared_statement
) -> duckdb_statement_type {
    let p = DUCKDB_PREPARED_STATEMENT_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_prepared_statement_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement) -> duckdb_statement_type =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_BIND_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_value(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: duckdb_value,
) -> duckdb_state {
    let p = DUCKDB_BIND_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_prepared_statement,
            idx_t,
            duckdb_value,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_PARAMETER_INDEX: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_parameter_index(
    a0: duckdb_prepared_statement,
    a1: *mut idx_t,
    a2: *const ::std::os::raw::c_char,
) -> duckdb_state {
    let p = DUCKDB_BIND_PARAMETER_INDEX.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_parameter_index: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_prepared_statement,
            *mut idx_t,
            *const ::std::os::raw::c_char,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_BOOLEAN: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_boolean(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: bool,
) -> duckdb_state {
    let p = DUCKDB_BIND_BOOLEAN.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_boolean: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t, bool) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_INT8: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_int8(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: i8,
) -> duckdb_state {
    let p = DUCKDB_BIND_INT8.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_int8: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t, i8) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_INT16: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_int16(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: i16,
) -> duckdb_state {
    let p = DUCKDB_BIND_INT16.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_int16: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t, i16) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_INT32: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_int32(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: i32,
) -> duckdb_state {
    let p = DUCKDB_BIND_INT32.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_int32: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t, i32) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_INT64: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_int64(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: i64,
) -> duckdb_state {
    let p = DUCKDB_BIND_INT64.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_int64: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t, i64) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_HUGEINT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_hugeint(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: duckdb_hugeint,
) -> duckdb_state {
    let p = DUCKDB_BIND_HUGEINT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_hugeint: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_prepared_statement,
            idx_t,
            duckdb_hugeint,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_UHUGEINT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_uhugeint(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: duckdb_uhugeint,
) -> duckdb_state {
    let p = DUCKDB_BIND_UHUGEINT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_uhugeint: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_prepared_statement,
            idx_t,
            duckdb_uhugeint,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_DECIMAL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_decimal(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: duckdb_decimal,
) -> duckdb_state {
    let p = DUCKDB_BIND_DECIMAL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_decimal: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_prepared_statement,
            idx_t,
            duckdb_decimal,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_UINT8: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_uint8(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: u8,
) -> duckdb_state {
    let p = DUCKDB_BIND_UINT8.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_uint8: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t, u8) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_UINT16: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_uint16(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: u16,
) -> duckdb_state {
    let p = DUCKDB_BIND_UINT16.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_uint16: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t, u16) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_UINT32: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_uint32(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: u32,
) -> duckdb_state {
    let p = DUCKDB_BIND_UINT32.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_uint32: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t, u32) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_UINT64: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_uint64(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: u64,
) -> duckdb_state {
    let p = DUCKDB_BIND_UINT64.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_uint64: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t, u64) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_FLOAT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_float(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: f32,
) -> duckdb_state {
    let p = DUCKDB_BIND_FLOAT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_float: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t, f32) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_DOUBLE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_double(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: f64,
) -> duckdb_state {
    let p = DUCKDB_BIND_DOUBLE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_double: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t, f64) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_DATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_date(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: duckdb_date,
) -> duckdb_state {
    let p = DUCKDB_BIND_DATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_date: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t, duckdb_date) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_TIME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_time(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: duckdb_time,
) -> duckdb_state {
    let p = DUCKDB_BIND_TIME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_time: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t, duckdb_time) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_TIMESTAMP: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_timestamp(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: duckdb_timestamp,
) -> duckdb_state {
    let p = DUCKDB_BIND_TIMESTAMP.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_timestamp: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_prepared_statement,
            idx_t,
            duckdb_timestamp,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_TIMESTAMP_TZ: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_timestamp_tz(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: duckdb_timestamp,
) -> duckdb_state {
    let p = DUCKDB_BIND_TIMESTAMP_TZ.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_timestamp_tz: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_prepared_statement,
            idx_t,
            duckdb_timestamp,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_INTERVAL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_interval(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: duckdb_interval,
) -> duckdb_state {
    let p = DUCKDB_BIND_INTERVAL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_interval: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_prepared_statement,
            idx_t,
            duckdb_interval,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_VARCHAR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_varchar(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: *const ::std::os::raw::c_char,
) -> duckdb_state {
    let p = DUCKDB_BIND_VARCHAR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_varchar: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_prepared_statement,
            idx_t,
            *const ::std::os::raw::c_char,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_VARCHAR_LENGTH: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_varchar_length(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: *const ::std::os::raw::c_char,
    a3: idx_t,
) -> duckdb_state {
    let p = DUCKDB_BIND_VARCHAR_LENGTH.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_varchar_length: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_prepared_statement,
            idx_t,
            *const ::std::os::raw::c_char,
            idx_t,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2, a3)
    }
}
static DUCKDB_BIND_BLOB: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_blob(
    a0: duckdb_prepared_statement,
    a1: idx_t,
    a2: *const ::std::os::raw::c_void,
    a3: idx_t,
) -> duckdb_state {
    let p = DUCKDB_BIND_BLOB.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_blob: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_prepared_statement,
            idx_t,
            *const ::std::os::raw::c_void,
            idx_t,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2, a3)
    }
}
static DUCKDB_BIND_NULL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_null(
    a0: duckdb_prepared_statement,
    a1: idx_t,
) -> duckdb_state {
    let p = DUCKDB_BIND_NULL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_null: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_EXECUTE_PREPARED: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_execute_prepared(
    a0: duckdb_prepared_statement,
    a1: *mut duckdb_result,
) -> duckdb_state {
    let p = DUCKDB_EXECUTE_PREPARED.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_execute_prepared: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, *mut duckdb_result) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_EXTRACT_STATEMENTS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_extract_statements(
    a0: duckdb_connection,
    a1: *const ::std::os::raw::c_char,
    a2: *mut duckdb_extracted_statements,
) -> idx_t {
    let p = DUCKDB_EXTRACT_STATEMENTS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_extract_statements: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            *const ::std::os::raw::c_char,
            *mut duckdb_extracted_statements,
        ) -> idx_t = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_PREPARE_EXTRACTED_STATEMENT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_prepare_extracted_statement(
    a0: duckdb_connection,
    a1: duckdb_extracted_statements,
    a2: idx_t,
    a3: *mut duckdb_prepared_statement,
) -> duckdb_state {
    let p = DUCKDB_PREPARE_EXTRACTED_STATEMENT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_prepare_extracted_statement: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            duckdb_extracted_statements,
            idx_t,
            *mut duckdb_prepared_statement,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2, a3)
    }
}
static DUCKDB_EXTRACT_STATEMENTS_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_extract_statements_error(
    a0: duckdb_extracted_statements
) -> *const ::std::os::raw::c_char {
    let p = DUCKDB_EXTRACT_STATEMENTS_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_extract_statements_error: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_extracted_statements) -> *const ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DESTROY_EXTRACTED: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_extracted(a0: *mut duckdb_extracted_statements) {
    let p = DUCKDB_DESTROY_EXTRACTED.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_extracted: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_extracted_statements) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_PENDING_PREPARED: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_pending_prepared(
    a0: duckdb_prepared_statement,
    a1: *mut duckdb_pending_result,
) -> duckdb_state {
    let p = DUCKDB_PENDING_PREPARED.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_pending_prepared: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_prepared_statement,
            *mut duckdb_pending_result,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_DESTROY_PENDING: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_pending(a0: *mut duckdb_pending_result) {
    let p = DUCKDB_DESTROY_PENDING.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_pending: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_pending_result) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_PENDING_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_pending_error(a0: duckdb_pending_result) -> *const ::std::os::raw::c_char {
    let p = DUCKDB_PENDING_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_pending_error: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_pending_result) -> *const ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_PENDING_EXECUTE_TASK: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_pending_execute_task(a0: duckdb_pending_result) -> duckdb_pending_state {
    let p = DUCKDB_PENDING_EXECUTE_TASK.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_pending_execute_task: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_pending_result) -> duckdb_pending_state =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_PENDING_EXECUTE_CHECK_STATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_pending_execute_check_state(
    a0: duckdb_pending_result
) -> duckdb_pending_state {
    let p = DUCKDB_PENDING_EXECUTE_CHECK_STATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_pending_execute_check_state: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_pending_result) -> duckdb_pending_state =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_EXECUTE_PENDING: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_execute_pending(
    a0: duckdb_pending_result,
    a1: *mut duckdb_result,
) -> duckdb_state {
    let p = DUCKDB_EXECUTE_PENDING.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_execute_pending: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_pending_result, *mut duckdb_result) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_PENDING_EXECUTION_IS_FINISHED: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_pending_execution_is_finished(a0: duckdb_pending_state) -> bool {
    let p = DUCKDB_PENDING_EXECUTION_IS_FINISHED.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_pending_execution_is_finished: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_pending_state) -> bool = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DESTROY_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_value(a0: *mut duckdb_value) {
    let p = DUCKDB_DESTROY_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_value) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_VARCHAR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_varchar(a0: *const ::std::os::raw::c_char) -> duckdb_value {
    let p = DUCKDB_CREATE_VARCHAR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_varchar: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*const ::std::os::raw::c_char) -> duckdb_value =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_VARCHAR_LENGTH: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_varchar_length(
    a0: *const ::std::os::raw::c_char,
    a1: idx_t,
) -> duckdb_value {
    let p = DUCKDB_CREATE_VARCHAR_LENGTH.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_varchar_length: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*const ::std::os::raw::c_char, idx_t) -> duckdb_value =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_BOOL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_bool(a0: bool) -> duckdb_value {
    let p = DUCKDB_CREATE_BOOL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_bool: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(bool) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_INT8: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_int8(a0: i8) -> duckdb_value {
    let p = DUCKDB_CREATE_INT8.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_int8: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(i8) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_UINT8: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_uint8(a0: u8) -> duckdb_value {
    let p = DUCKDB_CREATE_UINT8.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_uint8: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(u8) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_INT16: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_int16(a0: i16) -> duckdb_value {
    let p = DUCKDB_CREATE_INT16.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_int16: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(i16) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_UINT16: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_uint16(a0: u16) -> duckdb_value {
    let p = DUCKDB_CREATE_UINT16.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_uint16: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(u16) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_INT32: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_int32(a0: i32) -> duckdb_value {
    let p = DUCKDB_CREATE_INT32.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_int32: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(i32) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_UINT32: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_uint32(a0: u32) -> duckdb_value {
    let p = DUCKDB_CREATE_UINT32.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_uint32: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(u32) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_UINT64: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_uint64(a0: u64) -> duckdb_value {
    let p = DUCKDB_CREATE_UINT64.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_uint64: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(u64) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_INT64: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_int64(a0: i64) -> duckdb_value {
    let p = DUCKDB_CREATE_INT64.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_int64: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(i64) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_HUGEINT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_hugeint(a0: duckdb_hugeint) -> duckdb_value {
    let p = DUCKDB_CREATE_HUGEINT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_hugeint: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_hugeint) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_UHUGEINT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_uhugeint(a0: duckdb_uhugeint) -> duckdb_value {
    let p = DUCKDB_CREATE_UHUGEINT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_uhugeint: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_uhugeint) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_FLOAT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_float(a0: f32) -> duckdb_value {
    let p = DUCKDB_CREATE_FLOAT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_float: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(f32) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_DOUBLE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_double(a0: f64) -> duckdb_value {
    let p = DUCKDB_CREATE_DOUBLE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_double: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(f64) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_DATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_date(a0: duckdb_date) -> duckdb_value {
    let p = DUCKDB_CREATE_DATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_date: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_date) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_TIME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_time(a0: duckdb_time) -> duckdb_value {
    let p = DUCKDB_CREATE_TIME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_time: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_time) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_TIME_TZ_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_time_tz_value(a0: duckdb_time_tz) -> duckdb_value {
    let p = DUCKDB_CREATE_TIME_TZ_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_time_tz_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_time_tz) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_TIMESTAMP: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_timestamp(a0: duckdb_timestamp) -> duckdb_value {
    let p = DUCKDB_CREATE_TIMESTAMP.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_timestamp: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_timestamp) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_INTERVAL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_interval(a0: duckdb_interval) -> duckdb_value {
    let p = DUCKDB_CREATE_INTERVAL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_interval: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_interval) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_BLOB: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_blob(
    a0: *const u8,
    a1: idx_t,
) -> duckdb_value {
    let p = DUCKDB_CREATE_BLOB.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_blob: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*const u8, idx_t) -> duckdb_value = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_BIGNUM: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_bignum(a0: duckdb_bignum) -> duckdb_value {
    let p = DUCKDB_CREATE_BIGNUM.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_bignum: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bignum) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_DECIMAL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_decimal(a0: duckdb_decimal) -> duckdb_value {
    let p = DUCKDB_CREATE_DECIMAL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_decimal: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_decimal) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_BIT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_bit(a0: duckdb_bit) -> duckdb_value {
    let p = DUCKDB_CREATE_BIT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_bit: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bit) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_UUID: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_uuid(a0: duckdb_uhugeint) -> duckdb_value {
    let p = DUCKDB_CREATE_UUID.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_uuid: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_uhugeint) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_BOOL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_bool(a0: duckdb_value) -> bool {
    let p = DUCKDB_GET_BOOL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_bool: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> bool = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_INT8: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_int8(a0: duckdb_value) -> i8 {
    let p = DUCKDB_GET_INT8.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_int8: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> i8 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_UINT8: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_uint8(a0: duckdb_value) -> u8 {
    let p = DUCKDB_GET_UINT8.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_uint8: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> u8 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_INT16: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_int16(a0: duckdb_value) -> i16 {
    let p = DUCKDB_GET_INT16.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_int16: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> i16 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_UINT16: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_uint16(a0: duckdb_value) -> u16 {
    let p = DUCKDB_GET_UINT16.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_uint16: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> u16 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_INT32: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_int32(a0: duckdb_value) -> i32 {
    let p = DUCKDB_GET_INT32.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_int32: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> i32 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_UINT32: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_uint32(a0: duckdb_value) -> u32 {
    let p = DUCKDB_GET_UINT32.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_uint32: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> u32 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_INT64: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_int64(a0: duckdb_value) -> i64 {
    let p = DUCKDB_GET_INT64.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_int64: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> i64 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_UINT64: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_uint64(a0: duckdb_value) -> u64 {
    let p = DUCKDB_GET_UINT64.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_uint64: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> u64 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_HUGEINT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_hugeint(a0: duckdb_value) -> duckdb_hugeint {
    let p = DUCKDB_GET_HUGEINT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_hugeint: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_hugeint = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_UHUGEINT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_uhugeint(a0: duckdb_value) -> duckdb_uhugeint {
    let p = DUCKDB_GET_UHUGEINT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_uhugeint: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_uhugeint = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_FLOAT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_float(a0: duckdb_value) -> f32 {
    let p = DUCKDB_GET_FLOAT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_float: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> f32 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_DOUBLE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_double(a0: duckdb_value) -> f64 {
    let p = DUCKDB_GET_DOUBLE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_double: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> f64 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_DATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_date(a0: duckdb_value) -> duckdb_date {
    let p = DUCKDB_GET_DATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_date: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_date = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_TIME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_time(a0: duckdb_value) -> duckdb_time {
    let p = DUCKDB_GET_TIME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_time: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_time = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_TIME_TZ: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_time_tz(a0: duckdb_value) -> duckdb_time_tz {
    let p = DUCKDB_GET_TIME_TZ.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_time_tz: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_time_tz = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_TIMESTAMP: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_timestamp(a0: duckdb_value) -> duckdb_timestamp {
    let p = DUCKDB_GET_TIMESTAMP.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_timestamp: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_timestamp = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_INTERVAL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_interval(a0: duckdb_value) -> duckdb_interval {
    let p = DUCKDB_GET_INTERVAL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_interval: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_interval = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_VALUE_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_value_type(a0: duckdb_value) -> duckdb_logical_type {
    let p = DUCKDB_GET_VALUE_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_value_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_logical_type = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_BLOB: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_blob(a0: duckdb_value) -> duckdb_blob {
    let p = DUCKDB_GET_BLOB.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_blob: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_blob = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_BIGNUM: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_bignum(a0: duckdb_value) -> duckdb_bignum {
    let p = DUCKDB_GET_BIGNUM.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_bignum: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_bignum = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_DECIMAL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_decimal(a0: duckdb_value) -> duckdb_decimal {
    let p = DUCKDB_GET_DECIMAL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_decimal: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_decimal = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_BIT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_bit(a0: duckdb_value) -> duckdb_bit {
    let p = DUCKDB_GET_BIT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_bit: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_bit = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_UUID: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_uuid(a0: duckdb_value) -> duckdb_uhugeint {
    let p = DUCKDB_GET_UUID.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_uuid: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_uhugeint = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_VARCHAR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_varchar(a0: duckdb_value) -> *mut ::std::os::raw::c_char {
    let p = DUCKDB_GET_VARCHAR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_varchar: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> *mut ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_STRUCT_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_struct_value(
    a0: duckdb_logical_type,
    a1: *mut duckdb_value,
) -> duckdb_value {
    let p = DUCKDB_CREATE_STRUCT_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_struct_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type, *mut duckdb_value) -> duckdb_value =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_LIST_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_list_value(
    a0: duckdb_logical_type,
    a1: *mut duckdb_value,
    a2: idx_t,
) -> duckdb_value {
    let p = DUCKDB_CREATE_LIST_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_list_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type, *mut duckdb_value, idx_t) -> duckdb_value =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_CREATE_ARRAY_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_array_value(
    a0: duckdb_logical_type,
    a1: *mut duckdb_value,
    a2: idx_t,
) -> duckdb_value {
    let p = DUCKDB_CREATE_ARRAY_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_array_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type, *mut duckdb_value, idx_t) -> duckdb_value =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_GET_MAP_SIZE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_map_size(a0: duckdb_value) -> idx_t {
    let p = DUCKDB_GET_MAP_SIZE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_map_size: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_MAP_KEY: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_map_key(
    a0: duckdb_value,
    a1: idx_t,
) -> duckdb_value {
    let p = DUCKDB_GET_MAP_KEY.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_map_key: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value, idx_t) -> duckdb_value = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_GET_MAP_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_map_value(
    a0: duckdb_value,
    a1: idx_t,
) -> duckdb_value {
    let p = DUCKDB_GET_MAP_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_map_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value, idx_t) -> duckdb_value = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_IS_NULL_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_is_null_value(a0: duckdb_value) -> bool {
    let p = DUCKDB_IS_NULL_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_is_null_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> bool = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_NULL_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_null_value() -> duckdb_value {
    let p = DUCKDB_CREATE_NULL_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_null_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn() -> duckdb_value = ::std::mem::transmute(p);
        f()
    }
}
static DUCKDB_GET_LIST_SIZE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_list_size(a0: duckdb_value) -> idx_t {
    let p = DUCKDB_GET_LIST_SIZE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_list_size: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_LIST_CHILD: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_list_child(
    a0: duckdb_value,
    a1: idx_t,
) -> duckdb_value {
    let p = DUCKDB_GET_LIST_CHILD.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_list_child: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value, idx_t) -> duckdb_value = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_ENUM_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_enum_value(
    a0: duckdb_logical_type,
    a1: u64,
) -> duckdb_value {
    let p = DUCKDB_CREATE_ENUM_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_enum_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type, u64) -> duckdb_value =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_GET_ENUM_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_enum_value(a0: duckdb_value) -> u64 {
    let p = DUCKDB_GET_ENUM_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_enum_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> u64 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_STRUCT_CHILD: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_struct_child(
    a0: duckdb_value,
    a1: idx_t,
) -> duckdb_value {
    let p = DUCKDB_GET_STRUCT_CHILD.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_struct_child: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value, idx_t) -> duckdb_value = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_LOGICAL_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_logical_type(a0: duckdb_type) -> duckdb_logical_type {
    let p = DUCKDB_CREATE_LOGICAL_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_logical_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_type) -> duckdb_logical_type = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_LOGICAL_TYPE_GET_ALIAS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_logical_type_get_alias(
    a0: duckdb_logical_type
) -> *mut ::std::os::raw::c_char {
    let p = DUCKDB_LOGICAL_TYPE_GET_ALIAS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_logical_type_get_alias: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type) -> *mut ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_LOGICAL_TYPE_SET_ALIAS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_logical_type_set_alias(
    a0: duckdb_logical_type,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_LOGICAL_TYPE_SET_ALIAS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_logical_type_set_alias: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_LIST_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_list_type(a0: duckdb_logical_type) -> duckdb_logical_type {
    let p = DUCKDB_CREATE_LIST_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_list_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_ARRAY_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_array_type(
    a0: duckdb_logical_type,
    a1: idx_t,
) -> duckdb_logical_type {
    let p = DUCKDB_CREATE_ARRAY_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_array_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type, idx_t) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_MAP_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_map_type(
    a0: duckdb_logical_type,
    a1: duckdb_logical_type,
) -> duckdb_logical_type {
    let p = DUCKDB_CREATE_MAP_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_map_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_logical_type,
            duckdb_logical_type,
        ) -> duckdb_logical_type = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_UNION_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_union_type(
    a0: *mut duckdb_logical_type,
    a1: *mut *const ::std::os::raw::c_char,
    a2: idx_t,
) -> duckdb_logical_type {
    let p = DUCKDB_CREATE_UNION_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_union_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            *mut duckdb_logical_type,
            *mut *const ::std::os::raw::c_char,
            idx_t,
        ) -> duckdb_logical_type = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_CREATE_STRUCT_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_struct_type(
    a0: *mut duckdb_logical_type,
    a1: *mut *const ::std::os::raw::c_char,
    a2: idx_t,
) -> duckdb_logical_type {
    let p = DUCKDB_CREATE_STRUCT_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_struct_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            *mut duckdb_logical_type,
            *mut *const ::std::os::raw::c_char,
            idx_t,
        ) -> duckdb_logical_type = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_CREATE_ENUM_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_enum_type(
    a0: *mut *const ::std::os::raw::c_char,
    a1: idx_t,
) -> duckdb_logical_type {
    let p = DUCKDB_CREATE_ENUM_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_enum_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            *mut *const ::std::os::raw::c_char,
            idx_t,
        ) -> duckdb_logical_type = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_DECIMAL_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_decimal_type(
    a0: u8,
    a1: u8,
) -> duckdb_logical_type {
    let p = DUCKDB_CREATE_DECIMAL_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_decimal_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(u8, u8) -> duckdb_logical_type = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_GET_TYPE_ID: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_type_id(a0: duckdb_logical_type) -> duckdb_type {
    let p = DUCKDB_GET_TYPE_ID.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_type_id: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type) -> duckdb_type = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DECIMAL_WIDTH: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_decimal_width(a0: duckdb_logical_type) -> u8 {
    let p = DUCKDB_DECIMAL_WIDTH.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_decimal_width: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type) -> u8 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DECIMAL_SCALE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_decimal_scale(a0: duckdb_logical_type) -> u8 {
    let p = DUCKDB_DECIMAL_SCALE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_decimal_scale: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type) -> u8 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DECIMAL_INTERNAL_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_decimal_internal_type(a0: duckdb_logical_type) -> duckdb_type {
    let p = DUCKDB_DECIMAL_INTERNAL_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_decimal_internal_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type) -> duckdb_type = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_ENUM_INTERNAL_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_enum_internal_type(a0: duckdb_logical_type) -> duckdb_type {
    let p = DUCKDB_ENUM_INTERNAL_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_enum_internal_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type) -> duckdb_type = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_ENUM_DICTIONARY_SIZE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_enum_dictionary_size(a0: duckdb_logical_type) -> u32 {
    let p = DUCKDB_ENUM_DICTIONARY_SIZE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_enum_dictionary_size: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type) -> u32 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_ENUM_DICTIONARY_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_enum_dictionary_value(
    a0: duckdb_logical_type,
    a1: idx_t,
) -> *mut ::std::os::raw::c_char {
    let p = DUCKDB_ENUM_DICTIONARY_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_enum_dictionary_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type, idx_t) -> *mut ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_LIST_TYPE_CHILD_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_list_type_child_type(a0: duckdb_logical_type) -> duckdb_logical_type {
    let p = DUCKDB_LIST_TYPE_CHILD_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_list_type_child_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_ARRAY_TYPE_CHILD_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_array_type_child_type(a0: duckdb_logical_type) -> duckdb_logical_type {
    let p = DUCKDB_ARRAY_TYPE_CHILD_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_array_type_child_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_ARRAY_TYPE_ARRAY_SIZE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_array_type_array_size(a0: duckdb_logical_type) -> idx_t {
    let p = DUCKDB_ARRAY_TYPE_ARRAY_SIZE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_array_type_array_size: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_MAP_TYPE_KEY_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_map_type_key_type(a0: duckdb_logical_type) -> duckdb_logical_type {
    let p = DUCKDB_MAP_TYPE_KEY_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_map_type_key_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_MAP_TYPE_VALUE_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_map_type_value_type(a0: duckdb_logical_type) -> duckdb_logical_type {
    let p = DUCKDB_MAP_TYPE_VALUE_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_map_type_value_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_STRUCT_TYPE_CHILD_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_struct_type_child_count(a0: duckdb_logical_type) -> idx_t {
    let p = DUCKDB_STRUCT_TYPE_CHILD_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_struct_type_child_count: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_STRUCT_TYPE_CHILD_NAME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_struct_type_child_name(
    a0: duckdb_logical_type,
    a1: idx_t,
) -> *mut ::std::os::raw::c_char {
    let p = DUCKDB_STRUCT_TYPE_CHILD_NAME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_struct_type_child_name: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type, idx_t) -> *mut ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_STRUCT_TYPE_CHILD_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_struct_type_child_type(
    a0: duckdb_logical_type,
    a1: idx_t,
) -> duckdb_logical_type {
    let p = DUCKDB_STRUCT_TYPE_CHILD_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_struct_type_child_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type, idx_t) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_UNION_TYPE_MEMBER_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_union_type_member_count(a0: duckdb_logical_type) -> idx_t {
    let p = DUCKDB_UNION_TYPE_MEMBER_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_union_type_member_count: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_UNION_TYPE_MEMBER_NAME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_union_type_member_name(
    a0: duckdb_logical_type,
    a1: idx_t,
) -> *mut ::std::os::raw::c_char {
    let p = DUCKDB_UNION_TYPE_MEMBER_NAME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_union_type_member_name: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type, idx_t) -> *mut ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_UNION_TYPE_MEMBER_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_union_type_member_type(
    a0: duckdb_logical_type,
    a1: idx_t,
) -> duckdb_logical_type {
    let p = DUCKDB_UNION_TYPE_MEMBER_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_union_type_member_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type, idx_t) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_DESTROY_LOGICAL_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_logical_type(a0: *mut duckdb_logical_type) {
    let p = DUCKDB_DESTROY_LOGICAL_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_logical_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_logical_type) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_REGISTER_LOGICAL_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_register_logical_type(
    a0: duckdb_connection,
    a1: duckdb_logical_type,
    a2: duckdb_create_type_info,
) -> duckdb_state {
    let p = DUCKDB_REGISTER_LOGICAL_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_register_logical_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            duckdb_logical_type,
            duckdb_create_type_info,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_CREATE_DATA_CHUNK: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_data_chunk(
    a0: *mut duckdb_logical_type,
    a1: idx_t,
) -> duckdb_data_chunk {
    let p = DUCKDB_CREATE_DATA_CHUNK.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_data_chunk: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_logical_type, idx_t) -> duckdb_data_chunk =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_DESTROY_DATA_CHUNK: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_data_chunk(a0: *mut duckdb_data_chunk) {
    let p = DUCKDB_DESTROY_DATA_CHUNK.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_data_chunk: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_data_chunk) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DATA_CHUNK_RESET: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_data_chunk_reset(a0: duckdb_data_chunk) {
    let p = DUCKDB_DATA_CHUNK_RESET.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_data_chunk_reset: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_data_chunk) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DATA_CHUNK_GET_COLUMN_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_data_chunk_get_column_count(a0: duckdb_data_chunk) -> idx_t {
    let p = DUCKDB_DATA_CHUNK_GET_COLUMN_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_data_chunk_get_column_count: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_data_chunk) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DATA_CHUNK_GET_VECTOR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_data_chunk_get_vector(
    a0: duckdb_data_chunk,
    a1: idx_t,
) -> duckdb_vector {
    let p = DUCKDB_DATA_CHUNK_GET_VECTOR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_data_chunk_get_vector: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_data_chunk, idx_t) -> duckdb_vector =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_DATA_CHUNK_GET_SIZE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_data_chunk_get_size(a0: duckdb_data_chunk) -> idx_t {
    let p = DUCKDB_DATA_CHUNK_GET_SIZE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_data_chunk_get_size: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_data_chunk) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DATA_CHUNK_SET_SIZE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_data_chunk_set_size(
    a0: duckdb_data_chunk,
    a1: idx_t,
) {
    let p = DUCKDB_DATA_CHUNK_SET_SIZE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_data_chunk_set_size: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_data_chunk, idx_t) = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_VECTOR_GET_COLUMN_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_vector_get_column_type(a0: duckdb_vector) -> duckdb_logical_type {
    let p = DUCKDB_VECTOR_GET_COLUMN_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_vector_get_column_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_vector) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_VECTOR_GET_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_vector_get_data(a0: duckdb_vector) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_VECTOR_GET_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_vector_get_data: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_vector) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_VECTOR_GET_VALIDITY: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_vector_get_validity(a0: duckdb_vector) -> *mut u64 {
    let p = DUCKDB_VECTOR_GET_VALIDITY.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_vector_get_validity: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_vector) -> *mut u64 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_VECTOR_ENSURE_VALIDITY_WRITABLE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_vector_ensure_validity_writable(a0: duckdb_vector) {
    let p = DUCKDB_VECTOR_ENSURE_VALIDITY_WRITABLE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_vector_ensure_validity_writable: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_vector) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_VECTOR_ASSIGN_STRING_ELEMENT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_vector_assign_string_element(
    a0: duckdb_vector,
    a1: idx_t,
    a2: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_VECTOR_ASSIGN_STRING_ELEMENT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_vector_assign_string_element: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_vector, idx_t, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VECTOR_ASSIGN_STRING_ELEMENT_LEN: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_vector_assign_string_element_len(
    a0: duckdb_vector,
    a1: idx_t,
    a2: *const ::std::os::raw::c_char,
    a3: idx_t,
) {
    let p = DUCKDB_VECTOR_ASSIGN_STRING_ELEMENT_LEN.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_vector_assign_string_element_len: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_vector, idx_t, *const ::std::os::raw::c_char, idx_t) =
            ::std::mem::transmute(p);
        f(a0, a1, a2, a3)
    }
}
static DUCKDB_LIST_VECTOR_GET_CHILD: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_list_vector_get_child(a0: duckdb_vector) -> duckdb_vector {
    let p = DUCKDB_LIST_VECTOR_GET_CHILD.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_list_vector_get_child: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_vector) -> duckdb_vector = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_LIST_VECTOR_GET_SIZE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_list_vector_get_size(a0: duckdb_vector) -> idx_t {
    let p = DUCKDB_LIST_VECTOR_GET_SIZE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_list_vector_get_size: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_vector) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_LIST_VECTOR_SET_SIZE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_list_vector_set_size(
    a0: duckdb_vector,
    a1: idx_t,
) -> duckdb_state {
    let p = DUCKDB_LIST_VECTOR_SET_SIZE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_list_vector_set_size: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_vector, idx_t) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_LIST_VECTOR_RESERVE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_list_vector_reserve(
    a0: duckdb_vector,
    a1: idx_t,
) -> duckdb_state {
    let p = DUCKDB_LIST_VECTOR_RESERVE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_list_vector_reserve: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_vector, idx_t) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_STRUCT_VECTOR_GET_CHILD: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_struct_vector_get_child(
    a0: duckdb_vector,
    a1: idx_t,
) -> duckdb_vector {
    let p = DUCKDB_STRUCT_VECTOR_GET_CHILD.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_struct_vector_get_child: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_vector, idx_t) -> duckdb_vector =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_ARRAY_VECTOR_GET_CHILD: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_array_vector_get_child(a0: duckdb_vector) -> duckdb_vector {
    let p = DUCKDB_ARRAY_VECTOR_GET_CHILD.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_array_vector_get_child: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_vector) -> duckdb_vector = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_VALIDITY_ROW_IS_VALID: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_validity_row_is_valid(
    a0: *mut u64,
    a1: idx_t,
) -> bool {
    let p = DUCKDB_VALIDITY_ROW_IS_VALID.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_validity_row_is_valid: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut u64, idx_t) -> bool = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_VALIDITY_SET_ROW_VALIDITY: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_validity_set_row_validity(
    a0: *mut u64,
    a1: idx_t,
    a2: bool,
) {
    let p = DUCKDB_VALIDITY_SET_ROW_VALIDITY.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_validity_set_row_validity: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut u64, idx_t, bool) = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALIDITY_SET_ROW_INVALID: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_validity_set_row_invalid(
    a0: *mut u64,
    a1: idx_t,
) {
    let p = DUCKDB_VALIDITY_SET_ROW_INVALID.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_validity_set_row_invalid: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut u64, idx_t) = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_VALIDITY_SET_ROW_VALID: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_validity_set_row_valid(
    a0: *mut u64,
    a1: idx_t,
) {
    let p = DUCKDB_VALIDITY_SET_ROW_VALID.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_validity_set_row_valid: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut u64, idx_t) = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_SCALAR_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_scalar_function() -> duckdb_scalar_function {
    let p = DUCKDB_CREATE_SCALAR_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_scalar_function: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn() -> duckdb_scalar_function = ::std::mem::transmute(p);
        f()
    }
}
static DUCKDB_DESTROY_SCALAR_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_scalar_function(a0: *mut duckdb_scalar_function) {
    let p = DUCKDB_DESTROY_SCALAR_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_scalar_function: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_scalar_function) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_SCALAR_FUNCTION_SET_NAME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_set_name(
    a0: duckdb_scalar_function,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_SCALAR_FUNCTION_SET_NAME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_scalar_function_set_name: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_scalar_function, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_SCALAR_FUNCTION_SET_VARARGS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_set_varargs(
    a0: duckdb_scalar_function,
    a1: duckdb_logical_type,
) {
    let p = DUCKDB_SCALAR_FUNCTION_SET_VARARGS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_set_varargs: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_scalar_function, duckdb_logical_type) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_SCALAR_FUNCTION_SET_SPECIAL_HANDLING: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_set_special_handling(a0: duckdb_scalar_function) {
    let p =
        DUCKDB_SCALAR_FUNCTION_SET_SPECIAL_HANDLING.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_set_special_handling: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_scalar_function) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_SCALAR_FUNCTION_SET_VOLATILE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_set_volatile(a0: duckdb_scalar_function) {
    let p = DUCKDB_SCALAR_FUNCTION_SET_VOLATILE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_set_volatile: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_scalar_function) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_SCALAR_FUNCTION_ADD_PARAMETER: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_add_parameter(
    a0: duckdb_scalar_function,
    a1: duckdb_logical_type,
) {
    let p = DUCKDB_SCALAR_FUNCTION_ADD_PARAMETER.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_add_parameter: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_scalar_function, duckdb_logical_type) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_SCALAR_FUNCTION_SET_RETURN_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_set_return_type(
    a0: duckdb_scalar_function,
    a1: duckdb_logical_type,
) {
    let p = DUCKDB_SCALAR_FUNCTION_SET_RETURN_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_set_return_type: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_scalar_function, duckdb_logical_type) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_SCALAR_FUNCTION_SET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_set_extra_info(
    a0: duckdb_scalar_function,
    a1: *mut ::std::os::raw::c_void,
    a2: duckdb_delete_callback_t,
) {
    let p = DUCKDB_SCALAR_FUNCTION_SET_EXTRA_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_set_extra_info: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_scalar_function,
            *mut ::std::os::raw::c_void,
            duckdb_delete_callback_t,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_SCALAR_FUNCTION_SET_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_set_function(
    a0: duckdb_scalar_function,
    a1: duckdb_scalar_function_t,
) {
    let p = DUCKDB_SCALAR_FUNCTION_SET_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_set_function: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_scalar_function, duckdb_scalar_function_t) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_REGISTER_SCALAR_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_register_scalar_function(
    a0: duckdb_connection,
    a1: duckdb_scalar_function,
) -> duckdb_state {
    let p = DUCKDB_REGISTER_SCALAR_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_register_scalar_function: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_connection, duckdb_scalar_function) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_SCALAR_FUNCTION_GET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_get_extra_info(
    a0: duckdb_function_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_SCALAR_FUNCTION_GET_EXTRA_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_get_extra_info: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_function_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_SCALAR_FUNCTION_SET_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_set_error(
    a0: duckdb_function_info,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_SCALAR_FUNCTION_SET_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_scalar_function_set_error: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_function_info, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_SCALAR_FUNCTION_SET: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_scalar_function_set(
    a0: *const ::std::os::raw::c_char
) -> duckdb_scalar_function_set {
    let p = DUCKDB_CREATE_SCALAR_FUNCTION_SET.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_create_scalar_function_set: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(*const ::std::os::raw::c_char) -> duckdb_scalar_function_set =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DESTROY_SCALAR_FUNCTION_SET: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_scalar_function_set(a0: *mut duckdb_scalar_function_set) {
    let p = DUCKDB_DESTROY_SCALAR_FUNCTION_SET.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_destroy_scalar_function_set: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_scalar_function_set) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_ADD_SCALAR_FUNCTION_TO_SET: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_add_scalar_function_to_set(
    a0: duckdb_scalar_function_set,
    a1: duckdb_scalar_function,
) -> duckdb_state {
    let p = DUCKDB_ADD_SCALAR_FUNCTION_TO_SET.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_add_scalar_function_to_set: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_scalar_function_set,
            duckdb_scalar_function,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_REGISTER_SCALAR_FUNCTION_SET: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_register_scalar_function_set(
    a0: duckdb_connection,
    a1: duckdb_scalar_function_set,
) -> duckdb_state {
    let p = DUCKDB_REGISTER_SCALAR_FUNCTION_SET.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_register_scalar_function_set: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_connection, duckdb_scalar_function_set) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_AGGREGATE_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_aggregate_function() -> duckdb_aggregate_function {
    let p = DUCKDB_CREATE_AGGREGATE_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_aggregate_function: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn() -> duckdb_aggregate_function = ::std::mem::transmute(p);
        f()
    }
}
static DUCKDB_DESTROY_AGGREGATE_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_aggregate_function(a0: *mut duckdb_aggregate_function) {
    let p = DUCKDB_DESTROY_AGGREGATE_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_destroy_aggregate_function: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_aggregate_function) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_AGGREGATE_FUNCTION_SET_NAME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_aggregate_function_set_name(
    a0: duckdb_aggregate_function,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_AGGREGATE_FUNCTION_SET_NAME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_aggregate_function_set_name: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_aggregate_function, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_AGGREGATE_FUNCTION_ADD_PARAMETER: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_aggregate_function_add_parameter(
    a0: duckdb_aggregate_function,
    a1: duckdb_logical_type,
) {
    let p = DUCKDB_AGGREGATE_FUNCTION_ADD_PARAMETER.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_aggregate_function_add_parameter: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_aggregate_function, duckdb_logical_type) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_AGGREGATE_FUNCTION_SET_RETURN_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_aggregate_function_set_return_type(
    a0: duckdb_aggregate_function,
    a1: duckdb_logical_type,
) {
    let p = DUCKDB_AGGREGATE_FUNCTION_SET_RETURN_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_aggregate_function_set_return_type: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_aggregate_function, duckdb_logical_type) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_AGGREGATE_FUNCTION_SET_FUNCTIONS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_aggregate_function_set_functions(
    a0: duckdb_aggregate_function,
    a1: duckdb_aggregate_state_size,
    a2: duckdb_aggregate_init_t,
    a3: duckdb_aggregate_update_t,
    a4: duckdb_aggregate_combine_t,
    a5: duckdb_aggregate_finalize_t,
) {
    let p = DUCKDB_AGGREGATE_FUNCTION_SET_FUNCTIONS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_aggregate_function_set_functions: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_aggregate_function,
            duckdb_aggregate_state_size,
            duckdb_aggregate_init_t,
            duckdb_aggregate_update_t,
            duckdb_aggregate_combine_t,
            duckdb_aggregate_finalize_t,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2, a3, a4, a5)
    }
}
static DUCKDB_AGGREGATE_FUNCTION_SET_DESTRUCTOR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_aggregate_function_set_destructor(
    a0: duckdb_aggregate_function,
    a1: duckdb_aggregate_destroy_t,
) {
    let p = DUCKDB_AGGREGATE_FUNCTION_SET_DESTRUCTOR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_aggregate_function_set_destructor: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_aggregate_function, duckdb_aggregate_destroy_t) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_REGISTER_AGGREGATE_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_register_aggregate_function(
    a0: duckdb_connection,
    a1: duckdb_aggregate_function,
) -> duckdb_state {
    let p = DUCKDB_REGISTER_AGGREGATE_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_register_aggregate_function: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_connection, duckdb_aggregate_function) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_AGGREGATE_FUNCTION_SET_SPECIAL_HANDLING: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_aggregate_function_set_special_handling(a0: duckdb_aggregate_function) {
    let p =
        DUCKDB_AGGREGATE_FUNCTION_SET_SPECIAL_HANDLING.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_aggregate_function_set_special_handling: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_aggregate_function) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_AGGREGATE_FUNCTION_SET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_aggregate_function_set_extra_info(
    a0: duckdb_aggregate_function,
    a1: *mut ::std::os::raw::c_void,
    a2: duckdb_delete_callback_t,
) {
    let p = DUCKDB_AGGREGATE_FUNCTION_SET_EXTRA_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_aggregate_function_set_extra_info: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_aggregate_function,
            *mut ::std::os::raw::c_void,
            duckdb_delete_callback_t,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_AGGREGATE_FUNCTION_GET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_aggregate_function_get_extra_info(
    a0: duckdb_function_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_AGGREGATE_FUNCTION_GET_EXTRA_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_aggregate_function_get_extra_info: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_function_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_AGGREGATE_FUNCTION_SET_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_aggregate_function_set_error(
    a0: duckdb_function_info,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_AGGREGATE_FUNCTION_SET_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_aggregate_function_set_error: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_function_info, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_AGGREGATE_FUNCTION_SET: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_aggregate_function_set(
    a0: *const ::std::os::raw::c_char
) -> duckdb_aggregate_function_set {
    let p = DUCKDB_CREATE_AGGREGATE_FUNCTION_SET.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_create_aggregate_function_set: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            *const ::std::os::raw::c_char,
        ) -> duckdb_aggregate_function_set = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DESTROY_AGGREGATE_FUNCTION_SET: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_aggregate_function_set(a0: *mut duckdb_aggregate_function_set) {
    let p = DUCKDB_DESTROY_AGGREGATE_FUNCTION_SET.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_destroy_aggregate_function_set: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_aggregate_function_set) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_ADD_AGGREGATE_FUNCTION_TO_SET: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_add_aggregate_function_to_set(
    a0: duckdb_aggregate_function_set,
    a1: duckdb_aggregate_function,
) -> duckdb_state {
    let p = DUCKDB_ADD_AGGREGATE_FUNCTION_TO_SET.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_add_aggregate_function_to_set: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_aggregate_function_set,
            duckdb_aggregate_function,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_REGISTER_AGGREGATE_FUNCTION_SET: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_register_aggregate_function_set(
    a0: duckdb_connection,
    a1: duckdb_aggregate_function_set,
) -> duckdb_state {
    let p = DUCKDB_REGISTER_AGGREGATE_FUNCTION_SET.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_register_aggregate_function_set: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            duckdb_aggregate_function_set,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_TABLE_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_table_function() -> duckdb_table_function {
    let p = DUCKDB_CREATE_TABLE_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_table_function: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn() -> duckdb_table_function = ::std::mem::transmute(p);
        f()
    }
}
static DUCKDB_DESTROY_TABLE_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_table_function(a0: *mut duckdb_table_function) {
    let p = DUCKDB_DESTROY_TABLE_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_table_function: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_table_function) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_TABLE_FUNCTION_SET_NAME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_function_set_name(
    a0: duckdb_table_function,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_TABLE_FUNCTION_SET_NAME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_table_function_set_name: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_table_function, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_TABLE_FUNCTION_ADD_PARAMETER: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_function_add_parameter(
    a0: duckdb_table_function,
    a1: duckdb_logical_type,
) {
    let p = DUCKDB_TABLE_FUNCTION_ADD_PARAMETER.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_table_function_add_parameter: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_table_function, duckdb_logical_type) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_TABLE_FUNCTION_ADD_NAMED_PARAMETER: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_function_add_named_parameter(
    a0: duckdb_table_function,
    a1: *const ::std::os::raw::c_char,
    a2: duckdb_logical_type,
) {
    let p = DUCKDB_TABLE_FUNCTION_ADD_NAMED_PARAMETER.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_table_function_add_named_parameter: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_table_function,
            *const ::std::os::raw::c_char,
            duckdb_logical_type,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_TABLE_FUNCTION_SET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_function_set_extra_info(
    a0: duckdb_table_function,
    a1: *mut ::std::os::raw::c_void,
    a2: duckdb_delete_callback_t,
) {
    let p = DUCKDB_TABLE_FUNCTION_SET_EXTRA_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_table_function_set_extra_info: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_table_function,
            *mut ::std::os::raw::c_void,
            duckdb_delete_callback_t,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_TABLE_FUNCTION_SET_BIND: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_function_set_bind(
    a0: duckdb_table_function,
    a1: duckdb_table_function_bind_t,
) {
    let p = DUCKDB_TABLE_FUNCTION_SET_BIND.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_table_function_set_bind: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_table_function, duckdb_table_function_bind_t) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_TABLE_FUNCTION_SET_INIT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_function_set_init(
    a0: duckdb_table_function,
    a1: duckdb_table_function_init_t,
) {
    let p = DUCKDB_TABLE_FUNCTION_SET_INIT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_table_function_set_init: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_table_function, duckdb_table_function_init_t) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_TABLE_FUNCTION_SET_LOCAL_INIT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_function_set_local_init(
    a0: duckdb_table_function,
    a1: duckdb_table_function_init_t,
) {
    let p = DUCKDB_TABLE_FUNCTION_SET_LOCAL_INIT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_table_function_set_local_init: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_table_function, duckdb_table_function_init_t) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_TABLE_FUNCTION_SET_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_function_set_function(
    a0: duckdb_table_function,
    a1: duckdb_table_function_t,
) {
    let p = DUCKDB_TABLE_FUNCTION_SET_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_table_function_set_function: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_table_function, duckdb_table_function_t) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_TABLE_FUNCTION_SUPPORTS_PROJECTION_PUSHDOWN: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_function_supports_projection_pushdown(
    a0: duckdb_table_function,
    a1: bool,
) {
    let p = DUCKDB_TABLE_FUNCTION_SUPPORTS_PROJECTION_PUSHDOWN
        .load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_table_function_supports_projection_pushdown: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_table_function, bool) = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_REGISTER_TABLE_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_register_table_function(
    a0: duckdb_connection,
    a1: duckdb_table_function,
) -> duckdb_state {
    let p = DUCKDB_REGISTER_TABLE_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_register_table_function: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_connection, duckdb_table_function) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_BIND_GET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_get_extra_info(a0: duckdb_bind_info) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_BIND_GET_EXTRA_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_get_extra_info: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bind_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_BIND_ADD_RESULT_COLUMN: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_add_result_column(
    a0: duckdb_bind_info,
    a1: *const ::std::os::raw::c_char,
    a2: duckdb_logical_type,
) {
    let p = DUCKDB_BIND_ADD_RESULT_COLUMN.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_add_result_column: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_bind_info,
            *const ::std::os::raw::c_char,
            duckdb_logical_type,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_GET_PARAMETER_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_get_parameter_count(a0: duckdb_bind_info) -> idx_t {
    let p = DUCKDB_BIND_GET_PARAMETER_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_get_parameter_count: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bind_info) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_BIND_GET_PARAMETER: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_get_parameter(
    a0: duckdb_bind_info,
    a1: idx_t,
) -> duckdb_value {
    let p = DUCKDB_BIND_GET_PARAMETER.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_get_parameter: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bind_info, idx_t) -> duckdb_value =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_BIND_GET_NAMED_PARAMETER: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_get_named_parameter(
    a0: duckdb_bind_info,
    a1: *const ::std::os::raw::c_char,
) -> duckdb_value {
    let p = DUCKDB_BIND_GET_NAMED_PARAMETER.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_get_named_parameter: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_bind_info,
            *const ::std::os::raw::c_char,
        ) -> duckdb_value = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_BIND_SET_BIND_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_set_bind_data(
    a0: duckdb_bind_info,
    a1: *mut ::std::os::raw::c_void,
    a2: duckdb_delete_callback_t,
) {
    let p = DUCKDB_BIND_SET_BIND_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_set_bind_data: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_bind_info,
            *mut ::std::os::raw::c_void,
            duckdb_delete_callback_t,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_SET_CARDINALITY: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_set_cardinality(
    a0: duckdb_bind_info,
    a1: idx_t,
    a2: bool,
) {
    let p = DUCKDB_BIND_SET_CARDINALITY.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_set_cardinality: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bind_info, idx_t, bool) = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_BIND_SET_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_bind_set_error(
    a0: duckdb_bind_info,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_BIND_SET_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_bind_set_error: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bind_info, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_INIT_GET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_init_get_extra_info(a0: duckdb_init_info) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_INIT_GET_EXTRA_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_init_get_extra_info: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_init_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_INIT_GET_BIND_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_init_get_bind_data(a0: duckdb_init_info) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_INIT_GET_BIND_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_init_get_bind_data: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_init_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_INIT_SET_INIT_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_init_set_init_data(
    a0: duckdb_init_info,
    a1: *mut ::std::os::raw::c_void,
    a2: duckdb_delete_callback_t,
) {
    let p = DUCKDB_INIT_SET_INIT_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_init_set_init_data: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_init_info,
            *mut ::std::os::raw::c_void,
            duckdb_delete_callback_t,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_INIT_GET_COLUMN_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_init_get_column_count(a0: duckdb_init_info) -> idx_t {
    let p = DUCKDB_INIT_GET_COLUMN_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_init_get_column_count: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_init_info) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_INIT_GET_COLUMN_INDEX: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_init_get_column_index(
    a0: duckdb_init_info,
    a1: idx_t,
) -> idx_t {
    let p = DUCKDB_INIT_GET_COLUMN_INDEX.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_init_get_column_index: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_init_info, idx_t) -> idx_t = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_INIT_SET_MAX_THREADS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_init_set_max_threads(
    a0: duckdb_init_info,
    a1: idx_t,
) {
    let p = DUCKDB_INIT_SET_MAX_THREADS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_init_set_max_threads: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_init_info, idx_t) = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_INIT_SET_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_init_set_error(
    a0: duckdb_init_info,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_INIT_SET_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_init_set_error: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_init_info, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_FUNCTION_GET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_function_get_extra_info(
    a0: duckdb_function_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_FUNCTION_GET_EXTRA_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_function_get_extra_info: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_function_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_FUNCTION_GET_BIND_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_function_get_bind_data(
    a0: duckdb_function_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_FUNCTION_GET_BIND_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_function_get_bind_data: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_function_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_FUNCTION_GET_INIT_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_function_get_init_data(
    a0: duckdb_function_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_FUNCTION_GET_INIT_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_function_get_init_data: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_function_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_FUNCTION_GET_LOCAL_INIT_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_function_get_local_init_data(
    a0: duckdb_function_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_FUNCTION_GET_LOCAL_INIT_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_function_get_local_init_data: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_function_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_FUNCTION_SET_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_function_set_error(
    a0: duckdb_function_info,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_FUNCTION_SET_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_function_set_error: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_function_info, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_ADD_REPLACEMENT_SCAN: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_add_replacement_scan(
    a0: duckdb_database,
    a1: duckdb_replacement_callback_t,
    a2: *mut ::std::os::raw::c_void,
    a3: duckdb_delete_callback_t,
) {
    let p = DUCKDB_ADD_REPLACEMENT_SCAN.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_add_replacement_scan: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_database,
            duckdb_replacement_callback_t,
            *mut ::std::os::raw::c_void,
            duckdb_delete_callback_t,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2, a3)
    }
}
static DUCKDB_REPLACEMENT_SCAN_SET_FUNCTION_NAME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_replacement_scan_set_function_name(
    a0: duckdb_replacement_scan_info,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_REPLACEMENT_SCAN_SET_FUNCTION_NAME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_replacement_scan_set_function_name: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_replacement_scan_info, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_REPLACEMENT_SCAN_ADD_PARAMETER: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_replacement_scan_add_parameter(
    a0: duckdb_replacement_scan_info,
    a1: duckdb_value,
) {
    let p = DUCKDB_REPLACEMENT_SCAN_ADD_PARAMETER.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_replacement_scan_add_parameter: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_replacement_scan_info, duckdb_value) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_REPLACEMENT_SCAN_SET_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_replacement_scan_set_error(
    a0: duckdb_replacement_scan_info,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_REPLACEMENT_SCAN_SET_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_replacement_scan_set_error: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_replacement_scan_info, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_PROFILING_INFO_GET_METRICS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_profiling_info_get_metrics(a0: duckdb_profiling_info) -> duckdb_value {
    let p = DUCKDB_PROFILING_INFO_GET_METRICS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_profiling_info_get_metrics: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_profiling_info) -> duckdb_value =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_PROFILING_INFO_GET_CHILD_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_profiling_info_get_child_count(a0: duckdb_profiling_info) -> idx_t {
    let p = DUCKDB_PROFILING_INFO_GET_CHILD_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_profiling_info_get_child_count: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_profiling_info) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_PROFILING_INFO_GET_CHILD: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_profiling_info_get_child(
    a0: duckdb_profiling_info,
    a1: idx_t,
) -> duckdb_profiling_info {
    let p = DUCKDB_PROFILING_INFO_GET_CHILD.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_profiling_info_get_child: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_profiling_info, idx_t) -> duckdb_profiling_info =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPENDER_CREATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_appender_create(
    a0: duckdb_connection,
    a1: *const ::std::os::raw::c_char,
    a2: *const ::std::os::raw::c_char,
    a3: *mut duckdb_appender,
) -> duckdb_state {
    let p = DUCKDB_APPENDER_CREATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_appender_create: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            *const ::std::os::raw::c_char,
            *const ::std::os::raw::c_char,
            *mut duckdb_appender,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2, a3)
    }
}
static DUCKDB_APPENDER_CREATE_EXT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_appender_create_ext(
    a0: duckdb_connection,
    a1: *const ::std::os::raw::c_char,
    a2: *const ::std::os::raw::c_char,
    a3: *const ::std::os::raw::c_char,
    a4: *mut duckdb_appender,
) -> duckdb_state {
    let p = DUCKDB_APPENDER_CREATE_EXT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_appender_create_ext: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            *const ::std::os::raw::c_char,
            *const ::std::os::raw::c_char,
            *const ::std::os::raw::c_char,
            *mut duckdb_appender,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2, a3, a4)
    }
}
static DUCKDB_APPENDER_COLUMN_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_appender_column_count(a0: duckdb_appender) -> idx_t {
    let p = DUCKDB_APPENDER_COLUMN_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_appender_column_count: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_APPENDER_COLUMN_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_appender_column_type(
    a0: duckdb_appender,
    a1: idx_t,
) -> duckdb_logical_type {
    let p = DUCKDB_APPENDER_COLUMN_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_appender_column_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, idx_t) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPENDER_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_appender_error(a0: duckdb_appender) -> *const ::std::os::raw::c_char {
    let p = DUCKDB_APPENDER_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_appender_error: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender) -> *const ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_APPENDER_FLUSH: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_appender_flush(a0: duckdb_appender) -> duckdb_state {
    let p = DUCKDB_APPENDER_FLUSH.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_appender_flush: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender) -> duckdb_state = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_APPENDER_CLOSE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_appender_close(a0: duckdb_appender) -> duckdb_state {
    let p = DUCKDB_APPENDER_CLOSE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_appender_close: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender) -> duckdb_state = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_APPENDER_DESTROY: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_appender_destroy(a0: *mut duckdb_appender) -> duckdb_state {
    let p = DUCKDB_APPENDER_DESTROY.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_appender_destroy: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_appender) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_APPENDER_ADD_COLUMN: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_appender_add_column(
    a0: duckdb_appender,
    a1: *const ::std::os::raw::c_char,
) -> duckdb_state {
    let p = DUCKDB_APPENDER_ADD_COLUMN.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_appender_add_column: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_appender,
            *const ::std::os::raw::c_char,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPENDER_CLEAR_COLUMNS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_appender_clear_columns(a0: duckdb_appender) -> duckdb_state {
    let p = DUCKDB_APPENDER_CLEAR_COLUMNS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_appender_clear_columns: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender) -> duckdb_state = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_APPEND_DATA_CHUNK: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_data_chunk(
    a0: duckdb_appender,
    a1: duckdb_data_chunk,
) -> duckdb_state {
    let p = DUCKDB_APPEND_DATA_CHUNK.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_data_chunk: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, duckdb_data_chunk) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_TABLE_DESCRIPTION_CREATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_description_create(
    a0: duckdb_connection,
    a1: *const ::std::os::raw::c_char,
    a2: *const ::std::os::raw::c_char,
    a3: *mut duckdb_table_description,
) -> duckdb_state {
    let p = DUCKDB_TABLE_DESCRIPTION_CREATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_table_description_create: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            *const ::std::os::raw::c_char,
            *const ::std::os::raw::c_char,
            *mut duckdb_table_description,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2, a3)
    }
}
static DUCKDB_TABLE_DESCRIPTION_CREATE_EXT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_description_create_ext(
    a0: duckdb_connection,
    a1: *const ::std::os::raw::c_char,
    a2: *const ::std::os::raw::c_char,
    a3: *const ::std::os::raw::c_char,
    a4: *mut duckdb_table_description,
) -> duckdb_state {
    let p = DUCKDB_TABLE_DESCRIPTION_CREATE_EXT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_table_description_create_ext: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            *const ::std::os::raw::c_char,
            *const ::std::os::raw::c_char,
            *const ::std::os::raw::c_char,
            *mut duckdb_table_description,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2, a3, a4)
    }
}
static DUCKDB_TABLE_DESCRIPTION_DESTROY: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_description_destroy(a0: *mut duckdb_table_description) {
    let p = DUCKDB_TABLE_DESCRIPTION_DESTROY.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_table_description_destroy: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_table_description) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_TABLE_DESCRIPTION_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_description_error(
    a0: duckdb_table_description
) -> *const ::std::os::raw::c_char {
    let p = DUCKDB_TABLE_DESCRIPTION_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_table_description_error: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_table_description) -> *const ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COLUMN_HAS_DEFAULT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_column_has_default(
    a0: duckdb_table_description,
    a1: idx_t,
    a2: *mut bool,
) -> duckdb_state {
    let p = DUCKDB_COLUMN_HAS_DEFAULT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_column_has_default: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_table_description, idx_t, *mut bool) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_TABLE_DESCRIPTION_GET_COLUMN_NAME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_description_get_column_name(
    a0: duckdb_table_description,
    a1: idx_t,
) -> *mut ::std::os::raw::c_char {
    let p = DUCKDB_TABLE_DESCRIPTION_GET_COLUMN_NAME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_table_description_get_column_name: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_table_description,
            idx_t,
        ) -> *mut ::std::os::raw::c_char = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_EXECUTE_TASKS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_execute_tasks(
    a0: duckdb_database,
    a1: idx_t,
) {
    let p = DUCKDB_EXECUTE_TASKS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_execute_tasks: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_database, idx_t) = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_TASK_STATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_task_state(a0: duckdb_database) -> duckdb_task_state {
    let p = DUCKDB_CREATE_TASK_STATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_task_state: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_database) -> duckdb_task_state =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_EXECUTE_TASKS_STATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_execute_tasks_state(a0: duckdb_task_state) {
    let p = DUCKDB_EXECUTE_TASKS_STATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_execute_tasks_state: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_task_state) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_EXECUTE_N_TASKS_STATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_execute_n_tasks_state(
    a0: duckdb_task_state,
    a1: idx_t,
) -> idx_t {
    let p = DUCKDB_EXECUTE_N_TASKS_STATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_execute_n_tasks_state: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_task_state, idx_t) -> idx_t = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_FINISH_EXECUTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_finish_execution(a0: duckdb_task_state) {
    let p = DUCKDB_FINISH_EXECUTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_finish_execution: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_task_state) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_TASK_STATE_IS_FINISHED: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_task_state_is_finished(a0: duckdb_task_state) -> bool {
    let p = DUCKDB_TASK_STATE_IS_FINISHED.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_task_state_is_finished: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_task_state) -> bool = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DESTROY_TASK_STATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_task_state(a0: duckdb_task_state) {
    let p = DUCKDB_DESTROY_TASK_STATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_task_state: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_task_state) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_EXECUTION_IS_FINISHED: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_execution_is_finished(a0: duckdb_connection) -> bool {
    let p = DUCKDB_EXECUTION_IS_FINISHED.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_execution_is_finished: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_connection) -> bool = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_FETCH_CHUNK: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_fetch_chunk(a0: duckdb_result) -> duckdb_data_chunk {
    let p = DUCKDB_FETCH_CHUNK.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_fetch_chunk: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_result) -> duckdb_data_chunk = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_CAST_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_cast_function() -> duckdb_cast_function {
    let p = DUCKDB_CREATE_CAST_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_cast_function: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn() -> duckdb_cast_function = ::std::mem::transmute(p);
        f()
    }
}
static DUCKDB_CAST_FUNCTION_SET_SOURCE_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_cast_function_set_source_type(
    a0: duckdb_cast_function,
    a1: duckdb_logical_type,
) {
    let p = DUCKDB_CAST_FUNCTION_SET_SOURCE_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_cast_function_set_source_type: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_cast_function, duckdb_logical_type) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CAST_FUNCTION_SET_TARGET_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_cast_function_set_target_type(
    a0: duckdb_cast_function,
    a1: duckdb_logical_type,
) {
    let p = DUCKDB_CAST_FUNCTION_SET_TARGET_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_cast_function_set_target_type: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_cast_function, duckdb_logical_type) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CAST_FUNCTION_SET_IMPLICIT_CAST_COST: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_cast_function_set_implicit_cast_cost(
    a0: duckdb_cast_function,
    a1: i64,
) {
    let p =
        DUCKDB_CAST_FUNCTION_SET_IMPLICIT_CAST_COST.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_cast_function_set_implicit_cast_cost: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_cast_function, i64) = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CAST_FUNCTION_SET_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_cast_function_set_function(
    a0: duckdb_cast_function,
    a1: duckdb_cast_function_t,
) {
    let p = DUCKDB_CAST_FUNCTION_SET_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_cast_function_set_function: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_cast_function, duckdb_cast_function_t) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CAST_FUNCTION_SET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_cast_function_set_extra_info(
    a0: duckdb_cast_function,
    a1: *mut ::std::os::raw::c_void,
    a2: duckdb_delete_callback_t,
) {
    let p = DUCKDB_CAST_FUNCTION_SET_EXTRA_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_cast_function_set_extra_info: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_cast_function,
            *mut ::std::os::raw::c_void,
            duckdb_delete_callback_t,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_CAST_FUNCTION_GET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_cast_function_get_extra_info(
    a0: duckdb_function_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_CAST_FUNCTION_GET_EXTRA_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_cast_function_get_extra_info: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_function_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CAST_FUNCTION_GET_CAST_MODE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_cast_function_get_cast_mode(a0: duckdb_function_info) -> duckdb_cast_mode {
    let p = DUCKDB_CAST_FUNCTION_GET_CAST_MODE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_cast_function_get_cast_mode: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_function_info) -> duckdb_cast_mode =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CAST_FUNCTION_SET_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_cast_function_set_error(
    a0: duckdb_function_info,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_CAST_FUNCTION_SET_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_cast_function_set_error: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_function_info, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CAST_FUNCTION_SET_ROW_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_cast_function_set_row_error(
    a0: duckdb_function_info,
    a1: *const ::std::os::raw::c_char,
    a2: idx_t,
    a3: duckdb_vector,
) {
    let p = DUCKDB_CAST_FUNCTION_SET_ROW_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_cast_function_set_row_error: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_function_info,
            *const ::std::os::raw::c_char,
            idx_t,
            duckdb_vector,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2, a3)
    }
}
static DUCKDB_REGISTER_CAST_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_register_cast_function(
    a0: duckdb_connection,
    a1: duckdb_cast_function,
) -> duckdb_state {
    let p = DUCKDB_REGISTER_CAST_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_register_cast_function: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_connection, duckdb_cast_function) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_DESTROY_CAST_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_cast_function(a0: *mut duckdb_cast_function) {
    let p = DUCKDB_DESTROY_CAST_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_cast_function: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_cast_function) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_IS_FINITE_TIMESTAMP_S: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_is_finite_timestamp_s(a0: duckdb_timestamp_s) -> bool {
    let p = DUCKDB_IS_FINITE_TIMESTAMP_S.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_is_finite_timestamp_s: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_timestamp_s) -> bool = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_IS_FINITE_TIMESTAMP_MS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_is_finite_timestamp_ms(a0: duckdb_timestamp_ms) -> bool {
    let p = DUCKDB_IS_FINITE_TIMESTAMP_MS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_is_finite_timestamp_ms: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_timestamp_ms) -> bool = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_IS_FINITE_TIMESTAMP_NS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_is_finite_timestamp_ns(a0: duckdb_timestamp_ns) -> bool {
    let p = DUCKDB_IS_FINITE_TIMESTAMP_NS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_is_finite_timestamp_ns: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_timestamp_ns) -> bool = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_TIMESTAMP_TZ: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_timestamp_tz(a0: duckdb_timestamp) -> duckdb_value {
    let p = DUCKDB_CREATE_TIMESTAMP_TZ.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_timestamp_tz: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_timestamp) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_TIMESTAMP_S: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_timestamp_s(a0: duckdb_timestamp_s) -> duckdb_value {
    let p = DUCKDB_CREATE_TIMESTAMP_S.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_timestamp_s: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_timestamp_s) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_TIMESTAMP_MS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_timestamp_ms(a0: duckdb_timestamp_ms) -> duckdb_value {
    let p = DUCKDB_CREATE_TIMESTAMP_MS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_timestamp_ms: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_timestamp_ms) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_TIMESTAMP_NS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_timestamp_ns(a0: duckdb_timestamp_ns) -> duckdb_value {
    let p = DUCKDB_CREATE_TIMESTAMP_NS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_timestamp_ns: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_timestamp_ns) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_TIMESTAMP_TZ: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_timestamp_tz(a0: duckdb_value) -> duckdb_timestamp {
    let p = DUCKDB_GET_TIMESTAMP_TZ.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_timestamp_tz: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_timestamp = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_TIMESTAMP_S: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_timestamp_s(a0: duckdb_value) -> duckdb_timestamp_s {
    let p = DUCKDB_GET_TIMESTAMP_S.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_timestamp_s: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_timestamp_s = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_TIMESTAMP_MS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_timestamp_ms(a0: duckdb_value) -> duckdb_timestamp_ms {
    let p = DUCKDB_GET_TIMESTAMP_MS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_timestamp_ms: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_timestamp_ms = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_TIMESTAMP_NS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_timestamp_ns(a0: duckdb_value) -> duckdb_timestamp_ns {
    let p = DUCKDB_GET_TIMESTAMP_NS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_timestamp_ns: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_timestamp_ns = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_APPEND_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_value(
    a0: duckdb_appender,
    a1: duckdb_value,
) -> duckdb_state {
    let p = DUCKDB_APPEND_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, duckdb_value) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_GET_PROFILING_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_profiling_info(a0: duckdb_connection) -> duckdb_profiling_info {
    let p = DUCKDB_GET_PROFILING_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_profiling_info: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_connection) -> duckdb_profiling_info =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_PROFILING_INFO_GET_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_profiling_info_get_value(
    a0: duckdb_profiling_info,
    a1: *const ::std::os::raw::c_char,
) -> duckdb_value {
    let p = DUCKDB_PROFILING_INFO_GET_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_profiling_info_get_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_profiling_info,
            *const ::std::os::raw::c_char,
        ) -> duckdb_value = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPENDER_BEGIN_ROW: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_appender_begin_row(a0: duckdb_appender) -> duckdb_state {
    let p = DUCKDB_APPENDER_BEGIN_ROW.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_appender_begin_row: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender) -> duckdb_state = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_APPENDER_END_ROW: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_appender_end_row(a0: duckdb_appender) -> duckdb_state {
    let p = DUCKDB_APPENDER_END_ROW.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_appender_end_row: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender) -> duckdb_state = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_APPEND_DEFAULT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_default(a0: duckdb_appender) -> duckdb_state {
    let p = DUCKDB_APPEND_DEFAULT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_default: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender) -> duckdb_state = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_APPEND_BOOL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_bool(
    a0: duckdb_appender,
    a1: bool,
) -> duckdb_state {
    let p = DUCKDB_APPEND_BOOL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_bool: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, bool) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_INT8: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_int8(
    a0: duckdb_appender,
    a1: i8,
) -> duckdb_state {
    let p = DUCKDB_APPEND_INT8.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_int8: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, i8) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_INT16: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_int16(
    a0: duckdb_appender,
    a1: i16,
) -> duckdb_state {
    let p = DUCKDB_APPEND_INT16.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_int16: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, i16) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_INT32: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_int32(
    a0: duckdb_appender,
    a1: i32,
) -> duckdb_state {
    let p = DUCKDB_APPEND_INT32.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_int32: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, i32) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_INT64: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_int64(
    a0: duckdb_appender,
    a1: i64,
) -> duckdb_state {
    let p = DUCKDB_APPEND_INT64.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_int64: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, i64) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_HUGEINT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_hugeint(
    a0: duckdb_appender,
    a1: duckdb_hugeint,
) -> duckdb_state {
    let p = DUCKDB_APPEND_HUGEINT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_hugeint: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, duckdb_hugeint) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_UINT8: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_uint8(
    a0: duckdb_appender,
    a1: u8,
) -> duckdb_state {
    let p = DUCKDB_APPEND_UINT8.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_uint8: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, u8) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_UINT16: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_uint16(
    a0: duckdb_appender,
    a1: u16,
) -> duckdb_state {
    let p = DUCKDB_APPEND_UINT16.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_uint16: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, u16) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_UINT32: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_uint32(
    a0: duckdb_appender,
    a1: u32,
) -> duckdb_state {
    let p = DUCKDB_APPEND_UINT32.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_uint32: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, u32) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_UINT64: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_uint64(
    a0: duckdb_appender,
    a1: u64,
) -> duckdb_state {
    let p = DUCKDB_APPEND_UINT64.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_uint64: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, u64) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_UHUGEINT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_uhugeint(
    a0: duckdb_appender,
    a1: duckdb_uhugeint,
) -> duckdb_state {
    let p = DUCKDB_APPEND_UHUGEINT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_uhugeint: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, duckdb_uhugeint) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_FLOAT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_float(
    a0: duckdb_appender,
    a1: f32,
) -> duckdb_state {
    let p = DUCKDB_APPEND_FLOAT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_float: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, f32) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_DOUBLE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_double(
    a0: duckdb_appender,
    a1: f64,
) -> duckdb_state {
    let p = DUCKDB_APPEND_DOUBLE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_double: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, f64) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_DATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_date(
    a0: duckdb_appender,
    a1: duckdb_date,
) -> duckdb_state {
    let p = DUCKDB_APPEND_DATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_date: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, duckdb_date) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_TIME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_time(
    a0: duckdb_appender,
    a1: duckdb_time,
) -> duckdb_state {
    let p = DUCKDB_APPEND_TIME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_time: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, duckdb_time) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_TIMESTAMP: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_timestamp(
    a0: duckdb_appender,
    a1: duckdb_timestamp,
) -> duckdb_state {
    let p = DUCKDB_APPEND_TIMESTAMP.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_timestamp: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, duckdb_timestamp) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_INTERVAL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_interval(
    a0: duckdb_appender,
    a1: duckdb_interval,
) -> duckdb_state {
    let p = DUCKDB_APPEND_INTERVAL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_interval: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender, duckdb_interval) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_VARCHAR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_varchar(
    a0: duckdb_appender,
    a1: *const ::std::os::raw::c_char,
) -> duckdb_state {
    let p = DUCKDB_APPEND_VARCHAR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_varchar: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_appender,
            *const ::std::os::raw::c_char,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_APPEND_VARCHAR_LENGTH: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_varchar_length(
    a0: duckdb_appender,
    a1: *const ::std::os::raw::c_char,
    a2: idx_t,
) -> duckdb_state {
    let p = DUCKDB_APPEND_VARCHAR_LENGTH.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_varchar_length: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_appender,
            *const ::std::os::raw::c_char,
            idx_t,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_APPEND_BLOB: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_blob(
    a0: duckdb_appender,
    a1: *const ::std::os::raw::c_void,
    a2: idx_t,
) -> duckdb_state {
    let p = DUCKDB_APPEND_BLOB.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_blob: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_appender,
            *const ::std::os::raw::c_void,
            idx_t,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_APPEND_NULL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_null(a0: duckdb_appender) -> duckdb_state {
    let p = DUCKDB_APPEND_NULL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_null: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender) -> duckdb_state = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_ROW_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_row_count(a0: *mut duckdb_result) -> idx_t {
    let p = DUCKDB_ROW_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_row_count: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COLUMN_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_column_data(
    a0: *mut duckdb_result,
    a1: idx_t,
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_COLUMN_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_column_data: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_NULLMASK_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_nullmask_data(
    a0: *mut duckdb_result,
    a1: idx_t,
) -> *mut bool {
    let p = DUCKDB_NULLMASK_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_nullmask_data: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t) -> *mut bool =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_RESULT_GET_CHUNK: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_result_get_chunk(
    a0: duckdb_result,
    a1: idx_t,
) -> duckdb_data_chunk {
    let p = DUCKDB_RESULT_GET_CHUNK.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_result_get_chunk: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_result, idx_t) -> duckdb_data_chunk =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_RESULT_IS_STREAMING: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_result_is_streaming(a0: duckdb_result) -> bool {
    let p = DUCKDB_RESULT_IS_STREAMING.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_result_is_streaming: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_result) -> bool = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_RESULT_CHUNK_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_result_chunk_count(a0: duckdb_result) -> idx_t {
    let p = DUCKDB_RESULT_CHUNK_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_result_chunk_count: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_result) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_VALUE_BOOLEAN: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_boolean(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> bool {
    let p = DUCKDB_VALUE_BOOLEAN.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_boolean: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> bool =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_INT8: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_int8(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> i8 {
    let p = DUCKDB_VALUE_INT8.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_int8: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> i8 =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_INT16: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_int16(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> i16 {
    let p = DUCKDB_VALUE_INT16.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_int16: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> i16 =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_INT32: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_int32(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> i32 {
    let p = DUCKDB_VALUE_INT32.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_int32: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> i32 =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_INT64: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_int64(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> i64 {
    let p = DUCKDB_VALUE_INT64.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_int64: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> i64 =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_HUGEINT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_hugeint(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> duckdb_hugeint {
    let p = DUCKDB_VALUE_HUGEINT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_hugeint: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> duckdb_hugeint =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_UHUGEINT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_uhugeint(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> duckdb_uhugeint {
    let p = DUCKDB_VALUE_UHUGEINT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_uhugeint: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> duckdb_uhugeint =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_DECIMAL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_decimal(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> duckdb_decimal {
    let p = DUCKDB_VALUE_DECIMAL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_decimal: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> duckdb_decimal =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_UINT8: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_uint8(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> u8 {
    let p = DUCKDB_VALUE_UINT8.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_uint8: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> u8 =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_UINT16: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_uint16(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> u16 {
    let p = DUCKDB_VALUE_UINT16.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_uint16: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> u16 =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_UINT32: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_uint32(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> u32 {
    let p = DUCKDB_VALUE_UINT32.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_uint32: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> u32 =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_UINT64: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_uint64(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> u64 {
    let p = DUCKDB_VALUE_UINT64.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_uint64: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> u64 =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_FLOAT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_float(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> f32 {
    let p = DUCKDB_VALUE_FLOAT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_float: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> f32 =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_DOUBLE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_double(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> f64 {
    let p = DUCKDB_VALUE_DOUBLE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_double: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> f64 =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_DATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_date(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> duckdb_date {
    let p = DUCKDB_VALUE_DATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_date: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> duckdb_date =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_TIME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_time(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> duckdb_time {
    let p = DUCKDB_VALUE_TIME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_time: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> duckdb_time =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_TIMESTAMP: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_timestamp(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> duckdb_timestamp {
    let p = DUCKDB_VALUE_TIMESTAMP.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_timestamp: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> duckdb_timestamp =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_INTERVAL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_interval(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> duckdb_interval {
    let p = DUCKDB_VALUE_INTERVAL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_interval: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> duckdb_interval =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_VARCHAR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_varchar(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> *mut ::std::os::raw::c_char {
    let p = DUCKDB_VALUE_VARCHAR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_varchar: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            *mut duckdb_result,
            idx_t,
            idx_t,
        ) -> *mut ::std::os::raw::c_char = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_STRING: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_string(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> duckdb_string {
    let p = DUCKDB_VALUE_STRING.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_string: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> duckdb_string =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_VARCHAR_INTERNAL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_varchar_internal(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> *mut ::std::os::raw::c_char {
    let p = DUCKDB_VALUE_VARCHAR_INTERNAL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_varchar_internal: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            *mut duckdb_result,
            idx_t,
            idx_t,
        ) -> *mut ::std::os::raw::c_char = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_STRING_INTERNAL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_string_internal(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> duckdb_string {
    let p = DUCKDB_VALUE_STRING_INTERNAL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_string_internal: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> duckdb_string =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_BLOB: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_blob(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> duckdb_blob {
    let p = DUCKDB_VALUE_BLOB.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_blob: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> duckdb_blob =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VALUE_IS_NULL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_is_null(
    a0: *mut duckdb_result,
    a1: idx_t,
    a2: idx_t,
) -> bool {
    let p = DUCKDB_VALUE_IS_NULL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_is_null: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result, idx_t, idx_t) -> bool =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_EXECUTE_PREPARED_STREAMING: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_execute_prepared_streaming(
    a0: duckdb_prepared_statement,
    a1: *mut duckdb_result,
) -> duckdb_state {
    let p = DUCKDB_EXECUTE_PREPARED_STREAMING.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_execute_prepared_streaming: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, *mut duckdb_result) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_PENDING_PREPARED_STREAMING: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_pending_prepared_streaming(
    a0: duckdb_prepared_statement,
    a1: *mut duckdb_pending_result,
) -> duckdb_state {
    let p = DUCKDB_PENDING_PREPARED_STREAMING.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_pending_prepared_streaming: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_prepared_statement,
            *mut duckdb_pending_result,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_QUERY_ARROW: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_query_arrow(
    a0: duckdb_connection,
    a1: *const ::std::os::raw::c_char,
    a2: *mut duckdb_arrow,
) -> duckdb_state {
    let p = DUCKDB_QUERY_ARROW.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_query_arrow: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            *const ::std::os::raw::c_char,
            *mut duckdb_arrow,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_QUERY_ARROW_SCHEMA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_query_arrow_schema(
    a0: duckdb_arrow,
    a1: *mut duckdb_arrow_schema,
) -> duckdb_state {
    let p = DUCKDB_QUERY_ARROW_SCHEMA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_query_arrow_schema: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_arrow, *mut duckdb_arrow_schema) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_PREPARED_ARROW_SCHEMA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_prepared_arrow_schema(
    a0: duckdb_prepared_statement,
    a1: *mut duckdb_arrow_schema,
) -> duckdb_state {
    let p = DUCKDB_PREPARED_ARROW_SCHEMA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_prepared_arrow_schema: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_prepared_statement,
            *mut duckdb_arrow_schema,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_RESULT_ARROW_ARRAY: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_result_arrow_array(
    a0: duckdb_result,
    a1: duckdb_data_chunk,
    a2: *mut duckdb_arrow_array,
) {
    let p = DUCKDB_RESULT_ARROW_ARRAY.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_result_arrow_array: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_result, duckdb_data_chunk, *mut duckdb_arrow_array) =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_QUERY_ARROW_ARRAY: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_query_arrow_array(
    a0: duckdb_arrow,
    a1: *mut duckdb_arrow_array,
) -> duckdb_state {
    let p = DUCKDB_QUERY_ARROW_ARRAY.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_query_arrow_array: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_arrow, *mut duckdb_arrow_array) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_ARROW_COLUMN_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_arrow_column_count(a0: duckdb_arrow) -> idx_t {
    let p = DUCKDB_ARROW_COLUMN_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_arrow_column_count: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_arrow) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_ARROW_ROW_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_arrow_row_count(a0: duckdb_arrow) -> idx_t {
    let p = DUCKDB_ARROW_ROW_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_arrow_row_count: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_arrow) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_ARROW_ROWS_CHANGED: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_arrow_rows_changed(a0: duckdb_arrow) -> idx_t {
    let p = DUCKDB_ARROW_ROWS_CHANGED.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_arrow_rows_changed: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_arrow) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_QUERY_ARROW_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_query_arrow_error(a0: duckdb_arrow) -> *const ::std::os::raw::c_char {
    let p = DUCKDB_QUERY_ARROW_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_query_arrow_error: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_arrow) -> *const ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DESTROY_ARROW: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_arrow(a0: *mut duckdb_arrow) {
    let p = DUCKDB_DESTROY_ARROW.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_arrow: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_arrow) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DESTROY_ARROW_STREAM: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_arrow_stream(a0: *mut duckdb_arrow_stream) {
    let p = DUCKDB_DESTROY_ARROW_STREAM.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_arrow_stream: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_arrow_stream) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_EXECUTE_PREPARED_ARROW: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_execute_prepared_arrow(
    a0: duckdb_prepared_statement,
    a1: *mut duckdb_arrow,
) -> duckdb_state {
    let p = DUCKDB_EXECUTE_PREPARED_ARROW.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_execute_prepared_arrow: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, *mut duckdb_arrow) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_ARROW_SCAN: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_arrow_scan(
    a0: duckdb_connection,
    a1: *const ::std::os::raw::c_char,
    a2: duckdb_arrow_stream,
) -> duckdb_state {
    let p = DUCKDB_ARROW_SCAN.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_arrow_scan: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            *const ::std::os::raw::c_char,
            duckdb_arrow_stream,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_ARROW_ARRAY_SCAN: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_arrow_array_scan(
    a0: duckdb_connection,
    a1: *const ::std::os::raw::c_char,
    a2: duckdb_arrow_schema,
    a3: duckdb_arrow_array,
    a4: *mut duckdb_arrow_stream,
) -> duckdb_state {
    let p = DUCKDB_ARROW_ARRAY_SCAN.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_arrow_array_scan: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            *const ::std::os::raw::c_char,
            duckdb_arrow_schema,
            duckdb_arrow_array,
            *mut duckdb_arrow_stream,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2, a3, a4)
    }
}
static DUCKDB_STREAM_FETCH_CHUNK: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_stream_fetch_chunk(a0: duckdb_result) -> duckdb_data_chunk {
    let p = DUCKDB_STREAM_FETCH_CHUNK.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_stream_fetch_chunk: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_result) -> duckdb_data_chunk = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_INSTANCE_CACHE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_instance_cache() -> duckdb_instance_cache {
    let p = DUCKDB_CREATE_INSTANCE_CACHE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_instance_cache: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn() -> duckdb_instance_cache = ::std::mem::transmute(p);
        f()
    }
}
static DUCKDB_GET_OR_CREATE_FROM_CACHE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_or_create_from_cache(
    a0: duckdb_instance_cache,
    a1: *const ::std::os::raw::c_char,
    a2: *mut duckdb_database,
    a3: duckdb_config,
    a4: *mut *mut ::std::os::raw::c_char,
) -> duckdb_state {
    let p = DUCKDB_GET_OR_CREATE_FROM_CACHE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_or_create_from_cache: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_instance_cache,
            *const ::std::os::raw::c_char,
            *mut duckdb_database,
            duckdb_config,
            *mut *mut ::std::os::raw::c_char,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2, a3, a4)
    }
}
static DUCKDB_DESTROY_INSTANCE_CACHE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_instance_cache(a0: *mut duckdb_instance_cache) {
    let p = DUCKDB_DESTROY_INSTANCE_CACHE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_instance_cache: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_instance_cache) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_APPEND_DEFAULT_TO_CHUNK: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_append_default_to_chunk(
    a0: duckdb_appender,
    a1: duckdb_data_chunk,
    a2: idx_t,
    a3: idx_t,
) -> duckdb_state {
    let p = DUCKDB_APPEND_DEFAULT_TO_CHUNK.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_append_default_to_chunk: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_appender,
            duckdb_data_chunk,
            idx_t,
            idx_t,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2, a3)
    }
}
static DUCKDB_APPENDER_ERROR_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_appender_error_data(a0: duckdb_appender) -> duckdb_error_data {
    let p = DUCKDB_APPENDER_ERROR_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_appender_error_data: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender) -> duckdb_error_data =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_APPENDER_CREATE_QUERY: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_appender_create_query(
    a0: duckdb_connection,
    a1: *const ::std::os::raw::c_char,
    a2: idx_t,
    a3: *mut duckdb_logical_type,
    a4: *const ::std::os::raw::c_char,
    a5: *mut *const ::std::os::raw::c_char,
    a6: *mut duckdb_appender,
) -> duckdb_state {
    let p = DUCKDB_APPENDER_CREATE_QUERY.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_appender_create_query: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            *const ::std::os::raw::c_char,
            idx_t,
            *mut duckdb_logical_type,
            *const ::std::os::raw::c_char,
            *mut *const ::std::os::raw::c_char,
            *mut duckdb_appender,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2, a3, a4, a5, a6)
    }
}
static DUCKDB_APPENDER_CLEAR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_appender_clear(a0: duckdb_appender) -> duckdb_state {
    let p = DUCKDB_APPENDER_CLEAR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_appender_clear: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_appender) -> duckdb_state = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_TO_ARROW_SCHEMA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_to_arrow_schema(
    a0: duckdb_arrow_options,
    a1: *mut duckdb_logical_type,
    a2: *mut *const ::std::os::raw::c_char,
    a3: idx_t,
    a4: *mut ArrowSchema,
) -> duckdb_error_data {
    let p = DUCKDB_TO_ARROW_SCHEMA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_to_arrow_schema: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_arrow_options,
            *mut duckdb_logical_type,
            *mut *const ::std::os::raw::c_char,
            idx_t,
            *mut ArrowSchema,
        ) -> duckdb_error_data = ::std::mem::transmute(p);
        f(a0, a1, a2, a3, a4)
    }
}
static DUCKDB_DATA_CHUNK_TO_ARROW: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_data_chunk_to_arrow(
    a0: duckdb_arrow_options,
    a1: duckdb_data_chunk,
    a2: *mut ArrowArray,
) -> duckdb_error_data {
    let p = DUCKDB_DATA_CHUNK_TO_ARROW.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_data_chunk_to_arrow: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_arrow_options,
            duckdb_data_chunk,
            *mut ArrowArray,
        ) -> duckdb_error_data = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_SCHEMA_FROM_ARROW: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_schema_from_arrow(
    a0: duckdb_connection,
    a1: *mut ArrowSchema,
    a2: *mut duckdb_arrow_converted_schema,
) -> duckdb_error_data {
    let p = DUCKDB_SCHEMA_FROM_ARROW.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_schema_from_arrow: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            *mut ArrowSchema,
            *mut duckdb_arrow_converted_schema,
        ) -> duckdb_error_data = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_DATA_CHUNK_FROM_ARROW: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_data_chunk_from_arrow(
    a0: duckdb_connection,
    a1: *mut ArrowArray,
    a2: duckdb_arrow_converted_schema,
    a3: *mut duckdb_data_chunk,
) -> duckdb_error_data {
    let p = DUCKDB_DATA_CHUNK_FROM_ARROW.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_data_chunk_from_arrow: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            *mut ArrowArray,
            duckdb_arrow_converted_schema,
            *mut duckdb_data_chunk,
        ) -> duckdb_error_data = ::std::mem::transmute(p);
        f(a0, a1, a2, a3)
    }
}
static DUCKDB_DESTROY_ARROW_CONVERTED_SCHEMA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_arrow_converted_schema(a0: *mut duckdb_arrow_converted_schema) {
    let p = DUCKDB_DESTROY_ARROW_CONVERTED_SCHEMA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_destroy_arrow_converted_schema: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_arrow_converted_schema) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CLIENT_CONTEXT_GET_CATALOG: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_client_context_get_catalog(
    a0: duckdb_client_context,
    a1: *const ::std::os::raw::c_char,
) -> duckdb_catalog {
    let p = DUCKDB_CLIENT_CONTEXT_GET_CATALOG.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_client_context_get_catalog: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_client_context,
            *const ::std::os::raw::c_char,
        ) -> duckdb_catalog = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CATALOG_GET_TYPE_NAME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_catalog_get_type_name(a0: duckdb_catalog) -> *const ::std::os::raw::c_char {
    let p = DUCKDB_CATALOG_GET_TYPE_NAME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_catalog_get_type_name: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_catalog) -> *const ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CATALOG_GET_ENTRY: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_catalog_get_entry(
    a0: duckdb_catalog,
    a1: duckdb_client_context,
    a2: duckdb_catalog_entry_type,
    a3: *const ::std::os::raw::c_char,
    a4: *const ::std::os::raw::c_char,
) -> duckdb_catalog_entry {
    let p = DUCKDB_CATALOG_GET_ENTRY.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_catalog_get_entry: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_catalog,
            duckdb_client_context,
            duckdb_catalog_entry_type,
            *const ::std::os::raw::c_char,
            *const ::std::os::raw::c_char,
        ) -> duckdb_catalog_entry = ::std::mem::transmute(p);
        f(a0, a1, a2, a3, a4)
    }
}
static DUCKDB_DESTROY_CATALOG: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_catalog(a0: *mut duckdb_catalog) {
    let p = DUCKDB_DESTROY_CATALOG.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_catalog: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_catalog) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CATALOG_ENTRY_GET_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_catalog_entry_get_type(a0: duckdb_catalog_entry) -> duckdb_catalog_entry_type {
    let p = DUCKDB_CATALOG_ENTRY_GET_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_catalog_entry_get_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_catalog_entry) -> duckdb_catalog_entry_type =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CATALOG_ENTRY_GET_NAME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_catalog_entry_get_name(
    a0: duckdb_catalog_entry
) -> *const ::std::os::raw::c_char {
    let p = DUCKDB_CATALOG_ENTRY_GET_NAME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_catalog_entry_get_name: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_catalog_entry) -> *const ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DESTROY_CATALOG_ENTRY: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_catalog_entry(a0: *mut duckdb_catalog_entry) {
    let p = DUCKDB_DESTROY_CATALOG_ENTRY.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_catalog_entry: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_catalog_entry) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_CONFIG_OPTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_config_option() -> duckdb_config_option {
    let p = DUCKDB_CREATE_CONFIG_OPTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_config_option: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn() -> duckdb_config_option = ::std::mem::transmute(p);
        f()
    }
}
static DUCKDB_DESTROY_CONFIG_OPTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_config_option(a0: *mut duckdb_config_option) {
    let p = DUCKDB_DESTROY_CONFIG_OPTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_config_option: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_config_option) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CONFIG_OPTION_SET_NAME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_config_option_set_name(
    a0: duckdb_config_option,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_CONFIG_OPTION_SET_NAME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_config_option_set_name: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_config_option, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CONFIG_OPTION_SET_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_config_option_set_type(
    a0: duckdb_config_option,
    a1: duckdb_logical_type,
) {
    let p = DUCKDB_CONFIG_OPTION_SET_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_config_option_set_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_config_option, duckdb_logical_type) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CONFIG_OPTION_SET_DEFAULT_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_config_option_set_default_value(
    a0: duckdb_config_option,
    a1: duckdb_value,
) {
    let p = DUCKDB_CONFIG_OPTION_SET_DEFAULT_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_config_option_set_default_value: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_config_option, duckdb_value) = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CONFIG_OPTION_SET_DEFAULT_SCOPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_config_option_set_default_scope(
    a0: duckdb_config_option,
    a1: duckdb_config_option_scope,
) {
    let p = DUCKDB_CONFIG_OPTION_SET_DEFAULT_SCOPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_config_option_set_default_scope: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_config_option, duckdb_config_option_scope) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CONFIG_OPTION_SET_DESCRIPTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_config_option_set_description(
    a0: duckdb_config_option,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_CONFIG_OPTION_SET_DESCRIPTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_config_option_set_description: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_config_option, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_REGISTER_CONFIG_OPTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_register_config_option(
    a0: duckdb_connection,
    a1: duckdb_config_option,
) -> duckdb_state {
    let p = DUCKDB_REGISTER_CONFIG_OPTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_register_config_option: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_connection, duckdb_config_option) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CLIENT_CONTEXT_GET_CONFIG_OPTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_client_context_get_config_option(
    a0: duckdb_client_context,
    a1: *const ::std::os::raw::c_char,
    a2: *mut duckdb_config_option_scope,
) -> duckdb_value {
    let p = DUCKDB_CLIENT_CONTEXT_GET_CONFIG_OPTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_client_context_get_config_option: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_client_context,
            *const ::std::os::raw::c_char,
            *mut duckdb_config_option_scope,
        ) -> duckdb_value = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_CREATE_COPY_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_copy_function() -> duckdb_copy_function {
    let p = DUCKDB_CREATE_COPY_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_copy_function: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn() -> duckdb_copy_function = ::std::mem::transmute(p);
        f()
    }
}
static DUCKDB_COPY_FUNCTION_SET_NAME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_set_name(
    a0: duckdb_copy_function,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_COPY_FUNCTION_SET_NAME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_copy_function_set_name: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_COPY_FUNCTION_SET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_set_extra_info(
    a0: duckdb_copy_function,
    a1: *mut ::std::os::raw::c_void,
    a2: duckdb_delete_callback_t,
) {
    let p = DUCKDB_COPY_FUNCTION_SET_EXTRA_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_set_extra_info: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_copy_function,
            *mut ::std::os::raw::c_void,
            duckdb_delete_callback_t,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_REGISTER_COPY_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_register_copy_function(
    a0: duckdb_connection,
    a1: duckdb_copy_function,
) -> duckdb_state {
    let p = DUCKDB_REGISTER_COPY_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_register_copy_function: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_connection, duckdb_copy_function) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_DESTROY_COPY_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_copy_function(a0: *mut duckdb_copy_function) {
    let p = DUCKDB_DESTROY_COPY_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_copy_function: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_copy_function) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_SET_BIND: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_set_bind(
    a0: duckdb_copy_function,
    a1: duckdb_copy_function_bind_t,
) {
    let p = DUCKDB_COPY_FUNCTION_SET_BIND.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_copy_function_set_bind: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function, duckdb_copy_function_bind_t) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_COPY_FUNCTION_BIND_SET_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_bind_set_error(
    a0: duckdb_copy_function_bind_info,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_COPY_FUNCTION_BIND_SET_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_bind_set_error: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function_bind_info, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_COPY_FUNCTION_BIND_GET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_bind_get_extra_info(
    a0: duckdb_copy_function_bind_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_COPY_FUNCTION_BIND_GET_EXTRA_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_bind_get_extra_info: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function_bind_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_BIND_GET_CLIENT_CONTEXT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_bind_get_client_context(
    a0: duckdb_copy_function_bind_info
) -> duckdb_client_context {
    let p =
        DUCKDB_COPY_FUNCTION_BIND_GET_CLIENT_CONTEXT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_bind_get_client_context: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function_bind_info) -> duckdb_client_context =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_BIND_GET_COLUMN_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_bind_get_column_count(
    a0: duckdb_copy_function_bind_info
) -> idx_t {
    let p = DUCKDB_COPY_FUNCTION_BIND_GET_COLUMN_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_bind_get_column_count: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function_bind_info) -> idx_t =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_BIND_GET_COLUMN_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_bind_get_column_type(
    a0: duckdb_copy_function_bind_info,
    a1: idx_t,
) -> duckdb_logical_type {
    let p = DUCKDB_COPY_FUNCTION_BIND_GET_COLUMN_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_bind_get_column_type: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function_bind_info, idx_t) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_COPY_FUNCTION_BIND_GET_OPTIONS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_bind_get_options(
    a0: duckdb_copy_function_bind_info
) -> duckdb_value {
    let p = DUCKDB_COPY_FUNCTION_BIND_GET_OPTIONS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_bind_get_options: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function_bind_info) -> duckdb_value =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_BIND_SET_BIND_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_bind_set_bind_data(
    a0: duckdb_copy_function_bind_info,
    a1: *mut ::std::os::raw::c_void,
    a2: duckdb_delete_callback_t,
) {
    let p = DUCKDB_COPY_FUNCTION_BIND_SET_BIND_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_bind_set_bind_data: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_copy_function_bind_info,
            *mut ::std::os::raw::c_void,
            duckdb_delete_callback_t,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_COPY_FUNCTION_SET_GLOBAL_INIT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_set_global_init(
    a0: duckdb_copy_function,
    a1: duckdb_copy_function_global_init_t,
) {
    let p = DUCKDB_COPY_FUNCTION_SET_GLOBAL_INIT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_set_global_init: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function, duckdb_copy_function_global_init_t) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_COPY_FUNCTION_GLOBAL_INIT_SET_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_global_init_set_error(
    a0: duckdb_copy_function_global_init_info,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_COPY_FUNCTION_GLOBAL_INIT_SET_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_global_init_set_error: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_copy_function_global_init_info,
            *const ::std::os::raw::c_char,
        ) = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_COPY_FUNCTION_GLOBAL_INIT_GET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_global_init_get_extra_info(
    a0: duckdb_copy_function_global_init_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_COPY_FUNCTION_GLOBAL_INIT_GET_EXTRA_INFO
        .load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_global_init_get_extra_info: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_copy_function_global_init_info,
        ) -> *mut ::std::os::raw::c_void = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_GLOBAL_INIT_GET_CLIENT_CONTEXT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_global_init_get_client_context(
    a0: duckdb_copy_function_global_init_info
) -> duckdb_client_context {
    let p = DUCKDB_COPY_FUNCTION_GLOBAL_INIT_GET_CLIENT_CONTEXT
        .load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_global_init_get_client_context: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_copy_function_global_init_info,
        ) -> duckdb_client_context = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_GLOBAL_INIT_GET_BIND_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_global_init_get_bind_data(
    a0: duckdb_copy_function_global_init_info
) -> *mut ::std::os::raw::c_void {
    let p =
        DUCKDB_COPY_FUNCTION_GLOBAL_INIT_GET_BIND_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_global_init_get_bind_data: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_copy_function_global_init_info,
        ) -> *mut ::std::os::raw::c_void = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_GLOBAL_INIT_SET_GLOBAL_STATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_global_init_set_global_state(
    a0: duckdb_copy_function_global_init_info,
    a1: *mut ::std::os::raw::c_void,
    a2: duckdb_delete_callback_t,
) {
    let p = DUCKDB_COPY_FUNCTION_GLOBAL_INIT_SET_GLOBAL_STATE
        .load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_global_init_set_global_state: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_copy_function_global_init_info,
            *mut ::std::os::raw::c_void,
            duckdb_delete_callback_t,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_COPY_FUNCTION_GLOBAL_INIT_GET_FILE_PATH: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_global_init_get_file_path(
    a0: duckdb_copy_function_global_init_info
) -> *const ::std::os::raw::c_char {
    let p =
        DUCKDB_COPY_FUNCTION_GLOBAL_INIT_GET_FILE_PATH.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_global_init_get_file_path: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_copy_function_global_init_info,
        ) -> *const ::std::os::raw::c_char = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_SET_SINK: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_set_sink(
    a0: duckdb_copy_function,
    a1: duckdb_copy_function_sink_t,
) {
    let p = DUCKDB_COPY_FUNCTION_SET_SINK.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_copy_function_set_sink: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function, duckdb_copy_function_sink_t) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_COPY_FUNCTION_SINK_SET_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_sink_set_error(
    a0: duckdb_copy_function_sink_info,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_COPY_FUNCTION_SINK_SET_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_sink_set_error: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function_sink_info, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_COPY_FUNCTION_SINK_GET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_sink_get_extra_info(
    a0: duckdb_copy_function_sink_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_COPY_FUNCTION_SINK_GET_EXTRA_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_sink_get_extra_info: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function_sink_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_SINK_GET_CLIENT_CONTEXT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_sink_get_client_context(
    a0: duckdb_copy_function_sink_info
) -> duckdb_client_context {
    let p =
        DUCKDB_COPY_FUNCTION_SINK_GET_CLIENT_CONTEXT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_sink_get_client_context: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function_sink_info) -> duckdb_client_context =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_SINK_GET_BIND_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_sink_get_bind_data(
    a0: duckdb_copy_function_sink_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_COPY_FUNCTION_SINK_GET_BIND_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_sink_get_bind_data: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function_sink_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_SINK_GET_GLOBAL_STATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_sink_get_global_state(
    a0: duckdb_copy_function_sink_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_COPY_FUNCTION_SINK_GET_GLOBAL_STATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_sink_get_global_state: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function_sink_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_SET_FINALIZE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_set_finalize(
    a0: duckdb_copy_function,
    a1: duckdb_copy_function_finalize_t,
) {
    let p = DUCKDB_COPY_FUNCTION_SET_FINALIZE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_set_finalize: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function, duckdb_copy_function_finalize_t) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_COPY_FUNCTION_FINALIZE_SET_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_finalize_set_error(
    a0: duckdb_copy_function_finalize_info,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_COPY_FUNCTION_FINALIZE_SET_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_finalize_set_error: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_copy_function_finalize_info,
            *const ::std::os::raw::c_char,
        ) = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_COPY_FUNCTION_FINALIZE_GET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_finalize_get_extra_info(
    a0: duckdb_copy_function_finalize_info
) -> *mut ::std::os::raw::c_void {
    let p =
        DUCKDB_COPY_FUNCTION_FINALIZE_GET_EXTRA_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_finalize_get_extra_info: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_copy_function_finalize_info,
        ) -> *mut ::std::os::raw::c_void = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_FINALIZE_GET_CLIENT_CONTEXT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_finalize_get_client_context(
    a0: duckdb_copy_function_finalize_info
) -> duckdb_client_context {
    let p = DUCKDB_COPY_FUNCTION_FINALIZE_GET_CLIENT_CONTEXT
        .load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_finalize_get_client_context: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function_finalize_info) -> duckdb_client_context =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_FINALIZE_GET_BIND_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_finalize_get_bind_data(
    a0: duckdb_copy_function_finalize_info
) -> *mut ::std::os::raw::c_void {
    let p =
        DUCKDB_COPY_FUNCTION_FINALIZE_GET_BIND_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_finalize_get_bind_data: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_copy_function_finalize_info,
        ) -> *mut ::std::os::raw::c_void = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_FINALIZE_GET_GLOBAL_STATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_finalize_get_global_state(
    a0: duckdb_copy_function_finalize_info
) -> *mut ::std::os::raw::c_void {
    let p =
        DUCKDB_COPY_FUNCTION_FINALIZE_GET_GLOBAL_STATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_finalize_get_global_state: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_copy_function_finalize_info,
        ) -> *mut ::std::os::raw::c_void = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_COPY_FUNCTION_SET_COPY_FROM_FUNCTION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_copy_function_set_copy_from_function(
    a0: duckdb_copy_function,
    a1: duckdb_table_function,
) {
    let p =
        DUCKDB_COPY_FUNCTION_SET_COPY_FROM_FUNCTION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_copy_function_set_copy_from_function: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_copy_function, duckdb_table_function) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_TABLE_FUNCTION_BIND_GET_RESULT_COLUMN_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_function_bind_get_result_column_count(a0: duckdb_bind_info) -> idx_t {
    let p = DUCKDB_TABLE_FUNCTION_BIND_GET_RESULT_COLUMN_COUNT
        .load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_table_function_bind_get_result_column_count: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bind_info) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_TABLE_FUNCTION_BIND_GET_RESULT_COLUMN_NAME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_function_bind_get_result_column_name(
    a0: duckdb_bind_info,
    a1: idx_t,
) -> *const ::std::os::raw::c_char {
    let p = DUCKDB_TABLE_FUNCTION_BIND_GET_RESULT_COLUMN_NAME
        .load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_table_function_bind_get_result_column_name: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bind_info, idx_t) -> *const ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_TABLE_FUNCTION_BIND_GET_RESULT_COLUMN_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_function_bind_get_result_column_type(
    a0: duckdb_bind_info,
    a1: idx_t,
) -> duckdb_logical_type {
    let p = DUCKDB_TABLE_FUNCTION_BIND_GET_RESULT_COLUMN_TYPE
        .load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_table_function_bind_get_result_column_type: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bind_info, idx_t) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_ERROR_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_error_data(
    a0: duckdb_error_type,
    a1: *const ::std::os::raw::c_char,
) -> duckdb_error_data {
    let p = DUCKDB_CREATE_ERROR_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_error_data: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_error_type,
            *const ::std::os::raw::c_char,
        ) -> duckdb_error_data = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_DESTROY_ERROR_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_error_data(a0: *mut duckdb_error_data) {
    let p = DUCKDB_DESTROY_ERROR_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_error_data: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_error_data) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_ERROR_DATA_ERROR_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_error_data_error_type(a0: duckdb_error_data) -> duckdb_error_type {
    let p = DUCKDB_ERROR_DATA_ERROR_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_error_data_error_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_error_data) -> duckdb_error_type =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_ERROR_DATA_MESSAGE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_error_data_message(a0: duckdb_error_data) -> *const ::std::os::raw::c_char {
    let p = DUCKDB_ERROR_DATA_MESSAGE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_error_data_message: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_error_data) -> *const ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_ERROR_DATA_HAS_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_error_data_has_error(a0: duckdb_error_data) -> bool {
    let p = DUCKDB_ERROR_DATA_HAS_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_error_data_has_error: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_error_data) -> bool = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DESTROY_EXPRESSION: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_expression(a0: *mut duckdb_expression) {
    let p = DUCKDB_DESTROY_EXPRESSION.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_expression: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_expression) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_EXPRESSION_RETURN_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_expression_return_type(a0: duckdb_expression) -> duckdb_logical_type {
    let p = DUCKDB_EXPRESSION_RETURN_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_expression_return_type: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_expression) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_EXPRESSION_IS_FOLDABLE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_expression_is_foldable(a0: duckdb_expression) -> bool {
    let p = DUCKDB_EXPRESSION_IS_FOLDABLE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_expression_is_foldable: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_expression) -> bool = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_EXPRESSION_FOLD: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_expression_fold(
    a0: duckdb_client_context,
    a1: duckdb_expression,
    a2: *mut duckdb_value,
) -> duckdb_error_data {
    let p = DUCKDB_EXPRESSION_FOLD.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_expression_fold: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_client_context,
            duckdb_expression,
            *mut duckdb_value,
        ) -> duckdb_error_data = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_CLIENT_CONTEXT_GET_FILE_SYSTEM: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_client_context_get_file_system(
    a0: duckdb_client_context
) -> duckdb_file_system {
    let p = DUCKDB_CLIENT_CONTEXT_GET_FILE_SYSTEM.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_client_context_get_file_system: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_client_context) -> duckdb_file_system =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DESTROY_FILE_SYSTEM: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_file_system(a0: *mut duckdb_file_system) {
    let p = DUCKDB_DESTROY_FILE_SYSTEM.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_file_system: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_file_system) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_FILE_SYSTEM_OPEN: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_file_system_open(
    a0: duckdb_file_system,
    a1: *const ::std::os::raw::c_char,
    a2: duckdb_file_open_options,
    a3: *mut duckdb_file_handle,
) -> duckdb_state {
    let p = DUCKDB_FILE_SYSTEM_OPEN.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_file_system_open: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_file_system,
            *const ::std::os::raw::c_char,
            duckdb_file_open_options,
            *mut duckdb_file_handle,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2, a3)
    }
}
static DUCKDB_FILE_SYSTEM_ERROR_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_file_system_error_data(a0: duckdb_file_system) -> duckdb_error_data {
    let p = DUCKDB_FILE_SYSTEM_ERROR_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_file_system_error_data: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_file_system) -> duckdb_error_data =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_FILE_OPEN_OPTIONS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_file_open_options() -> duckdb_file_open_options {
    let p = DUCKDB_CREATE_FILE_OPEN_OPTIONS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_file_open_options: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn() -> duckdb_file_open_options = ::std::mem::transmute(p);
        f()
    }
}
static DUCKDB_FILE_OPEN_OPTIONS_SET_FLAG: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_file_open_options_set_flag(
    a0: duckdb_file_open_options,
    a1: duckdb_file_flag,
    a2: bool,
) -> duckdb_state {
    let p = DUCKDB_FILE_OPEN_OPTIONS_SET_FLAG.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_file_open_options_set_flag: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_file_open_options,
            duckdb_file_flag,
            bool,
        ) -> duckdb_state = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_DESTROY_FILE_OPEN_OPTIONS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_file_open_options(a0: *mut duckdb_file_open_options) {
    let p = DUCKDB_DESTROY_FILE_OPEN_OPTIONS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_file_open_options: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_file_open_options) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DESTROY_FILE_HANDLE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_file_handle(a0: *mut duckdb_file_handle) {
    let p = DUCKDB_DESTROY_FILE_HANDLE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_file_handle: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_file_handle) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_FILE_HANDLE_ERROR_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_file_handle_error_data(a0: duckdb_file_handle) -> duckdb_error_data {
    let p = DUCKDB_FILE_HANDLE_ERROR_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_file_handle_error_data: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_file_handle) -> duckdb_error_data =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_FILE_HANDLE_CLOSE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_file_handle_close(a0: duckdb_file_handle) -> duckdb_state {
    let p = DUCKDB_FILE_HANDLE_CLOSE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_file_handle_close: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_file_handle) -> duckdb_state = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_FILE_HANDLE_READ: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_file_handle_read(
    a0: duckdb_file_handle,
    a1: *mut ::std::os::raw::c_void,
    a2: i64,
) -> i64 {
    let p = DUCKDB_FILE_HANDLE_READ.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_file_handle_read: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_file_handle, *mut ::std::os::raw::c_void, i64) -> i64 =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_FILE_HANDLE_WRITE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_file_handle_write(
    a0: duckdb_file_handle,
    a1: *const ::std::os::raw::c_void,
    a2: i64,
) -> i64 {
    let p = DUCKDB_FILE_HANDLE_WRITE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_file_handle_write: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_file_handle, *const ::std::os::raw::c_void, i64) -> i64 =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_FILE_HANDLE_SEEK: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_file_handle_seek(
    a0: duckdb_file_handle,
    a1: i64,
) -> duckdb_state {
    let p = DUCKDB_FILE_HANDLE_SEEK.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_file_handle_seek: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_file_handle, i64) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_FILE_HANDLE_TELL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_file_handle_tell(a0: duckdb_file_handle) -> i64 {
    let p = DUCKDB_FILE_HANDLE_TELL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_file_handle_tell: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_file_handle) -> i64 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_FILE_HANDLE_SYNC: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_file_handle_sync(a0: duckdb_file_handle) -> duckdb_state {
    let p = DUCKDB_FILE_HANDLE_SYNC.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_file_handle_sync: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_file_handle) -> duckdb_state = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_FILE_HANDLE_SIZE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_file_handle_size(a0: duckdb_file_handle) -> i64 {
    let p = DUCKDB_FILE_HANDLE_SIZE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_file_handle_size: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_file_handle) -> i64 = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GEOMETRY_TYPE_GET_CRS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_geometry_type_get_crs(a0: duckdb_logical_type) -> *mut ::std::os::raw::c_char {
    let p = DUCKDB_GEOMETRY_TYPE_GET_CRS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_geometry_type_get_crs: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type) -> *mut ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_LOG_STORAGE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_log_storage() -> duckdb_log_storage {
    let p = DUCKDB_CREATE_LOG_STORAGE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_log_storage: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn() -> duckdb_log_storage = ::std::mem::transmute(p);
        f()
    }
}
static DUCKDB_DESTROY_LOG_STORAGE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_log_storage(a0: *mut duckdb_log_storage) {
    let p = DUCKDB_DESTROY_LOG_STORAGE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_log_storage: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_log_storage) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_LOG_STORAGE_SET_WRITE_LOG_ENTRY: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_log_storage_set_write_log_entry(
    a0: duckdb_log_storage,
    a1: duckdb_logger_write_log_entry_t,
) {
    let p = DUCKDB_LOG_STORAGE_SET_WRITE_LOG_ENTRY.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_log_storage_set_write_log_entry: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_log_storage, duckdb_logger_write_log_entry_t) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_LOG_STORAGE_SET_EXTRA_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_log_storage_set_extra_data(
    a0: duckdb_log_storage,
    a1: *mut ::std::os::raw::c_void,
    a2: duckdb_delete_callback_t,
) {
    let p = DUCKDB_LOG_STORAGE_SET_EXTRA_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_log_storage_set_extra_data: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_log_storage,
            *mut ::std::os::raw::c_void,
            duckdb_delete_callback_t,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_LOG_STORAGE_SET_NAME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_log_storage_set_name(
    a0: duckdb_log_storage,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_LOG_STORAGE_SET_NAME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_log_storage_set_name: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_log_storage, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_REGISTER_LOG_STORAGE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_register_log_storage(
    a0: duckdb_database,
    a1: duckdb_log_storage,
) -> duckdb_state {
    let p = DUCKDB_REGISTER_LOG_STORAGE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_register_log_storage: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_database, duckdb_log_storage) -> duckdb_state =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CLIENT_CONTEXT_GET_CONNECTION_ID: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_client_context_get_connection_id(a0: duckdb_client_context) -> idx_t {
    let p = DUCKDB_CLIENT_CONTEXT_GET_CONNECTION_ID.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_client_context_get_connection_id: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_client_context) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DESTROY_CLIENT_CONTEXT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_client_context(a0: *mut duckdb_client_context) {
    let p = DUCKDB_DESTROY_CLIENT_CONTEXT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_client_context: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_client_context) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CONNECTION_GET_CLIENT_CONTEXT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_connection_get_client_context(
    a0: duckdb_connection,
    a1: *mut duckdb_client_context,
) {
    let p = DUCKDB_CONNECTION_GET_CLIENT_CONTEXT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_connection_get_client_context: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_connection, *mut duckdb_client_context) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_GET_TABLE_NAMES: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_table_names(
    a0: duckdb_connection,
    a1: *const ::std::os::raw::c_char,
    a2: bool,
) -> duckdb_value {
    let p = DUCKDB_GET_TABLE_NAMES.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_table_names: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_connection,
            *const ::std::os::raw::c_char,
            bool,
        ) -> duckdb_value = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_CONNECTION_GET_ARROW_OPTIONS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_connection_get_arrow_options(
    a0: duckdb_connection,
    a1: *mut duckdb_arrow_options,
) {
    let p = DUCKDB_CONNECTION_GET_ARROW_OPTIONS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_connection_get_arrow_options: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_connection, *mut duckdb_arrow_options) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_DESTROY_ARROW_OPTIONS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_arrow_options(a0: *mut duckdb_arrow_options) {
    let p = DUCKDB_DESTROY_ARROW_OPTIONS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_arrow_options: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_arrow_options) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_PREPARED_STATEMENT_COLUMN_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_prepared_statement_column_count(a0: duckdb_prepared_statement) -> idx_t {
    let p = DUCKDB_PREPARED_STATEMENT_COLUMN_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_prepared_statement_column_count: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_PREPARED_STATEMENT_COLUMN_NAME: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_prepared_statement_column_name(
    a0: duckdb_prepared_statement,
    a1: idx_t,
) -> *const ::std::os::raw::c_char {
    let p = DUCKDB_PREPARED_STATEMENT_COLUMN_NAME.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_prepared_statement_column_name: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_prepared_statement,
            idx_t,
        ) -> *const ::std::os::raw::c_char = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_PREPARED_STATEMENT_COLUMN_LOGICAL_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_prepared_statement_column_logical_type(
    a0: duckdb_prepared_statement,
    a1: idx_t,
) -> duckdb_logical_type {
    let p =
        DUCKDB_PREPARED_STATEMENT_COLUMN_LOGICAL_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_prepared_statement_column_logical_type: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_PREPARED_STATEMENT_COLUMN_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_prepared_statement_column_type(
    a0: duckdb_prepared_statement,
    a1: idx_t,
) -> duckdb_type {
    let p = DUCKDB_PREPARED_STATEMENT_COLUMN_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_prepared_statement_column_type: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_prepared_statement, idx_t) -> duckdb_type =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_RESULT_GET_ARROW_OPTIONS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_result_get_arrow_options(a0: *mut duckdb_result) -> duckdb_arrow_options {
    let p = DUCKDB_RESULT_GET_ARROW_OPTIONS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_result_get_arrow_options: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_result) -> duckdb_arrow_options =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_SCALAR_FUNCTION_SET_BIND: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_set_bind(
    a0: duckdb_scalar_function,
    a1: duckdb_scalar_function_bind_t,
) {
    let p = DUCKDB_SCALAR_FUNCTION_SET_BIND.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_scalar_function_set_bind: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_scalar_function, duckdb_scalar_function_bind_t) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_SCALAR_FUNCTION_BIND_SET_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_bind_set_error(
    a0: duckdb_bind_info,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_SCALAR_FUNCTION_BIND_SET_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_bind_set_error: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bind_info, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_SCALAR_FUNCTION_GET_CLIENT_CONTEXT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_get_client_context(
    a0: duckdb_bind_info,
    a1: *mut duckdb_client_context,
) {
    let p = DUCKDB_SCALAR_FUNCTION_GET_CLIENT_CONTEXT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_get_client_context: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bind_info, *mut duckdb_client_context) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_SCALAR_FUNCTION_SET_BIND_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_set_bind_data(
    a0: duckdb_bind_info,
    a1: *mut ::std::os::raw::c_void,
    a2: duckdb_delete_callback_t,
) {
    let p = DUCKDB_SCALAR_FUNCTION_SET_BIND_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_set_bind_data: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_bind_info,
            *mut ::std::os::raw::c_void,
            duckdb_delete_callback_t,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_SCALAR_FUNCTION_GET_BIND_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_get_bind_data(
    a0: duckdb_function_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_SCALAR_FUNCTION_GET_BIND_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_get_bind_data: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_function_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_SCALAR_FUNCTION_BIND_GET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_bind_get_extra_info(
    a0: duckdb_bind_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_SCALAR_FUNCTION_BIND_GET_EXTRA_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_bind_get_extra_info: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bind_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_SCALAR_FUNCTION_BIND_GET_ARGUMENT_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_bind_get_argument_count(a0: duckdb_bind_info) -> idx_t {
    let p =
        DUCKDB_SCALAR_FUNCTION_BIND_GET_ARGUMENT_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_bind_get_argument_count: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bind_info) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_SCALAR_FUNCTION_BIND_GET_ARGUMENT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_bind_get_argument(
    a0: duckdb_bind_info,
    a1: idx_t,
) -> duckdb_expression {
    let p = DUCKDB_SCALAR_FUNCTION_BIND_GET_ARGUMENT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_bind_get_argument: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bind_info, idx_t) -> duckdb_expression =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_SCALAR_FUNCTION_SET_BIND_DATA_COPY: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_set_bind_data_copy(
    a0: duckdb_bind_info,
    a1: duckdb_copy_callback_t,
) {
    let p = DUCKDB_SCALAR_FUNCTION_SET_BIND_DATA_COPY.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_set_bind_data_copy: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bind_info, duckdb_copy_callback_t) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_SCALAR_FUNCTION_GET_STATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_get_state(
    a0: duckdb_function_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_SCALAR_FUNCTION_GET_STATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_scalar_function_get_state: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_function_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_SCALAR_FUNCTION_SET_INIT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_set_init(
    a0: duckdb_scalar_function,
    a1: duckdb_scalar_function_init_t,
) {
    let p = DUCKDB_SCALAR_FUNCTION_SET_INIT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_scalar_function_set_init: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_scalar_function, duckdb_scalar_function_init_t) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_SCALAR_FUNCTION_INIT_SET_ERROR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_init_set_error(
    a0: duckdb_init_info,
    a1: *const ::std::os::raw::c_char,
) {
    let p = DUCKDB_SCALAR_FUNCTION_INIT_SET_ERROR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_init_set_error: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_init_info, *const ::std::os::raw::c_char) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_SCALAR_FUNCTION_INIT_SET_STATE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_init_set_state(
    a0: duckdb_init_info,
    a1: *mut ::std::os::raw::c_void,
    a2: duckdb_delete_callback_t,
) {
    let p = DUCKDB_SCALAR_FUNCTION_INIT_SET_STATE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_init_set_state: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_init_info,
            *mut ::std::os::raw::c_void,
            duckdb_delete_callback_t,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_SCALAR_FUNCTION_INIT_GET_CLIENT_CONTEXT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_init_get_client_context(
    a0: duckdb_init_info,
    a1: *mut duckdb_client_context,
) {
    let p =
        DUCKDB_SCALAR_FUNCTION_INIT_GET_CLIENT_CONTEXT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_init_get_client_context: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_init_info, *mut duckdb_client_context) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_SCALAR_FUNCTION_INIT_GET_BIND_DATA: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_init_get_bind_data(
    a0: duckdb_init_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_SCALAR_FUNCTION_INIT_GET_BIND_DATA.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_init_get_bind_data: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_init_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_SCALAR_FUNCTION_INIT_GET_EXTRA_INFO: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_scalar_function_init_get_extra_info(
    a0: duckdb_init_info
) -> *mut ::std::os::raw::c_void {
    let p = DUCKDB_SCALAR_FUNCTION_INIT_GET_EXTRA_INFO.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_scalar_function_init_get_extra_info: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_init_info) -> *mut ::std::os::raw::c_void =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_VALUE_TO_STRING: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_value_to_string(a0: duckdb_value) -> *mut ::std::os::raw::c_char {
    let p = DUCKDB_VALUE_TO_STRING.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_value_to_string: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> *mut ::std::os::raw::c_char =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_VALID_UTF8_CHECK: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_valid_utf8_check(
    a0: *const ::std::os::raw::c_char,
    a1: idx_t,
) -> duckdb_error_data {
    let p = DUCKDB_VALID_UTF8_CHECK.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_valid_utf8_check: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*const ::std::os::raw::c_char, idx_t) -> duckdb_error_data =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_TABLE_DESCRIPTION_GET_COLUMN_COUNT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_description_get_column_count(a0: duckdb_table_description) -> idx_t {
    let p = DUCKDB_TABLE_DESCRIPTION_GET_COLUMN_COUNT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_table_description_get_column_count: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_table_description) -> idx_t = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_TABLE_DESCRIPTION_GET_COLUMN_TYPE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_description_get_column_type(
    a0: duckdb_table_description,
    a1: idx_t,
) -> duckdb_logical_type {
    let p = DUCKDB_TABLE_DESCRIPTION_GET_COLUMN_TYPE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_table_description_get_column_type: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_table_description, idx_t) -> duckdb_logical_type =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_TABLE_FUNCTION_GET_CLIENT_CONTEXT: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_table_function_get_client_context(
    a0: duckdb_bind_info,
    a1: *mut duckdb_client_context,
) {
    let p = DUCKDB_TABLE_FUNCTION_GET_CLIENT_CONTEXT.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_table_function_get_client_context: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_bind_info, *mut duckdb_client_context) =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_MAP_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_map_value(
    a0: duckdb_logical_type,
    a1: *mut duckdb_value,
    a2: *mut duckdb_value,
    a3: idx_t,
) -> duckdb_value {
    let p = DUCKDB_CREATE_MAP_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_map_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_logical_type,
            *mut duckdb_value,
            *mut duckdb_value,
            idx_t,
        ) -> duckdb_value = ::std::mem::transmute(p);
        f(a0, a1, a2, a3)
    }
}
static DUCKDB_CREATE_UNION_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_union_value(
    a0: duckdb_logical_type,
    a1: idx_t,
    a2: duckdb_value,
) -> duckdb_value {
    let p = DUCKDB_CREATE_UNION_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_union_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type, idx_t, duckdb_value) -> duckdb_value =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_CREATE_TIME_NS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_time_ns(a0: duckdb_time_ns) -> duckdb_value {
    let p = DUCKDB_CREATE_TIME_NS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_time_ns: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_time_ns) -> duckdb_value = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_GET_TIME_NS: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_get_time_ns(a0: duckdb_value) -> duckdb_time_ns {
    let p = DUCKDB_GET_TIME_NS.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_get_time_ns: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_value) -> duckdb_time_ns = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_CREATE_VECTOR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_vector(
    a0: duckdb_logical_type,
    a1: idx_t,
) -> duckdb_vector {
    let p = DUCKDB_CREATE_VECTOR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_vector: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_logical_type, idx_t) -> duckdb_vector =
            ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_DESTROY_VECTOR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_vector(a0: *mut duckdb_vector) {
    let p = DUCKDB_DESTROY_VECTOR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_vector: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(*mut duckdb_vector) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_SLICE_VECTOR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_slice_vector(
    a0: duckdb_vector,
    a1: duckdb_selection_vector,
    a2: idx_t,
) {
    let p = DUCKDB_SLICE_VECTOR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_slice_vector: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_vector, duckdb_selection_vector, idx_t) =
            ::std::mem::transmute(p);
        f(a0, a1, a2)
    }
}
static DUCKDB_VECTOR_REFERENCE_VALUE: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_vector_reference_value(
    a0: duckdb_vector,
    a1: duckdb_value,
) {
    let p = DUCKDB_VECTOR_REFERENCE_VALUE.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_vector_reference_value: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_vector, duckdb_value) = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_VECTOR_REFERENCE_VECTOR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_vector_reference_vector(
    a0: duckdb_vector,
    a1: duckdb_vector,
) {
    let p = DUCKDB_VECTOR_REFERENCE_VECTOR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_vector_reference_vector: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_vector, duckdb_vector) = ::std::mem::transmute(p);
        f(a0, a1)
    }
}
static DUCKDB_CREATE_SELECTION_VECTOR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_create_selection_vector(a0: idx_t) -> duckdb_selection_vector {
    let p = DUCKDB_CREATE_SELECTION_VECTOR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_create_selection_vector: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(idx_t) -> duckdb_selection_vector = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_DESTROY_SELECTION_VECTOR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_destroy_selection_vector(a0: duckdb_selection_vector) {
    let p = DUCKDB_DESTROY_SELECTION_VECTOR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_destroy_selection_vector: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(duckdb_selection_vector) = ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_SELECTION_VECTOR_GET_DATA_PTR: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_selection_vector_get_data_ptr(a0: duckdb_selection_vector) -> *mut sel_t {
    let p = DUCKDB_SELECTION_VECTOR_GET_DATA_PTR.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_selection_vector_get_data_ptr: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_selection_vector) -> *mut sel_t =
            ::std::mem::transmute(p);
        f(a0)
    }
}
static DUCKDB_VECTOR_COPY_SEL: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_vector_copy_sel(
    a0: duckdb_vector,
    a1: duckdb_vector,
    a2: duckdb_selection_vector,
    a3: idx_t,
    a4: idx_t,
    a5: idx_t,
) {
    let p = DUCKDB_VECTOR_COPY_SEL.load(::std::sync::atomic::Ordering::Acquire);
    assert!(!p.is_null(), "duckdb_vector_copy_sel: DuckDB extension API not initialized");
    unsafe {
        let f: unsafe extern "C" fn(
            duckdb_vector,
            duckdb_vector,
            duckdb_selection_vector,
            idx_t,
            idx_t,
            idx_t,
        ) = ::std::mem::transmute(p);
        f(a0, a1, a2, a3, a4, a5)
    }
}
static DUCKDB_UNSAFE_VECTOR_ASSIGN_STRING_ELEMENT_LEN: ::std::sync::atomic::AtomicPtr<()> =
    ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
/// Loadable-extension wrapper dispatching through the host's API table.
/// # Safety
/// The API must be initialized (`duckdb_rs_extension_api_init`) and the
/// arguments valid per the DuckDB C API.
pub unsafe fn duckdb_unsafe_vector_assign_string_element_len(
    a0: duckdb_vector,
    a1: idx_t,
    a2: *const ::std::os::raw::c_char,
    a3: idx_t,
) {
    let p =
        DUCKDB_UNSAFE_VECTOR_ASSIGN_STRING_ELEMENT_LEN.load(::std::sync::atomic::Ordering::Acquire);
    assert!(
        !p.is_null(),
        "duckdb_unsafe_vector_assign_string_element_len: DuckDB extension API not initialized"
    );
    unsafe {
        let f: unsafe extern "C" fn(duckdb_vector, idx_t, *const ::std::os::raw::c_char, idx_t) =
            ::std::mem::transmute(p);
        f(a0, a1, a2, a3)
    }
}
/// Populates the loadable-extension API table from the host `access` struct.
/// Returns `Ok(false)` on an API version mismatch (host returns a null table).
/// # Safety
/// `info`/`access` must be the handles DuckDB's loader passed to the entrypoint.
pub unsafe fn duckdb_rs_extension_api_init(
    info: duckdb_extension_info,
    access: *const duckdb_extension_access,
    minimum_version: &str,
) -> ::std::result::Result<bool, ::std::boxed::Box<dyn ::std::error::Error>> {
    let cversion = ::std::ffi::CString::new(minimum_version)?;
    unsafe {
        let get_api = (*access).get_api.ok_or("duckdb_extension_access.get_api is null")?;
        let api_ptr = get_api(info, cversion.as_ptr()) as *const duckdb_ext_api_v1;
        if api_ptr.is_null() {
            return ::std::result::Result::Ok(false);
        }
        let api = &*api_ptr;
        DUCKDB_OPEN.store(
            api.duckdb_open.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_OPEN_EXT.store(
            api.duckdb_open_ext.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CLOSE.store(
            api.duckdb_close.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CONNECT.store(
            api.duckdb_connect.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_INTERRUPT.store(
            api.duckdb_interrupt.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_QUERY_PROGRESS.store(
            api.duckdb_query_progress.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DISCONNECT.store(
            api.duckdb_disconnect.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_LIBRARY_VERSION.store(
            api.duckdb_library_version.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_CONFIG.store(
            api.duckdb_create_config.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CONFIG_COUNT.store(
            api.duckdb_config_count.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_CONFIG_FLAG.store(
            api.duckdb_get_config_flag.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SET_CONFIG.store(
            api.duckdb_set_config.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_CONFIG.store(
            api.duckdb_destroy_config.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_QUERY.store(
            api.duckdb_query.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_RESULT.store(
            api.duckdb_destroy_result.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COLUMN_NAME.store(
            api.duckdb_column_name.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COLUMN_TYPE.store(
            api.duckdb_column_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_RESULT_STATEMENT_TYPE.store(
            api.duckdb_result_statement_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COLUMN_LOGICAL_TYPE.store(
            api.duckdb_column_logical_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COLUMN_COUNT.store(
            api.duckdb_column_count.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ROWS_CHANGED.store(
            api.duckdb_rows_changed.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_RESULT_ERROR.store(
            api.duckdb_result_error.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_RESULT_ERROR_TYPE.store(
            api.duckdb_result_error_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_RESULT_RETURN_TYPE.store(
            api.duckdb_result_return_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_MALLOC.store(
            api.duckdb_malloc.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FREE.store(
            api.duckdb_free.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VECTOR_SIZE.store(
            api.duckdb_vector_size.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_STRING_IS_INLINED.store(
            api.duckdb_string_is_inlined.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_STRING_T_LENGTH.store(
            api.duckdb_string_t_length.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_STRING_T_DATA.store(
            api.duckdb_string_t_data.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FROM_DATE.store(
            api.duckdb_from_date.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TO_DATE.store(
            api.duckdb_to_date.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_IS_FINITE_DATE.store(
            api.duckdb_is_finite_date.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FROM_TIME.store(
            api.duckdb_from_time.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_TIME_TZ.store(
            api.duckdb_create_time_tz.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FROM_TIME_TZ.store(
            api.duckdb_from_time_tz.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TO_TIME.store(
            api.duckdb_to_time.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FROM_TIMESTAMP.store(
            api.duckdb_from_timestamp.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TO_TIMESTAMP.store(
            api.duckdb_to_timestamp.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_IS_FINITE_TIMESTAMP.store(
            api.duckdb_is_finite_timestamp.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_HUGEINT_TO_DOUBLE.store(
            api.duckdb_hugeint_to_double.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DOUBLE_TO_HUGEINT.store(
            api.duckdb_double_to_hugeint.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_UHUGEINT_TO_DOUBLE.store(
            api.duckdb_uhugeint_to_double.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DOUBLE_TO_UHUGEINT.store(
            api.duckdb_double_to_uhugeint.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DOUBLE_TO_DECIMAL.store(
            api.duckdb_double_to_decimal.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DECIMAL_TO_DOUBLE.store(
            api.duckdb_decimal_to_double.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PREPARE.store(
            api.duckdb_prepare.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_PREPARE.store(
            api.duckdb_destroy_prepare.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PREPARE_ERROR.store(
            api.duckdb_prepare_error.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_NPARAMS.store(
            api.duckdb_nparams.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PARAMETER_NAME.store(
            api.duckdb_parameter_name.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PARAM_TYPE.store(
            api.duckdb_param_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PARAM_LOGICAL_TYPE.store(
            api.duckdb_param_logical_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CLEAR_BINDINGS.store(
            api.duckdb_clear_bindings.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PREPARED_STATEMENT_TYPE.store(
            api.duckdb_prepared_statement_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_VALUE.store(
            api.duckdb_bind_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_PARAMETER_INDEX.store(
            api.duckdb_bind_parameter_index.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_BOOLEAN.store(
            api.duckdb_bind_boolean.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_INT8.store(
            api.duckdb_bind_int8.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_INT16.store(
            api.duckdb_bind_int16.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_INT32.store(
            api.duckdb_bind_int32.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_INT64.store(
            api.duckdb_bind_int64.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_HUGEINT.store(
            api.duckdb_bind_hugeint.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_UHUGEINT.store(
            api.duckdb_bind_uhugeint.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_DECIMAL.store(
            api.duckdb_bind_decimal.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_UINT8.store(
            api.duckdb_bind_uint8.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_UINT16.store(
            api.duckdb_bind_uint16.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_UINT32.store(
            api.duckdb_bind_uint32.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_UINT64.store(
            api.duckdb_bind_uint64.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_FLOAT.store(
            api.duckdb_bind_float.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_DOUBLE.store(
            api.duckdb_bind_double.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_DATE.store(
            api.duckdb_bind_date.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_TIME.store(
            api.duckdb_bind_time.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_TIMESTAMP.store(
            api.duckdb_bind_timestamp.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_TIMESTAMP_TZ.store(
            api.duckdb_bind_timestamp_tz.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_INTERVAL.store(
            api.duckdb_bind_interval.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_VARCHAR.store(
            api.duckdb_bind_varchar.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_VARCHAR_LENGTH.store(
            api.duckdb_bind_varchar_length.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_BLOB.store(
            api.duckdb_bind_blob.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_NULL.store(
            api.duckdb_bind_null.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_EXECUTE_PREPARED.store(
            api.duckdb_execute_prepared.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_EXTRACT_STATEMENTS.store(
            api.duckdb_extract_statements.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PREPARE_EXTRACTED_STATEMENT.store(
            api.duckdb_prepare_extracted_statement.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_EXTRACT_STATEMENTS_ERROR.store(
            api.duckdb_extract_statements_error.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_EXTRACTED.store(
            api.duckdb_destroy_extracted.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PENDING_PREPARED.store(
            api.duckdb_pending_prepared.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_PENDING.store(
            api.duckdb_destroy_pending.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PENDING_ERROR.store(
            api.duckdb_pending_error.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PENDING_EXECUTE_TASK.store(
            api.duckdb_pending_execute_task.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PENDING_EXECUTE_CHECK_STATE.store(
            api.duckdb_pending_execute_check_state.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_EXECUTE_PENDING.store(
            api.duckdb_execute_pending.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PENDING_EXECUTION_IS_FINISHED.store(
            api.duckdb_pending_execution_is_finished
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_VALUE.store(
            api.duckdb_destroy_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_VARCHAR.store(
            api.duckdb_create_varchar.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_VARCHAR_LENGTH.store(
            api.duckdb_create_varchar_length.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_BOOL.store(
            api.duckdb_create_bool.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_INT8.store(
            api.duckdb_create_int8.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_UINT8.store(
            api.duckdb_create_uint8.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_INT16.store(
            api.duckdb_create_int16.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_UINT16.store(
            api.duckdb_create_uint16.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_INT32.store(
            api.duckdb_create_int32.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_UINT32.store(
            api.duckdb_create_uint32.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_UINT64.store(
            api.duckdb_create_uint64.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_INT64.store(
            api.duckdb_create_int64.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_HUGEINT.store(
            api.duckdb_create_hugeint.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_UHUGEINT.store(
            api.duckdb_create_uhugeint.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_FLOAT.store(
            api.duckdb_create_float.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_DOUBLE.store(
            api.duckdb_create_double.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_DATE.store(
            api.duckdb_create_date.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_TIME.store(
            api.duckdb_create_time.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_TIME_TZ_VALUE.store(
            api.duckdb_create_time_tz_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_TIMESTAMP.store(
            api.duckdb_create_timestamp.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_INTERVAL.store(
            api.duckdb_create_interval.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_BLOB.store(
            api.duckdb_create_blob.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_BIGNUM.store(
            api.duckdb_create_bignum.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_DECIMAL.store(
            api.duckdb_create_decimal.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_BIT.store(
            api.duckdb_create_bit.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_UUID.store(
            api.duckdb_create_uuid.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_BOOL.store(
            api.duckdb_get_bool.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_INT8.store(
            api.duckdb_get_int8.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_UINT8.store(
            api.duckdb_get_uint8.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_INT16.store(
            api.duckdb_get_int16.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_UINT16.store(
            api.duckdb_get_uint16.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_INT32.store(
            api.duckdb_get_int32.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_UINT32.store(
            api.duckdb_get_uint32.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_INT64.store(
            api.duckdb_get_int64.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_UINT64.store(
            api.duckdb_get_uint64.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_HUGEINT.store(
            api.duckdb_get_hugeint.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_UHUGEINT.store(
            api.duckdb_get_uhugeint.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_FLOAT.store(
            api.duckdb_get_float.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_DOUBLE.store(
            api.duckdb_get_double.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_DATE.store(
            api.duckdb_get_date.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_TIME.store(
            api.duckdb_get_time.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_TIME_TZ.store(
            api.duckdb_get_time_tz.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_TIMESTAMP.store(
            api.duckdb_get_timestamp.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_INTERVAL.store(
            api.duckdb_get_interval.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_VALUE_TYPE.store(
            api.duckdb_get_value_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_BLOB.store(
            api.duckdb_get_blob.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_BIGNUM.store(
            api.duckdb_get_bignum.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_DECIMAL.store(
            api.duckdb_get_decimal.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_BIT.store(
            api.duckdb_get_bit.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_UUID.store(
            api.duckdb_get_uuid.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_VARCHAR.store(
            api.duckdb_get_varchar.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_STRUCT_VALUE.store(
            api.duckdb_create_struct_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_LIST_VALUE.store(
            api.duckdb_create_list_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_ARRAY_VALUE.store(
            api.duckdb_create_array_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_MAP_SIZE.store(
            api.duckdb_get_map_size.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_MAP_KEY.store(
            api.duckdb_get_map_key.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_MAP_VALUE.store(
            api.duckdb_get_map_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_IS_NULL_VALUE.store(
            api.duckdb_is_null_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_NULL_VALUE.store(
            api.duckdb_create_null_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_LIST_SIZE.store(
            api.duckdb_get_list_size.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_LIST_CHILD.store(
            api.duckdb_get_list_child.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_ENUM_VALUE.store(
            api.duckdb_create_enum_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_ENUM_VALUE.store(
            api.duckdb_get_enum_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_STRUCT_CHILD.store(
            api.duckdb_get_struct_child.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_LOGICAL_TYPE.store(
            api.duckdb_create_logical_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_LOGICAL_TYPE_GET_ALIAS.store(
            api.duckdb_logical_type_get_alias.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_LOGICAL_TYPE_SET_ALIAS.store(
            api.duckdb_logical_type_set_alias.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_LIST_TYPE.store(
            api.duckdb_create_list_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_ARRAY_TYPE.store(
            api.duckdb_create_array_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_MAP_TYPE.store(
            api.duckdb_create_map_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_UNION_TYPE.store(
            api.duckdb_create_union_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_STRUCT_TYPE.store(
            api.duckdb_create_struct_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_ENUM_TYPE.store(
            api.duckdb_create_enum_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_DECIMAL_TYPE.store(
            api.duckdb_create_decimal_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_TYPE_ID.store(
            api.duckdb_get_type_id.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DECIMAL_WIDTH.store(
            api.duckdb_decimal_width.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DECIMAL_SCALE.store(
            api.duckdb_decimal_scale.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DECIMAL_INTERNAL_TYPE.store(
            api.duckdb_decimal_internal_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ENUM_INTERNAL_TYPE.store(
            api.duckdb_enum_internal_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ENUM_DICTIONARY_SIZE.store(
            api.duckdb_enum_dictionary_size.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ENUM_DICTIONARY_VALUE.store(
            api.duckdb_enum_dictionary_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_LIST_TYPE_CHILD_TYPE.store(
            api.duckdb_list_type_child_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ARRAY_TYPE_CHILD_TYPE.store(
            api.duckdb_array_type_child_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ARRAY_TYPE_ARRAY_SIZE.store(
            api.duckdb_array_type_array_size.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_MAP_TYPE_KEY_TYPE.store(
            api.duckdb_map_type_key_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_MAP_TYPE_VALUE_TYPE.store(
            api.duckdb_map_type_value_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_STRUCT_TYPE_CHILD_COUNT.store(
            api.duckdb_struct_type_child_count.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_STRUCT_TYPE_CHILD_NAME.store(
            api.duckdb_struct_type_child_name.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_STRUCT_TYPE_CHILD_TYPE.store(
            api.duckdb_struct_type_child_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_UNION_TYPE_MEMBER_COUNT.store(
            api.duckdb_union_type_member_count.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_UNION_TYPE_MEMBER_NAME.store(
            api.duckdb_union_type_member_name.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_UNION_TYPE_MEMBER_TYPE.store(
            api.duckdb_union_type_member_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_LOGICAL_TYPE.store(
            api.duckdb_destroy_logical_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_REGISTER_LOGICAL_TYPE.store(
            api.duckdb_register_logical_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_DATA_CHUNK.store(
            api.duckdb_create_data_chunk.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_DATA_CHUNK.store(
            api.duckdb_destroy_data_chunk.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DATA_CHUNK_RESET.store(
            api.duckdb_data_chunk_reset.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DATA_CHUNK_GET_COLUMN_COUNT.store(
            api.duckdb_data_chunk_get_column_count.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DATA_CHUNK_GET_VECTOR.store(
            api.duckdb_data_chunk_get_vector.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DATA_CHUNK_GET_SIZE.store(
            api.duckdb_data_chunk_get_size.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DATA_CHUNK_SET_SIZE.store(
            api.duckdb_data_chunk_set_size.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VECTOR_GET_COLUMN_TYPE.store(
            api.duckdb_vector_get_column_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VECTOR_GET_DATA.store(
            api.duckdb_vector_get_data.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VECTOR_GET_VALIDITY.store(
            api.duckdb_vector_get_validity.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VECTOR_ENSURE_VALIDITY_WRITABLE.store(
            api.duckdb_vector_ensure_validity_writable
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VECTOR_ASSIGN_STRING_ELEMENT.store(
            api.duckdb_vector_assign_string_element
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VECTOR_ASSIGN_STRING_ELEMENT_LEN.store(
            api.duckdb_vector_assign_string_element_len
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_LIST_VECTOR_GET_CHILD.store(
            api.duckdb_list_vector_get_child.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_LIST_VECTOR_GET_SIZE.store(
            api.duckdb_list_vector_get_size.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_LIST_VECTOR_SET_SIZE.store(
            api.duckdb_list_vector_set_size.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_LIST_VECTOR_RESERVE.store(
            api.duckdb_list_vector_reserve.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_STRUCT_VECTOR_GET_CHILD.store(
            api.duckdb_struct_vector_get_child.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ARRAY_VECTOR_GET_CHILD.store(
            api.duckdb_array_vector_get_child.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALIDITY_ROW_IS_VALID.store(
            api.duckdb_validity_row_is_valid.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALIDITY_SET_ROW_VALIDITY.store(
            api.duckdb_validity_set_row_validity.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALIDITY_SET_ROW_INVALID.store(
            api.duckdb_validity_set_row_invalid.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALIDITY_SET_ROW_VALID.store(
            api.duckdb_validity_set_row_valid.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_SCALAR_FUNCTION.store(
            api.duckdb_create_scalar_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_SCALAR_FUNCTION.store(
            api.duckdb_destroy_scalar_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_SET_NAME.store(
            api.duckdb_scalar_function_set_name.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_SET_VARARGS.store(
            api.duckdb_scalar_function_set_varargs.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_SET_SPECIAL_HANDLING.store(
            api.duckdb_scalar_function_set_special_handling
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_SET_VOLATILE.store(
            api.duckdb_scalar_function_set_volatile
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_ADD_PARAMETER.store(
            api.duckdb_scalar_function_add_parameter
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_SET_RETURN_TYPE.store(
            api.duckdb_scalar_function_set_return_type
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_SET_EXTRA_INFO.store(
            api.duckdb_scalar_function_set_extra_info
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_SET_FUNCTION.store(
            api.duckdb_scalar_function_set_function
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_REGISTER_SCALAR_FUNCTION.store(
            api.duckdb_register_scalar_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_GET_EXTRA_INFO.store(
            api.duckdb_scalar_function_get_extra_info
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_SET_ERROR.store(
            api.duckdb_scalar_function_set_error.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_SCALAR_FUNCTION_SET.store(
            api.duckdb_create_scalar_function_set.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_SCALAR_FUNCTION_SET.store(
            api.duckdb_destroy_scalar_function_set.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ADD_SCALAR_FUNCTION_TO_SET.store(
            api.duckdb_add_scalar_function_to_set.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_REGISTER_SCALAR_FUNCTION_SET.store(
            api.duckdb_register_scalar_function_set
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_AGGREGATE_FUNCTION.store(
            api.duckdb_create_aggregate_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_AGGREGATE_FUNCTION.store(
            api.duckdb_destroy_aggregate_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_AGGREGATE_FUNCTION_SET_NAME.store(
            api.duckdb_aggregate_function_set_name.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_AGGREGATE_FUNCTION_ADD_PARAMETER.store(
            api.duckdb_aggregate_function_add_parameter
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_AGGREGATE_FUNCTION_SET_RETURN_TYPE.store(
            api.duckdb_aggregate_function_set_return_type
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_AGGREGATE_FUNCTION_SET_FUNCTIONS.store(
            api.duckdb_aggregate_function_set_functions
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_AGGREGATE_FUNCTION_SET_DESTRUCTOR.store(
            api.duckdb_aggregate_function_set_destructor
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_REGISTER_AGGREGATE_FUNCTION.store(
            api.duckdb_register_aggregate_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_AGGREGATE_FUNCTION_SET_SPECIAL_HANDLING.store(
            api.duckdb_aggregate_function_set_special_handling
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_AGGREGATE_FUNCTION_SET_EXTRA_INFO.store(
            api.duckdb_aggregate_function_set_extra_info
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_AGGREGATE_FUNCTION_GET_EXTRA_INFO.store(
            api.duckdb_aggregate_function_get_extra_info
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_AGGREGATE_FUNCTION_SET_ERROR.store(
            api.duckdb_aggregate_function_set_error
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_AGGREGATE_FUNCTION_SET.store(
            api.duckdb_create_aggregate_function_set
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_AGGREGATE_FUNCTION_SET.store(
            api.duckdb_destroy_aggregate_function_set
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ADD_AGGREGATE_FUNCTION_TO_SET.store(
            api.duckdb_add_aggregate_function_to_set
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_REGISTER_AGGREGATE_FUNCTION_SET.store(
            api.duckdb_register_aggregate_function_set
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_TABLE_FUNCTION.store(
            api.duckdb_create_table_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_TABLE_FUNCTION.store(
            api.duckdb_destroy_table_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_FUNCTION_SET_NAME.store(
            api.duckdb_table_function_set_name.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_FUNCTION_ADD_PARAMETER.store(
            api.duckdb_table_function_add_parameter
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_FUNCTION_ADD_NAMED_PARAMETER.store(
            api.duckdb_table_function_add_named_parameter
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_FUNCTION_SET_EXTRA_INFO.store(
            api.duckdb_table_function_set_extra_info
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_FUNCTION_SET_BIND.store(
            api.duckdb_table_function_set_bind.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_FUNCTION_SET_INIT.store(
            api.duckdb_table_function_set_init.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_FUNCTION_SET_LOCAL_INIT.store(
            api.duckdb_table_function_set_local_init
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_FUNCTION_SET_FUNCTION.store(
            api.duckdb_table_function_set_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_FUNCTION_SUPPORTS_PROJECTION_PUSHDOWN.store(
            api.duckdb_table_function_supports_projection_pushdown
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_REGISTER_TABLE_FUNCTION.store(
            api.duckdb_register_table_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_GET_EXTRA_INFO.store(
            api.duckdb_bind_get_extra_info.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_ADD_RESULT_COLUMN.store(
            api.duckdb_bind_add_result_column.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_GET_PARAMETER_COUNT.store(
            api.duckdb_bind_get_parameter_count.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_GET_PARAMETER.store(
            api.duckdb_bind_get_parameter.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_GET_NAMED_PARAMETER.store(
            api.duckdb_bind_get_named_parameter.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_SET_BIND_DATA.store(
            api.duckdb_bind_set_bind_data.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_SET_CARDINALITY.store(
            api.duckdb_bind_set_cardinality.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_BIND_SET_ERROR.store(
            api.duckdb_bind_set_error.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_INIT_GET_EXTRA_INFO.store(
            api.duckdb_init_get_extra_info.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_INIT_GET_BIND_DATA.store(
            api.duckdb_init_get_bind_data.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_INIT_SET_INIT_DATA.store(
            api.duckdb_init_set_init_data.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_INIT_GET_COLUMN_COUNT.store(
            api.duckdb_init_get_column_count.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_INIT_GET_COLUMN_INDEX.store(
            api.duckdb_init_get_column_index.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_INIT_SET_MAX_THREADS.store(
            api.duckdb_init_set_max_threads.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_INIT_SET_ERROR.store(
            api.duckdb_init_set_error.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FUNCTION_GET_EXTRA_INFO.store(
            api.duckdb_function_get_extra_info.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FUNCTION_GET_BIND_DATA.store(
            api.duckdb_function_get_bind_data.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FUNCTION_GET_INIT_DATA.store(
            api.duckdb_function_get_init_data.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FUNCTION_GET_LOCAL_INIT_DATA.store(
            api.duckdb_function_get_local_init_data
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FUNCTION_SET_ERROR.store(
            api.duckdb_function_set_error.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ADD_REPLACEMENT_SCAN.store(
            api.duckdb_add_replacement_scan.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_REPLACEMENT_SCAN_SET_FUNCTION_NAME.store(
            api.duckdb_replacement_scan_set_function_name
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_REPLACEMENT_SCAN_ADD_PARAMETER.store(
            api.duckdb_replacement_scan_add_parameter
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_REPLACEMENT_SCAN_SET_ERROR.store(
            api.duckdb_replacement_scan_set_error.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PROFILING_INFO_GET_METRICS.store(
            api.duckdb_profiling_info_get_metrics.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PROFILING_INFO_GET_CHILD_COUNT.store(
            api.duckdb_profiling_info_get_child_count
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PROFILING_INFO_GET_CHILD.store(
            api.duckdb_profiling_info_get_child.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPENDER_CREATE.store(
            api.duckdb_appender_create.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPENDER_CREATE_EXT.store(
            api.duckdb_appender_create_ext.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPENDER_COLUMN_COUNT.store(
            api.duckdb_appender_column_count.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPENDER_COLUMN_TYPE.store(
            api.duckdb_appender_column_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPENDER_ERROR.store(
            api.duckdb_appender_error.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPENDER_FLUSH.store(
            api.duckdb_appender_flush.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPENDER_CLOSE.store(
            api.duckdb_appender_close.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPENDER_DESTROY.store(
            api.duckdb_appender_destroy.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPENDER_ADD_COLUMN.store(
            api.duckdb_appender_add_column.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPENDER_CLEAR_COLUMNS.store(
            api.duckdb_appender_clear_columns.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_DATA_CHUNK.store(
            api.duckdb_append_data_chunk.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_DESCRIPTION_CREATE.store(
            api.duckdb_table_description_create.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_DESCRIPTION_CREATE_EXT.store(
            api.duckdb_table_description_create_ext
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_DESCRIPTION_DESTROY.store(
            api.duckdb_table_description_destroy.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_DESCRIPTION_ERROR.store(
            api.duckdb_table_description_error.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COLUMN_HAS_DEFAULT.store(
            api.duckdb_column_has_default.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_DESCRIPTION_GET_COLUMN_NAME.store(
            api.duckdb_table_description_get_column_name
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_EXECUTE_TASKS.store(
            api.duckdb_execute_tasks.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_TASK_STATE.store(
            api.duckdb_create_task_state.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_EXECUTE_TASKS_STATE.store(
            api.duckdb_execute_tasks_state.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_EXECUTE_N_TASKS_STATE.store(
            api.duckdb_execute_n_tasks_state.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FINISH_EXECUTION.store(
            api.duckdb_finish_execution.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TASK_STATE_IS_FINISHED.store(
            api.duckdb_task_state_is_finished.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_TASK_STATE.store(
            api.duckdb_destroy_task_state.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_EXECUTION_IS_FINISHED.store(
            api.duckdb_execution_is_finished.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FETCH_CHUNK.store(
            api.duckdb_fetch_chunk.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_CAST_FUNCTION.store(
            api.duckdb_create_cast_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CAST_FUNCTION_SET_SOURCE_TYPE.store(
            api.duckdb_cast_function_set_source_type
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CAST_FUNCTION_SET_TARGET_TYPE.store(
            api.duckdb_cast_function_set_target_type
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CAST_FUNCTION_SET_IMPLICIT_CAST_COST.store(
            api.duckdb_cast_function_set_implicit_cast_cost
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CAST_FUNCTION_SET_FUNCTION.store(
            api.duckdb_cast_function_set_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CAST_FUNCTION_SET_EXTRA_INFO.store(
            api.duckdb_cast_function_set_extra_info
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CAST_FUNCTION_GET_EXTRA_INFO.store(
            api.duckdb_cast_function_get_extra_info
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CAST_FUNCTION_GET_CAST_MODE.store(
            api.duckdb_cast_function_get_cast_mode.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CAST_FUNCTION_SET_ERROR.store(
            api.duckdb_cast_function_set_error.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CAST_FUNCTION_SET_ROW_ERROR.store(
            api.duckdb_cast_function_set_row_error.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_REGISTER_CAST_FUNCTION.store(
            api.duckdb_register_cast_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_CAST_FUNCTION.store(
            api.duckdb_destroy_cast_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_IS_FINITE_TIMESTAMP_S.store(
            api.duckdb_is_finite_timestamp_s.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_IS_FINITE_TIMESTAMP_MS.store(
            api.duckdb_is_finite_timestamp_ms.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_IS_FINITE_TIMESTAMP_NS.store(
            api.duckdb_is_finite_timestamp_ns.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_TIMESTAMP_TZ.store(
            api.duckdb_create_timestamp_tz.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_TIMESTAMP_S.store(
            api.duckdb_create_timestamp_s.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_TIMESTAMP_MS.store(
            api.duckdb_create_timestamp_ms.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_TIMESTAMP_NS.store(
            api.duckdb_create_timestamp_ns.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_TIMESTAMP_TZ.store(
            api.duckdb_get_timestamp_tz.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_TIMESTAMP_S.store(
            api.duckdb_get_timestamp_s.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_TIMESTAMP_MS.store(
            api.duckdb_get_timestamp_ms.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_TIMESTAMP_NS.store(
            api.duckdb_get_timestamp_ns.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_VALUE.store(
            api.duckdb_append_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_PROFILING_INFO.store(
            api.duckdb_get_profiling_info.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PROFILING_INFO_GET_VALUE.store(
            api.duckdb_profiling_info_get_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPENDER_BEGIN_ROW.store(
            api.duckdb_appender_begin_row.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPENDER_END_ROW.store(
            api.duckdb_appender_end_row.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_DEFAULT.store(
            api.duckdb_append_default.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_BOOL.store(
            api.duckdb_append_bool.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_INT8.store(
            api.duckdb_append_int8.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_INT16.store(
            api.duckdb_append_int16.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_INT32.store(
            api.duckdb_append_int32.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_INT64.store(
            api.duckdb_append_int64.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_HUGEINT.store(
            api.duckdb_append_hugeint.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_UINT8.store(
            api.duckdb_append_uint8.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_UINT16.store(
            api.duckdb_append_uint16.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_UINT32.store(
            api.duckdb_append_uint32.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_UINT64.store(
            api.duckdb_append_uint64.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_UHUGEINT.store(
            api.duckdb_append_uhugeint.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_FLOAT.store(
            api.duckdb_append_float.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_DOUBLE.store(
            api.duckdb_append_double.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_DATE.store(
            api.duckdb_append_date.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_TIME.store(
            api.duckdb_append_time.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_TIMESTAMP.store(
            api.duckdb_append_timestamp.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_INTERVAL.store(
            api.duckdb_append_interval.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_VARCHAR.store(
            api.duckdb_append_varchar.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_VARCHAR_LENGTH.store(
            api.duckdb_append_varchar_length.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_BLOB.store(
            api.duckdb_append_blob.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_NULL.store(
            api.duckdb_append_null.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ROW_COUNT.store(
            api.duckdb_row_count.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COLUMN_DATA.store(
            api.duckdb_column_data.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_NULLMASK_DATA.store(
            api.duckdb_nullmask_data.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_RESULT_GET_CHUNK.store(
            api.duckdb_result_get_chunk.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_RESULT_IS_STREAMING.store(
            api.duckdb_result_is_streaming.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_RESULT_CHUNK_COUNT.store(
            api.duckdb_result_chunk_count.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_BOOLEAN.store(
            api.duckdb_value_boolean.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_INT8.store(
            api.duckdb_value_int8.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_INT16.store(
            api.duckdb_value_int16.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_INT32.store(
            api.duckdb_value_int32.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_INT64.store(
            api.duckdb_value_int64.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_HUGEINT.store(
            api.duckdb_value_hugeint.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_UHUGEINT.store(
            api.duckdb_value_uhugeint.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_DECIMAL.store(
            api.duckdb_value_decimal.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_UINT8.store(
            api.duckdb_value_uint8.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_UINT16.store(
            api.duckdb_value_uint16.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_UINT32.store(
            api.duckdb_value_uint32.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_UINT64.store(
            api.duckdb_value_uint64.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_FLOAT.store(
            api.duckdb_value_float.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_DOUBLE.store(
            api.duckdb_value_double.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_DATE.store(
            api.duckdb_value_date.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_TIME.store(
            api.duckdb_value_time.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_TIMESTAMP.store(
            api.duckdb_value_timestamp.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_INTERVAL.store(
            api.duckdb_value_interval.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_VARCHAR.store(
            api.duckdb_value_varchar.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_STRING.store(
            api.duckdb_value_string.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_VARCHAR_INTERNAL.store(
            api.duckdb_value_varchar_internal.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_STRING_INTERNAL.store(
            api.duckdb_value_string_internal.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_BLOB.store(
            api.duckdb_value_blob.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_IS_NULL.store(
            api.duckdb_value_is_null.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_EXECUTE_PREPARED_STREAMING.store(
            api.duckdb_execute_prepared_streaming.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PENDING_PREPARED_STREAMING.store(
            api.duckdb_pending_prepared_streaming.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_QUERY_ARROW.store(
            api.duckdb_query_arrow.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_QUERY_ARROW_SCHEMA.store(
            api.duckdb_query_arrow_schema.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PREPARED_ARROW_SCHEMA.store(
            api.duckdb_prepared_arrow_schema.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_RESULT_ARROW_ARRAY.store(
            api.duckdb_result_arrow_array.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_QUERY_ARROW_ARRAY.store(
            api.duckdb_query_arrow_array.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ARROW_COLUMN_COUNT.store(
            api.duckdb_arrow_column_count.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ARROW_ROW_COUNT.store(
            api.duckdb_arrow_row_count.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ARROW_ROWS_CHANGED.store(
            api.duckdb_arrow_rows_changed.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_QUERY_ARROW_ERROR.store(
            api.duckdb_query_arrow_error.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_ARROW.store(
            api.duckdb_destroy_arrow.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_ARROW_STREAM.store(
            api.duckdb_destroy_arrow_stream.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_EXECUTE_PREPARED_ARROW.store(
            api.duckdb_execute_prepared_arrow.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ARROW_SCAN.store(
            api.duckdb_arrow_scan.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ARROW_ARRAY_SCAN.store(
            api.duckdb_arrow_array_scan.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_STREAM_FETCH_CHUNK.store(
            api.duckdb_stream_fetch_chunk.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_INSTANCE_CACHE.store(
            api.duckdb_create_instance_cache.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_OR_CREATE_FROM_CACHE.store(
            api.duckdb_get_or_create_from_cache.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_INSTANCE_CACHE.store(
            api.duckdb_destroy_instance_cache.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPEND_DEFAULT_TO_CHUNK.store(
            api.duckdb_append_default_to_chunk.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPENDER_ERROR_DATA.store(
            api.duckdb_appender_error_data.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPENDER_CREATE_QUERY.store(
            api.duckdb_appender_create_query.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_APPENDER_CLEAR.store(
            api.duckdb_appender_clear.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TO_ARROW_SCHEMA.store(
            api.duckdb_to_arrow_schema.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DATA_CHUNK_TO_ARROW.store(
            api.duckdb_data_chunk_to_arrow.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCHEMA_FROM_ARROW.store(
            api.duckdb_schema_from_arrow.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DATA_CHUNK_FROM_ARROW.store(
            api.duckdb_data_chunk_from_arrow.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_ARROW_CONVERTED_SCHEMA.store(
            api.duckdb_destroy_arrow_converted_schema
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CLIENT_CONTEXT_GET_CATALOG.store(
            api.duckdb_client_context_get_catalog.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CATALOG_GET_TYPE_NAME.store(
            api.duckdb_catalog_get_type_name.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CATALOG_GET_ENTRY.store(
            api.duckdb_catalog_get_entry.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_CATALOG.store(
            api.duckdb_destroy_catalog.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CATALOG_ENTRY_GET_TYPE.store(
            api.duckdb_catalog_entry_get_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CATALOG_ENTRY_GET_NAME.store(
            api.duckdb_catalog_entry_get_name.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_CATALOG_ENTRY.store(
            api.duckdb_destroy_catalog_entry.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_CONFIG_OPTION.store(
            api.duckdb_create_config_option.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_CONFIG_OPTION.store(
            api.duckdb_destroy_config_option.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CONFIG_OPTION_SET_NAME.store(
            api.duckdb_config_option_set_name.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CONFIG_OPTION_SET_TYPE.store(
            api.duckdb_config_option_set_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CONFIG_OPTION_SET_DEFAULT_VALUE.store(
            api.duckdb_config_option_set_default_value
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CONFIG_OPTION_SET_DEFAULT_SCOPE.store(
            api.duckdb_config_option_set_default_scope
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CONFIG_OPTION_SET_DESCRIPTION.store(
            api.duckdb_config_option_set_description
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_REGISTER_CONFIG_OPTION.store(
            api.duckdb_register_config_option.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CLIENT_CONTEXT_GET_CONFIG_OPTION.store(
            api.duckdb_client_context_get_config_option
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_COPY_FUNCTION.store(
            api.duckdb_create_copy_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_SET_NAME.store(
            api.duckdb_copy_function_set_name.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_SET_EXTRA_INFO.store(
            api.duckdb_copy_function_set_extra_info
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_REGISTER_COPY_FUNCTION.store(
            api.duckdb_register_copy_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_COPY_FUNCTION.store(
            api.duckdb_destroy_copy_function.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_SET_BIND.store(
            api.duckdb_copy_function_set_bind.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_BIND_SET_ERROR.store(
            api.duckdb_copy_function_bind_set_error
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_BIND_GET_EXTRA_INFO.store(
            api.duckdb_copy_function_bind_get_extra_info
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_BIND_GET_CLIENT_CONTEXT.store(
            api.duckdb_copy_function_bind_get_client_context
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_BIND_GET_COLUMN_COUNT.store(
            api.duckdb_copy_function_bind_get_column_count
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_BIND_GET_COLUMN_TYPE.store(
            api.duckdb_copy_function_bind_get_column_type
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_BIND_GET_OPTIONS.store(
            api.duckdb_copy_function_bind_get_options
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_BIND_SET_BIND_DATA.store(
            api.duckdb_copy_function_bind_set_bind_data
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_SET_GLOBAL_INIT.store(
            api.duckdb_copy_function_set_global_init
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_GLOBAL_INIT_SET_ERROR.store(
            api.duckdb_copy_function_global_init_set_error
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_GLOBAL_INIT_GET_EXTRA_INFO.store(
            api.duckdb_copy_function_global_init_get_extra_info
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_GLOBAL_INIT_GET_CLIENT_CONTEXT.store(
            api.duckdb_copy_function_global_init_get_client_context
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_GLOBAL_INIT_GET_BIND_DATA.store(
            api.duckdb_copy_function_global_init_get_bind_data
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_GLOBAL_INIT_SET_GLOBAL_STATE.store(
            api.duckdb_copy_function_global_init_set_global_state
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_GLOBAL_INIT_GET_FILE_PATH.store(
            api.duckdb_copy_function_global_init_get_file_path
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_SET_SINK.store(
            api.duckdb_copy_function_set_sink.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_SINK_SET_ERROR.store(
            api.duckdb_copy_function_sink_set_error
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_SINK_GET_EXTRA_INFO.store(
            api.duckdb_copy_function_sink_get_extra_info
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_SINK_GET_CLIENT_CONTEXT.store(
            api.duckdb_copy_function_sink_get_client_context
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_SINK_GET_BIND_DATA.store(
            api.duckdb_copy_function_sink_get_bind_data
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_SINK_GET_GLOBAL_STATE.store(
            api.duckdb_copy_function_sink_get_global_state
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_SET_FINALIZE.store(
            api.duckdb_copy_function_set_finalize.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_FINALIZE_SET_ERROR.store(
            api.duckdb_copy_function_finalize_set_error
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_FINALIZE_GET_EXTRA_INFO.store(
            api.duckdb_copy_function_finalize_get_extra_info
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_FINALIZE_GET_CLIENT_CONTEXT.store(
            api.duckdb_copy_function_finalize_get_client_context
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_FINALIZE_GET_BIND_DATA.store(
            api.duckdb_copy_function_finalize_get_bind_data
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_FINALIZE_GET_GLOBAL_STATE.store(
            api.duckdb_copy_function_finalize_get_global_state
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_COPY_FUNCTION_SET_COPY_FROM_FUNCTION.store(
            api.duckdb_copy_function_set_copy_from_function
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_FUNCTION_BIND_GET_RESULT_COLUMN_COUNT.store(
            api.duckdb_table_function_bind_get_result_column_count
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_FUNCTION_BIND_GET_RESULT_COLUMN_NAME.store(
            api.duckdb_table_function_bind_get_result_column_name
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_FUNCTION_BIND_GET_RESULT_COLUMN_TYPE.store(
            api.duckdb_table_function_bind_get_result_column_type
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_ERROR_DATA.store(
            api.duckdb_create_error_data.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_ERROR_DATA.store(
            api.duckdb_destroy_error_data.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ERROR_DATA_ERROR_TYPE.store(
            api.duckdb_error_data_error_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ERROR_DATA_MESSAGE.store(
            api.duckdb_error_data_message.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_ERROR_DATA_HAS_ERROR.store(
            api.duckdb_error_data_has_error.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_EXPRESSION.store(
            api.duckdb_destroy_expression.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_EXPRESSION_RETURN_TYPE.store(
            api.duckdb_expression_return_type.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_EXPRESSION_IS_FOLDABLE.store(
            api.duckdb_expression_is_foldable.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_EXPRESSION_FOLD.store(
            api.duckdb_expression_fold.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CLIENT_CONTEXT_GET_FILE_SYSTEM.store(
            api.duckdb_client_context_get_file_system
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_FILE_SYSTEM.store(
            api.duckdb_destroy_file_system.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FILE_SYSTEM_OPEN.store(
            api.duckdb_file_system_open.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FILE_SYSTEM_ERROR_DATA.store(
            api.duckdb_file_system_error_data.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_FILE_OPEN_OPTIONS.store(
            api.duckdb_create_file_open_options.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FILE_OPEN_OPTIONS_SET_FLAG.store(
            api.duckdb_file_open_options_set_flag.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_FILE_OPEN_OPTIONS.store(
            api.duckdb_destroy_file_open_options.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_FILE_HANDLE.store(
            api.duckdb_destroy_file_handle.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FILE_HANDLE_ERROR_DATA.store(
            api.duckdb_file_handle_error_data.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FILE_HANDLE_CLOSE.store(
            api.duckdb_file_handle_close.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FILE_HANDLE_READ.store(
            api.duckdb_file_handle_read.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FILE_HANDLE_WRITE.store(
            api.duckdb_file_handle_write.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FILE_HANDLE_SEEK.store(
            api.duckdb_file_handle_seek.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FILE_HANDLE_TELL.store(
            api.duckdb_file_handle_tell.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FILE_HANDLE_SYNC.store(
            api.duckdb_file_handle_sync.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_FILE_HANDLE_SIZE.store(
            api.duckdb_file_handle_size.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GEOMETRY_TYPE_GET_CRS.store(
            api.duckdb_geometry_type_get_crs.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_LOG_STORAGE.store(
            api.duckdb_create_log_storage.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_LOG_STORAGE.store(
            api.duckdb_destroy_log_storage.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_LOG_STORAGE_SET_WRITE_LOG_ENTRY.store(
            api.duckdb_log_storage_set_write_log_entry
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_LOG_STORAGE_SET_EXTRA_DATA.store(
            api.duckdb_log_storage_set_extra_data.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_LOG_STORAGE_SET_NAME.store(
            api.duckdb_log_storage_set_name.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_REGISTER_LOG_STORAGE.store(
            api.duckdb_register_log_storage.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CLIENT_CONTEXT_GET_CONNECTION_ID.store(
            api.duckdb_client_context_get_connection_id
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_CLIENT_CONTEXT.store(
            api.duckdb_destroy_client_context.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CONNECTION_GET_CLIENT_CONTEXT.store(
            api.duckdb_connection_get_client_context
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_TABLE_NAMES.store(
            api.duckdb_get_table_names.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CONNECTION_GET_ARROW_OPTIONS.store(
            api.duckdb_connection_get_arrow_options
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_ARROW_OPTIONS.store(
            api.duckdb_destroy_arrow_options.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PREPARED_STATEMENT_COLUMN_COUNT.store(
            api.duckdb_prepared_statement_column_count
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PREPARED_STATEMENT_COLUMN_NAME.store(
            api.duckdb_prepared_statement_column_name
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PREPARED_STATEMENT_COLUMN_LOGICAL_TYPE.store(
            api.duckdb_prepared_statement_column_logical_type
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_PREPARED_STATEMENT_COLUMN_TYPE.store(
            api.duckdb_prepared_statement_column_type
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_RESULT_GET_ARROW_OPTIONS.store(
            api.duckdb_result_get_arrow_options.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_SET_BIND.store(
            api.duckdb_scalar_function_set_bind.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_BIND_SET_ERROR.store(
            api.duckdb_scalar_function_bind_set_error
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_GET_CLIENT_CONTEXT.store(
            api.duckdb_scalar_function_get_client_context
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_SET_BIND_DATA.store(
            api.duckdb_scalar_function_set_bind_data
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_GET_BIND_DATA.store(
            api.duckdb_scalar_function_get_bind_data
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_BIND_GET_EXTRA_INFO.store(
            api.duckdb_scalar_function_bind_get_extra_info
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_BIND_GET_ARGUMENT_COUNT.store(
            api.duckdb_scalar_function_bind_get_argument_count
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_BIND_GET_ARGUMENT.store(
            api.duckdb_scalar_function_bind_get_argument
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_SET_BIND_DATA_COPY.store(
            api.duckdb_scalar_function_set_bind_data_copy
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_GET_STATE.store(
            api.duckdb_scalar_function_get_state.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_SET_INIT.store(
            api.duckdb_scalar_function_set_init.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_INIT_SET_ERROR.store(
            api.duckdb_scalar_function_init_set_error
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_INIT_SET_STATE.store(
            api.duckdb_scalar_function_init_set_state
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_INIT_GET_CLIENT_CONTEXT.store(
            api.duckdb_scalar_function_init_get_client_context
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_INIT_GET_BIND_DATA.store(
            api.duckdb_scalar_function_init_get_bind_data
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SCALAR_FUNCTION_INIT_GET_EXTRA_INFO.store(
            api.duckdb_scalar_function_init_get_extra_info
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALUE_TO_STRING.store(
            api.duckdb_value_to_string.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VALID_UTF8_CHECK.store(
            api.duckdb_valid_utf8_check.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_DESCRIPTION_GET_COLUMN_COUNT.store(
            api.duckdb_table_description_get_column_count
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_DESCRIPTION_GET_COLUMN_TYPE.store(
            api.duckdb_table_description_get_column_type
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_TABLE_FUNCTION_GET_CLIENT_CONTEXT.store(
            api.duckdb_table_function_get_client_context
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_MAP_VALUE.store(
            api.duckdb_create_map_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_UNION_VALUE.store(
            api.duckdb_create_union_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_TIME_NS.store(
            api.duckdb_create_time_ns.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_GET_TIME_NS.store(
            api.duckdb_get_time_ns.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_VECTOR.store(
            api.duckdb_create_vector.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_VECTOR.store(
            api.duckdb_destroy_vector.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SLICE_VECTOR.store(
            api.duckdb_slice_vector.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VECTOR_REFERENCE_VALUE.store(
            api.duckdb_vector_reference_value.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VECTOR_REFERENCE_VECTOR.store(
            api.duckdb_vector_reference_vector.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_CREATE_SELECTION_VECTOR.store(
            api.duckdb_create_selection_vector.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_DESTROY_SELECTION_VECTOR.store(
            api.duckdb_destroy_selection_vector.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_SELECTION_VECTOR_GET_DATA_PTR.store(
            api.duckdb_selection_vector_get_data_ptr
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_VECTOR_COPY_SEL.store(
            api.duckdb_vector_copy_sel.map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
        DUCKDB_UNSAFE_VECTOR_ASSIGN_STRING_ELEMENT_LEN.store(
            api.duckdb_unsafe_vector_assign_string_element_len
                .map_or(::std::ptr::null_mut(), |f| f as *mut ()),
            ::std::sync::atomic::Ordering::Release,
        );
    }
    ::std::result::Result::Ok(true)
}
