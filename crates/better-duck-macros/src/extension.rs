//! Expansion for the `#[duckdb_entrypoint]` attribute macro.
//!
//! Emits the C-ABI entrypoint DuckDB's loader calls for a Rust-authored **loadable
//! extension** (`.duckdb_extension`). Mirrors the proven shape of DuckDB's own
//! `duckdb-loadable-macros`:
//!
//! - `<name>_init_c_api(info, access) -> bool` — the exported entrypoint. It initializes
//!   the dynamically-loaded API table from `access`, obtains the database, wraps it in a
//!   [`Connection`], and invokes the user's `fn(&Connection) -> Result<...>`. Errors are
//!   reported back through `access.set_error`.
//!
//! The emitted code depends on the `better-duck-sys` **loadable-extension** FFI mode
//! (the `duckdb_ext_api_v1` pointer table + `better_duck_core::extension::c_api_init`)
//! and `Connection::open_from_raw`, which are provided by that feature. Until it is
//! built, code produced by this macro will not compile — the macro itself is stable
//! groundwork.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse::{Parse, ParseStream},
    ItemFn, LitStr, Token,
};

/// Parsed `#[duckdb_entrypoint(name = "...", min_duckdb_version = "...")]` arguments.
pub struct EntrypointArgs {
    name: String,
    min_duckdb_version: String,
}

impl Parse for EntrypointArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut name: Option<String> = None;
        let mut min_duckdb_version: Option<String> = None;
        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            let value: LitStr = input.parse()?;
            match key.to_string().as_str() {
                "name" => name = Some(value.value()),
                "min_duckdb_version" => min_duckdb_version = Some(value.value()),
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown `duckdb_entrypoint` argument `{other}` (expected `name` or `min_duckdb_version`)"),
                    ))
                },
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }
        let name = name.ok_or_else(|| {
            syn::Error::new(input.span(), "`duckdb_entrypoint` requires `name = \"<extension>\"`")
        })?;
        // DuckDB negotiates the API version at load time; default to the widest support.
        let min_duckdb_version = min_duckdb_version.unwrap_or_else(|| "v0.0.1".to_owned());
        Ok(EntrypointArgs { name, min_duckdb_version })
    }
}

pub fn expand(
    args: EntrypointArgs,
    user_fn: ItemFn,
) -> syn::Result<TokenStream> {
    let user_ident = user_fn.sig.ident.clone();
    let entry = format_ident!("{}_init_c_api", args.name);
    let internal = format_ident!("{}_init_c_api_internal", args.name);
    let min_version = &args.min_duckdb_version;

    Ok(quote! {
        #[allow(clippy::missing_safety_doc)]
        #user_fn

        /// Internal entrypoint: returns a `Result` so the exported `extern "C"` shim can
        /// convert failures into `access.set_error` calls.
        ///
        /// # Safety
        /// `info`/`access` must be the valid handles DuckDB's loader passes.
        unsafe fn #internal(
            info: ::better_duck_core::ffi::duckdb_extension_info,
            access: *const ::better_duck_core::ffi::duckdb_extension_access,
        ) -> ::std::result::Result<bool, ::std::boxed::Box<dyn ::std::error::Error>> {
            // Populate the dynamically-loaded DuckDB API table; a version mismatch
            // returns `false` and DuckDB surfaces the real reason.
            if !::better_duck_core::extension_api::c_api_init(info, access, #min_version) {
                return ::std::result::Result::Ok(false);
            }
            // SAFETY: `access` is the loader-provided pointer, valid for this call.
            let access_ref = unsafe { &*access };
            let get_database = access_ref
                .get_database
                .ok_or("get_database function pointer is null in duckdb_extension_access")?;
            // SAFETY: `get_database` is a valid pointer obtained above.
            let db = unsafe { get_database(info) };
            if db.is_null() {
                return ::std::result::Result::Ok(false);
            }
            // SAFETY: `db` is a valid `duckdb_database` handle borrowed from the host; the
            // resulting `Connection` must not close it (host-owned) — `open_from_raw`
            // documents that contract.
            let mut conn = unsafe { ::better_duck_core::connection::Connection::open_from_raw(db.cast())? };
            #user_ident(&mut conn)?;
            ::std::result::Result::Ok(true)
        }

        /// Extension entrypoint called by DuckDB's loader.
        ///
        /// # Safety
        /// Invoked by DuckDB with valid `info`/`access` handles.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn #entry(
            info: ::better_duck_core::ffi::duckdb_extension_info,
            access: *const ::better_duck_core::ffi::duckdb_extension_access,
        ) -> bool {
            // SAFETY: forwarding the loader-provided handles.
            match unsafe { #internal(info, access) } {
                ::std::result::Result::Ok(v) => v,
                ::std::result::Result::Err(e) => {
                    // SAFETY: `access` is valid for this call.
                    let access_ref = unsafe { &*access };
                    if let ::std::option::Option::Some(set_error) = access_ref.set_error {
                        if let ::std::result::Result::Ok(msg) =
                            ::std::ffi::CString::new(e.to_string())
                        {
                            // SAFETY: `set_error` is a valid pointer; `msg` is a live C string.
                            unsafe { set_error(info, msg.as_ptr()) };
                        }
                    }
                    false
                },
            }
        }
    })
}
