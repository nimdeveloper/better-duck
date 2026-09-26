//! `better-duck-tauri` — a Tauri v2 plugin exposing [better-duck](https://crates.io/crates/better-duck-core)
//! (embedded DuckDB) to the webview.
//!
//! Pick a backend feature and initialize it in your Tauri app:
//!
//! ```ignore
//! tauri::Builder::default()
//!     .plugin(better_duck_tauri::core::init())   // or ::diesel::init()
//!     .run(tauri::generate_context!())?;
//! ```
//!
//! The webview-facing command API (`load`/`close`/`select`/`execute`) is identical
//! regardless of backend. Results are row-oriented JSON by default.

#[cfg(not(any(feature = "backend-core", feature = "backend-diesel")))]
compile_error!(
    "better-duck-tauri needs a backend feature: `backend-core` (default) or `backend-diesel`"
);

mod commands;
mod error;
mod state;

pub mod backend;

#[cfg(any(feature = "backend-core", feature = "backend-diesel"))]
mod json;
#[cfg(any(feature = "backend-core", feature = "backend-diesel"))]
mod migration;
#[cfg(any(feature = "backend-core", feature = "backend-diesel"))]
mod policy;

use std::sync::Arc;

use tauri::plugin::{Builder, TauriPlugin};
use tauri::{Manager, Runtime};

pub use backend::{DataFormat, DuckBackend, ExecuteResult, Row};
pub use error::{Error, Result};
#[cfg(any(feature = "backend-core", feature = "backend-diesel"))]
pub use backend::ConnectHook;
/// The core DuckDB connection type, re-exported for writing `on_connect` hooks.
#[cfg(any(feature = "backend-core", feature = "backend-diesel"))]
pub use better_duck_core::connection::Connection;
#[cfg(any(feature = "backend-core", feature = "backend-diesel"))]
pub use migration::{Migration, MigrationKind};
#[cfg(any(feature = "backend-core", feature = "backend-diesel"))]
pub use policy::Policy;

use state::DuckState;

/// Builds the plugin around a chosen backend instance.
fn build_plugin<R: Runtime>(backend: Arc<dyn DuckBackend>) -> TauriPlugin<R> {
    Builder::new("duck")
        .invoke_handler(tauri::generate_handler![
            commands::load,
            commands::close,
            commands::select,
            commands::execute,
            commands::load_extension,
            commands::import,
            commands::export,
            commands::append_rows,
            commands::tables,
            commands::columns,
            commands::explain,
            commands::stream
        ])
        .setup(move |app, _api| {
            app.manage(DuckState::new(backend));
            Ok(())
        })
        .build()
}

#[cfg(feature = "backend-core")]
pub mod core {
    //! Entry point for the `better-duck-core` backend.
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use better_duck_core::connection::Connection;
    use tauri::plugin::TauriPlugin;
    use tauri::Runtime;

    use crate::backend::ConnectHook;
    use crate::error::Result;
    use crate::migration::Migration;
    use crate::policy::Policy;

    /// Builder for the core-backed plugin: register per-connection migrations, the
    /// security policy, and connection setup hooks (e.g. UDF registration) before init.
    #[derive(Default)]
    pub struct Builder {
        migrations: HashMap<String, Vec<Migration>>,
        policy: Policy,
        on_connect: Vec<ConnectHook>,
    }

    impl Builder {
        /// Creates an empty builder.
        pub fn new() -> Builder {
            Builder::default()
        }

        /// Registers migrations to run on `load` for the given connection string.
        #[must_use]
        pub fn add_migrations(
            mut self,
            conn_str: impl Into<String>,
            migrations: Vec<Migration>,
        ) -> Builder {
            self.migrations.entry(conn_str.into()).or_default().extend(migrations);
            self
        }

        /// Opens databases read-only and rejects write operations.
        #[must_use]
        pub fn read_only(
            mut self,
            enabled: bool,
        ) -> Builder {
            self.policy.read_only = enabled;
            self
        }

        /// Restricts `load` to this connection string (repeatable).
        #[must_use]
        pub fn allow_connection(
            mut self,
            conn_str: impl Into<String>,
        ) -> Builder {
            self.policy.allowed_connections.get_or_insert_with(Vec::new).push(conn_str.into());
            self
        }

        /// Restricts import/export to files within this directory (repeatable).
        #[must_use]
        pub fn allow_path(
            mut self,
            dir: impl Into<PathBuf>,
        ) -> Builder {
            self.policy.allowed_paths.get_or_insert_with(Vec::new).push(dir.into());
            self
        }

        /// Rejects risky statements (ATTACH/INSTALL/LOAD/PRAGMA/COPY/…) in `select`/`execute`.
        #[must_use]
        pub fn deny_risky_statements(
            mut self,
            enabled: bool,
        ) -> Builder {
            self.policy.deny_risky_statements = enabled;
            self
        }

        /// Registers a hook run on every opened connection — the place to register UDFs
        /// or `LOAD` extensions so each connection carries them.
        #[must_use]
        pub fn on_connect(
            mut self,
            hook: impl Fn(&mut Connection) -> Result<()> + Send + Sync + 'static,
        ) -> Builder {
            self.on_connect.push(Arc::new(hook));
            self
        }

        /// Builds the plugin.
        pub fn build<R: Runtime>(self) -> TauriPlugin<R> {
            super::build_plugin(Arc::new(crate::backend::DuckEngine::with_config(
                self.migrations,
                self.policy,
                self.on_connect,
            )))
        }
    }

    /// Initialize the plugin on the `better-duck-core` backend with defaults.
    pub fn init<R: Runtime>() -> TauriPlugin<R> {
        Builder::new().build()
    }
}

#[cfg(feature = "backend-diesel")]
pub mod diesel {
    //! Entry point for the `better-duck-diesel` backend.
    //!
    //! Currently shares the core execution path; the Diesel-native r2d2 pool and Diesel
    //! migrations are layered on in a later phase (TASKS T4.5). The webview API is identical.
    use std::sync::Arc;

    use tauri::plugin::TauriPlugin;
    use tauri::Runtime;

    /// Initialize the plugin on the `better-duck-diesel` backend.
    pub fn init<R: Runtime>() -> TauriPlugin<R> {
        super::build_plugin(Arc::new(crate::backend::DuckEngine::new()))
    }
}
