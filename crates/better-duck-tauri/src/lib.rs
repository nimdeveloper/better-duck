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

use std::sync::Arc;

use tauri::plugin::{Builder, TauriPlugin};
use tauri::{Manager, Runtime};

pub use backend::{DuckBackend, ExecuteResult, Row};
pub use error::{Error, Result};
#[cfg(any(feature = "backend-core", feature = "backend-diesel"))]
pub use migration::{Migration, MigrationKind};

use state::DuckState;

/// Builds the plugin around a chosen backend instance.
fn build_plugin<R: Runtime>(backend: Arc<dyn DuckBackend>) -> TauriPlugin<R> {
    Builder::new("duck")
        .invoke_handler(tauri::generate_handler![
            commands::load,
            commands::close,
            commands::select,
            commands::execute
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
    use std::sync::Arc;

    use tauri::plugin::TauriPlugin;
    use tauri::Runtime;

    use crate::migration::Migration;

    /// Builder for the core-backed plugin, allowing per-connection migrations to be
    /// registered before the plugin is initialized (run on `Database.load`).
    #[derive(Default)]
    pub struct Builder {
        migrations: HashMap<String, Vec<Migration>>,
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

        /// Builds the plugin.
        pub fn build<R: Runtime>(self) -> TauriPlugin<R> {
            super::build_plugin(Arc::new(crate::backend::DuckEngine::with_migrations(
                self.migrations,
            )))
        }
    }

    /// Initialize the plugin on the `better-duck-core` backend with no migrations.
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
