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
mod backup;
#[cfg(any(feature = "backend-core", feature = "backend-diesel"))]
mod checkpoint;
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
pub use backend::{ConnectHook, EngineConfig};
#[cfg(any(feature = "backend-core", feature = "backend-diesel"))]
pub use checkpoint::CheckpointConfig;
/// The core DuckDB connection type, re-exported for writing `on_connect` hooks.
#[cfg(any(feature = "backend-core", feature = "backend-diesel"))]
pub use better_duck_core::connection::Connection;
#[cfg(any(feature = "backend-core", feature = "backend-diesel"))]
pub use migration::{Migration, MigrationKind};
#[cfg(any(feature = "backend-core", feature = "backend-diesel"))]
pub use policy::Policy;

use state::DuckState;

/// Builds the plugin around a chosen backend instance and checkpoint strategy.
fn build_plugin<R: Runtime>(
    backend: Arc<dyn DuckBackend>,
    checkpoint: CheckpointConfig,
) -> TauriPlugin<R> {
    let interval = checkpoint.interval;
    let on_exit = checkpoint.on_exit;
    let force = checkpoint.force;
    let exit_backend = backend.clone();
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
            commands::stream,
            commands::checkpoint,
            commands::interrupt,
            commands::revert
        ])
        .setup(move |app, _api| {
            app.manage(DuckState::new(backend.clone()));
            // `Interval` strategy: a background thread sweeps all loaded connections.
            if let Some(period) = interval {
                let bg = backend.clone();
                std::thread::spawn(move || loop {
                    std::thread::sleep(period);
                    bg.checkpoint_all(force);
                });
            }
            Ok(())
        })
        .on_event(move |_app, event| {
            // `OnAppLifecycle` strategy: checkpoint everything as the app exits.
            if on_exit && matches!(event, tauri::RunEvent::Exit) {
                exit_backend.checkpoint_all(force);
            }
        })
        .build()
}

#[cfg(feature = "backend-core")]
pub mod core {
    //! Entry point for the `better-duck-core` backend.
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use better_duck_core::connection::Connection;
    use tauri::plugin::TauriPlugin;
    use tauri::Runtime;

    use crate::backend::{ConnectHook, EngineConfig};
    use crate::checkpoint::CheckpointConfig;
    use crate::error::Result;
    use crate::migration::Migration;
    use crate::policy::Policy;

    /// Builder for the core-backed plugin: register per-connection migrations, the
    /// security policy, connection setup hooks, and checkpoint strategies before init.
    #[derive(Default)]
    pub struct Builder {
        migrations: HashMap<String, Vec<Migration>>,
        policy: Policy,
        on_connect: Vec<ConnectHook>,
        checkpoint: CheckpointConfig,
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

        /// Allows loading network-backed extensions (`httpfs`/`aws`/`azure`). Off by default.
        #[must_use]
        pub fn allow_network(
            mut self,
            enabled: bool,
        ) -> Builder {
            self.policy.allow_network = enabled;
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

        /// `Automatic` checkpoint strategy: sets DuckDB's `checkpoint_threshold` (e.g. "64MB").
        #[must_use]
        pub fn checkpoint_threshold(
            mut self,
            threshold: impl Into<String>,
        ) -> Builder {
            self.checkpoint.threshold = Some(threshold.into());
            self
        }

        /// `AfterWrites` checkpoint strategy: checkpoint after every `n` write operations.
        #[must_use]
        pub fn checkpoint_after_writes(
            mut self,
            n: u64,
        ) -> Builder {
            self.checkpoint.after_writes = Some(n);
            self
        }

        /// `Interval` checkpoint strategy: a background thread sweeps all connections.
        #[must_use]
        pub fn checkpoint_interval(
            mut self,
            period: Duration,
        ) -> Builder {
            self.checkpoint.interval = Some(period);
            self
        }

        /// `OnAppLifecycle` checkpoint strategy: checkpoint all connections on app exit.
        #[must_use]
        pub fn checkpoint_on_exit(
            mut self,
            enabled: bool,
        ) -> Builder {
            self.checkpoint.on_exit = enabled;
            self
        }

        /// Use `FORCE CHECKPOINT` (waits for the lock) for background / after-write sweeps.
        #[must_use]
        pub fn checkpoint_force(
            mut self,
            enabled: bool,
        ) -> Builder {
            self.checkpoint.force = enabled;
            self
        }

        /// Builds the plugin.
        pub fn build<R: Runtime>(self) -> TauriPlugin<R> {
            let checkpoint = self.checkpoint.clone();
            let engine = crate::backend::DuckEngine::with_config(EngineConfig {
                migrations: self.migrations,
                policy: self.policy,
                on_connect: self.on_connect,
                checkpoint: self.checkpoint,
            });
            super::build_plugin(Arc::new(engine), checkpoint)
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

    use crate::checkpoint::CheckpointConfig;

    /// Initialize the plugin on the `better-duck-diesel` backend.
    pub fn init<R: Runtime>() -> TauriPlugin<R> {
        super::build_plugin(Arc::new(crate::backend::DuckEngine::new()), CheckpointConfig::default())
    }
}
