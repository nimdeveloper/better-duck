//! Error type surfaced to the webview (serialized as a string).

use serde::{Serialize, Serializer};

/// Convenience result alias for plugin operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by the plugin's commands.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// No connection was loaded for the given connection string.
    #[error("unknown connection: {0}")]
    UnknownConnection(String),
    /// The underlying DuckDB backend failed.
    #[error("backend error: {0}")]
    Backend(String),
    /// A Tauri runtime error.
    #[error(transparent)]
    Tauri(#[from] tauri::Error),
}

impl Serialize for Error {
    fn serialize<S: Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}
