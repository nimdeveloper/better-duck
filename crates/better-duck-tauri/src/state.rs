//! Plugin-managed state: the selected backend, shared across commands.

use std::sync::Arc;

use crate::backend::DuckBackend;

/// Managed state holding the active [`DuckBackend`].
pub(crate) struct DuckState {
    backend: Arc<dyn DuckBackend>,
}

impl DuckState {
    /// Wraps a backend for management via `app.manage`.
    pub(crate) fn new(backend: Arc<dyn DuckBackend>) -> DuckState {
        DuckState { backend }
    }

    /// Returns a cloned handle to the backend (cheap `Arc` bump) for moving into a
    /// blocking task.
    pub(crate) fn backend(&self) -> Arc<dyn DuckBackend> {
        Arc::clone(&self.backend)
    }
}
