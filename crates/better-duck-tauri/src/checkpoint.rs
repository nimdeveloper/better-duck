//! Composable checkpoint (WAL→file) strategies, configured at plugin init.
//!
//! `CHECKPOINT` fails while a transaction is active, so background/after-write sweeps use
//! `FORCE CHECKPOINT` (v1.4+ waits for the lock). Strategies are additive — enable any mix.

use std::time::Duration;

/// Checkpoint strategy configuration (all optional; default is fully passive — rely on
/// DuckDB's own WAL-size auto-checkpoint).
#[derive(Debug, Clone, Default)]
pub struct CheckpointConfig {
    /// `Automatic`: set DuckDB's `checkpoint_threshold` (e.g. `"64MB"`) at load.
    pub threshold: Option<String>,
    /// `AfterWrites`: checkpoint the connection after every N write operations.
    pub after_writes: Option<u64>,
    /// `Interval`: a background thread checkpoints all loaded connections this often.
    pub interval: Option<Duration>,
    /// `OnAppLifecycle`: checkpoint all connections when the app exits.
    pub on_exit: bool,
    /// Use `FORCE CHECKPOINT` (waits for the lock) for background / after-write sweeps.
    pub force: bool,
}
