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

impl CheckpointConfig {
    /// A sensible per-platform default strategy set (TX.2 / design §13).
    ///
    /// Mobile targets (Android/iOS) enable `OnAppLifecycle` checkpointing with
    /// `FORCE CHECKPOINT`, because the OS can suspend or kill the app without a clean
    /// process exit, so pending WAL data must be flushed on the lifecycle event.
    /// Desktop platforms return the passive default (rely on DuckDB's own WAL-size
    /// auto-checkpoint). This is opt-in — no default is imposed on the plugin; apps
    /// apply it via [`Builder::checkpoint_platform_defaults`](crate::core::Builder::checkpoint_platform_defaults).
    pub fn platform_default() -> CheckpointConfig {
        let mobile = cfg!(any(target_os = "android", target_os = "ios"));
        CheckpointConfig { on_exit: mobile, force: mobile, ..CheckpointConfig::default() }
    }

    /// Fills any strategy this config hasn't set from `other`, preserving values already
    /// configured here. Option fields fall back to `other`'s; the boolean toggles are OR-ed
    /// (a platform default can enable a strategy, but never disables one you turned on).
    pub(crate) fn merge_defaults(
        &mut self,
        other: CheckpointConfig,
    ) {
        self.threshold = self.threshold.take().or(other.threshold);
        self.after_writes = self.after_writes.or(other.after_writes);
        self.interval = self.interval.or(other.interval);
        self.on_exit |= other.on_exit;
        self.force |= other.force;
    }
}

#[cfg(test)]
mod tests {
    use super::CheckpointConfig;

    #[test]
    fn platform_default_matches_target() {
        let d = CheckpointConfig::platform_default();
        let mobile = cfg!(any(target_os = "android", target_os = "ios"));
        assert_eq!(d.on_exit, mobile, "OnAppLifecycle is on for mobile only");
        assert_eq!(d.force, mobile);
        assert!(d.threshold.is_none() && d.after_writes.is_none() && d.interval.is_none());
    }

    #[test]
    fn merge_defaults_preserves_explicit_values() {
        let mut cfg = CheckpointConfig { threshold: Some("64MB".to_owned()), ..Default::default() };
        cfg.merge_defaults(CheckpointConfig {
            threshold: Some("16MB".to_owned()),
            on_exit: true,
            force: true,
            ..Default::default()
        });
        // Explicit threshold kept; on_exit/force OR-ed in from the defaults.
        assert_eq!(cfg.threshold.as_deref(), Some("64MB"));
        assert!(cfg.on_exit && cfg.force);
    }
}
