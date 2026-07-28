//! Async facade over [`Connection`](crate::connection::Connection), gated by the
//! `async` feature.
//!
//! This module is tokio-only: every method dispatches to
//! `tokio::task::spawn_blocking`. There is no runtime-agnostic abstraction here —
//! an async-std or smol user can reimplement [`AsyncConnection`] over their own
//! blocking-task primitive in place of `spawn_blocking`; see its source for the
//! pattern, which is small.

mod connection;
mod database;
#[cfg(feature = "pool")]
mod pool;

pub use connection::AsyncConnection;
pub use database::AsyncDatabase;
#[cfg(feature = "pool")]
pub use pool::AsyncPool;
