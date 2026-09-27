#![allow(missing_docs)]

//! Reading a DuckDB `GEOMETRY` column (type-id 40, from the `spatial` extension).
//!
//! A GEOMETRY value physically stores its WKB bytes in the `string_t` layout, so
//! it surfaces as a `DuckValue::Blob` of those WKB bytes — previously this errored
//! with "reading DuckDB column type 40 is not yet supported".
//!
//! The test needs the `spatial` extension; it skips cleanly when it can't load
//! (e.g. offline). `INSTALL spatial` downloads from the DuckDB extension repo and
//! has no core-side network timeout (`http_timeout` lives in the httpfs extension,
//! not core — duckdb#21452), so a stalled download would otherwise hang the whole
//! test binary (observed intermittently on Windows CI). We run the
//! extension-dependent work on a worker thread and treat "didn't finish in time"
//! as a skip rather than blocking forever.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;

/// How long to wait for `INSTALL spatial` (a network download) before skipping.
const SPATIAL_TIMEOUT: Duration = Duration::from_secs(60);

enum Outcome {
    /// The extension loaded and the assertions ran (and passed).
    Ran,
    /// The `spatial` extension could not be loaded (e.g. offline) — skip.
    Unavailable,
}

#[test]
fn geometry_column_reads_as_wkb_blob() {
    let (tx, rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        // A panic here (e.g. a failed assertion) drops `tx` without sending,
        // which the main thread observes as `Disconnected` and re-raises.
        let outcome = run_geometry_checks();
        let _ = tx.send(outcome);
    });

    match rx.recv_timeout(SPATIAL_TIMEOUT) {
        Ok(Outcome::Ran) => worker.join().expect("geometry worker panicked"),
        Ok(Outcome::Unavailable) => {
            eprintln!("skipping geometry_column_reads_as_wkb_blob: `spatial` extension unavailable");
        },
        Err(mpsc::RecvTimeoutError::Timeout) => {
            // The worker is still blocked in the extension download; leave it
            // detached (the test binary reaps it on exit) and skip.
            eprintln!(
                "skipping geometry_column_reads_as_wkb_blob: `spatial` install did not finish within {}s",
                SPATIAL_TIMEOUT.as_secs()
            );
        },
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            // Worker returned without sending → it panicked; surface that panic.
            worker.join().expect("geometry worker panicked");
            unreachable!("worker disconnected without sending a result and without panicking");
        },
    }
}

/// Loads the `spatial` extension and exercises GEOMETRY reads. Returns
/// [`Outcome::Unavailable`] if the extension can't be loaded; panics on an
/// assertion failure (propagated to the test thread via the channel closing).
fn run_geometry_checks() -> Outcome {
    let mut conn = Connection::open_in_memory().unwrap();
    if conn.ensure_extension("spatial").is_err() {
        return Outcome::Unavailable;
    }

    // A raw GEOMETRY value (no ST_AsText/ST_AsWKB wrapper) used to error on read.
    let mut result = conn.execute("SELECT ST_Point(1.0, 2.0) AS g").unwrap();
    let row = result.next().unwrap().unwrap();
    match row.get("g") {
        Some(DuckValue::Blob(wkb)) => {
            // WKB for POINT(1 2): a non-empty byte string (endianness flag + type + coords).
            assert!(!wkb.0.is_empty(), "expected non-empty WKB bytes");
        },
        other => panic!("expected GEOMETRY to read as Blob(WKB), got {other:?}"),
    }

    // And the spatial functions themselves work through the normal query path.
    let mut txt = conn.execute("SELECT ST_AsText(ST_Point(1.0, 2.0)) AS t").unwrap();
    assert_eq!(
        txt.next().unwrap().unwrap().get("t"),
        Some(&DuckValue::Text("POINT (1 2)".to_owned()))
    );

    Outcome::Ran
}
