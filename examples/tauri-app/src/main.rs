//! Minimal Tauri v2 desktop app demonstrating the `better-duck-tauri` plugin.
//!
//! Registers the plugin on the core backend; the webview (see `dist/index.html`)
//! drives a load → create → insert → select round-trip over the plugin commands.
//! The `capabilities/default.json` capability grants the command permissions and a
//! connection/path scope, so this also exercises the runtime scope enforcement (T2.5).
//!
//! Run with a Tauri-capable toolchain (WebView2 on Windows): `cargo run` from this
//! directory. `cargo build` alone validates plugin + capability + scope wiring.

fn main() {
    tauri::Builder::default()
        .plugin(better_duck_tauri::core::init())
        .run(tauri::generate_context!())
        .expect("error while running the better-duck example app");
}
