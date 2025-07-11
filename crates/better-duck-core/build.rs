// build.rs
fn main() {
    if cfg!(target_os = "windows") {
        println!("cargo:rustc-link-lib=Rstrtmgr"); // Links against Rstrtmgr.lib
    }
}
