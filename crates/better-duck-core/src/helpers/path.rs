#![allow(dead_code)]

#[cfg(not(unix))]
use std::{ffi::CString, path::Path};

#[cfg(not(unix))]
use crate::error::Result;

#[cfg(unix)]
pub fn path_to_cstring(p: &Path) -> Result<CString> {
    use std::os::unix::ffi::OsStrExt;
    Ok(CString::new(p.as_os_str().as_bytes())?)
}

#[cfg(not(unix))]
pub fn path_to_cstring(p: &Path) -> Result<CString> {
    use crate::error::Error;

    let s = p.to_str().ok_or_else(|| Error::InvalidPath(p.to_owned()))?;
    Ok(CString::new(s)?)
}
