use crate::error::Result;
use std::{ffi::CString, path::Path};

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

#[cfg(test)]
mod tests {
    use super::path_to_cstring;
    use crate::error::Error;
    use std::path::Path;

    #[test]
    fn converts_ordinary_and_empty_paths() {
        let converted = path_to_cstring(Path::new("data/example.duckdb")).unwrap();
        assert_eq!(converted.as_bytes(), b"data/example.duckdb");
        assert!(path_to_cstring(Path::new("")).unwrap().as_bytes().is_empty());
    }

    #[test]
    fn rejects_path_with_embedded_nul() {
        let error = path_to_cstring(Path::new("data/\0example.duckdb")).unwrap_err();
        assert!(matches!(
            error,
            Error::NulError(ref source) if source.nul_position() == 5
        ));
    }

    #[cfg(unix)]
    #[test]
    fn preserves_non_utf8_path_bytes() {
        use std::{ffi::OsStr, os::unix::ffi::OsStrExt};

        let bytes = b"data/\xff.duckdb";
        let converted = path_to_cstring(Path::new(OsStr::from_bytes(bytes))).unwrap();
        assert_eq!(converted.as_bytes(), bytes);
    }

    #[cfg(windows)]
    #[test]
    fn rejects_non_utf8_path() {
        use std::{ffi::OsString, os::windows::ffi::OsStringExt, path::PathBuf};

        let path = PathBuf::from(OsString::from_wide(&[0xd800]));
        let error = path_to_cstring(&path).unwrap_err();
        assert!(matches!(error, Error::InvalidPath(ref invalid) if invalid == &path));
    }
}
