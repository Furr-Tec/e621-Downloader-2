use std::path::Path;
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    pub size: u64,
    pub mtime: SystemTime,
    pub ctime: Option<SystemTime>,
}

pub fn get_file_metadata(path: &Path) -> io::Result<FileMeta> {
    let md = path.metadata()?;
    let size = md.len();
    let mtime = md.modified().unwrap_or(UNIX_EPOCH);
    #[cfg(unix)]
    let ctime = {
        use std::os::unix::fs::MetadataExt;
        Some(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(md.ctime() as u64))
    };
    #[cfg(not(unix))]
    let ctime = None;
    Ok(FileMeta { size, mtime, ctime })
}

pub fn file_metadata_changed(new: &FileMeta, old: &FileMeta) -> bool {
    new.size != old.size
        || new.mtime != old.mtime
        || new.ctime != old.ctime
}

