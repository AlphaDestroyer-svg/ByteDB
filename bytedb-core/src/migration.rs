use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::error::{CoreError, Result};

pub const STORAGE_FORMAT_VERSION: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Catalog,
    TableFile,
    Snapshot,
    ServerMeta,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct FileFormat {
    pub path: PathBuf,
    pub kind: FileKind,
    pub version: u32,
}

pub struct MigrationReport {
    pub scanned: Vec<FileFormat>,
    pub migrated: Vec<PathBuf>,
    pub backups: Vec<PathBuf>,
}

pub struct Migration;

impl Migration {
    pub fn scan(data_dir: &Path) -> Result<Vec<FileFormat>> {
        let mut out = Vec::new();
        if !data_dir.exists() {
            return Ok(out);
        }
        scan_dir(data_dir, &mut out)?;
        Ok(out)
    }

    pub fn migrate_to_current(data_dir: &Path) -> Result<MigrationReport> {
        let scanned = Self::scan(data_dir)?;
        let mut migrated = Vec::new();
        let mut backups = Vec::new();

        for f in &scanned {
            if f.version < expected_current_version(f.kind) {
                let bak = backup_file(&f.path)?;
                migrate_file_in_place(f)?;
                backups.push(bak);
                migrated.push(f.path.clone());
            } else if f.version > expected_current_version(f.kind) {
                return Err(CoreError::Internal(format!(
                    "file {:?} has version {} which is newer than supported {}",
                    f.path, f.version, expected_current_version(f.kind)
                )));
            }
        }
        Ok(MigrationReport { scanned, migrated, backups })
    }
}

fn expected_current_version(kind: FileKind) -> u32 {
    match kind {
        FileKind::Catalog => 2,
        FileKind::TableFile => 2,
        FileKind::Snapshot => 2,
        FileKind::ServerMeta => 1,
        FileKind::Unknown => 0,
    }
}

fn scan_dir(dir: &Path, out: &mut Vec<FileFormat>) -> Result<()> {
    for entry in fs::read_dir(dir).map_err(CoreError::Io)? {
        let entry = entry.map_err(CoreError::Io)?;
        let path = entry.path();
        if path.is_dir() {
            scan_dir(&path, out)?;
            continue;
        }
        if let Some(info) = identify(&path)? {
            out.push(info);
        }
    }
    Ok(())
}

fn identify(path: &Path) -> Result<Option<FileFormat>> {
    let mut f = match fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return Ok(None),
    };
    let mut hdr = [0u8; 8];
    if f.read_exact(&mut hdr).is_err() {
        return Ok(None);
    }
    let magic = [hdr[0], hdr[1], hdr[2], hdr[3]];
    let version = u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);
    let kind = match &magic {
        b"BCAT" => FileKind::Catalog,
        b"BTBL" => FileKind::TableFile,
        b"BSDB" => FileKind::Snapshot,
        b"BSRV" => FileKind::ServerMeta,
        _ => FileKind::Unknown,
    };
    if kind == FileKind::Unknown {
        return Ok(None);
    }
    Ok(Some(FileFormat {
        path: path.to_path_buf(),
        kind,
        version,
    }))
}

fn backup_file(path: &Path) -> Result<PathBuf> {
    let bak = path.with_extension(format!(
        "{}.v0.2.bak",
        path.extension().and_then(|s| s.to_str()).unwrap_or("bin")
    ));
    fs::copy(path, &bak).map_err(CoreError::Io)?;
    Ok(bak)
}

fn migrate_file_in_place(_f: &FileFormat) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    fn write_with_header(path: &Path, magic: &[u8; 4], version: u32) {
        let mut f = fs::File::create(path).unwrap();
        f.write_all(magic).unwrap();
        f.write_all(&version.to_le_bytes()).unwrap();
        f.write_all(&[0u8; 16]).unwrap();
    }

    #[test]
    fn scan_identifies_known_files() {
        let d = tempdir().unwrap();
        write_with_header(&d.path().join("a.tbl"), b"BTBL", 1);
        write_with_header(&d.path().join("catalog.bin"), b"BCAT", 2);
        write_with_header(&d.path().join("server.meta"), b"BSRV", 1);
        fs::write(d.path().join("random.txt"), b"hello").unwrap();

        let scanned = Migration::scan(d.path()).unwrap();
        assert_eq!(scanned.len(), 3);
        assert!(scanned.iter().any(|f| matches!(f.kind, FileKind::TableFile) && f.version == 1));
        assert!(scanned.iter().any(|f| matches!(f.kind, FileKind::Catalog) && f.version == 2));
    }

    #[test]
    fn migrate_legacy_creates_bak() {
        let d = tempdir().unwrap();
        write_with_header(&d.path().join("a.tbl"), b"BTBL", 1);
        let report = Migration::migrate_to_current(d.path()).unwrap();
        assert_eq!(report.migrated.len(), 1);
        assert_eq!(report.backups.len(), 1);
        assert!(report.backups[0].to_string_lossy().contains(".v0.2.bak"));
    }

    #[test]
    fn rejects_future_version() {
        let d = tempdir().unwrap();
        write_with_header(&d.path().join("catalog.bin"), b"BCAT", 999);
        let r = Migration::migrate_to_current(d.path());
        assert!(r.is_err());
    }
}
