use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{CoreError, Result};
use crate::wal::log_manager::LogManager;

pub const BACKUP_MANIFEST_MAGIC: [u8; 4] = *b"BBKM";
pub const BACKUP_MANIFEST_VERSION: u32 = 1;

pub struct BackupManifest {
    pub source_data_dir: PathBuf,
    pub backup_lsn: u64,
    pub timestamp: i64,
    pub source_format_version: u32,
}

pub struct Backup;

impl Backup {
    pub fn create(
        source_data_dir: &Path,
        wal: &LogManager,
        backup_dir: &Path,
        source_format_version: u32,
    ) -> Result<BackupManifest> {
        fs::create_dir_all(backup_dir).map_err(CoreError::Io)?;

        let backup_lsn = wal.flush()?;

        let dst_wal = backup_dir.join("bytedb.wal");
        let src_wal = source_data_dir.join("bytedb.wal");
        if src_wal.exists() {
            copy_file(&src_wal, &dst_wal)?;
        }

        let src_snap = source_data_dir.join("snapshots");
        let dst_snap = backup_dir.join("snapshots");
        if src_snap.exists() {
            copy_dir(&src_snap, &dst_snap)?;
        }

        let src_dbs = source_data_dir.join("databases");
        let dst_dbs = backup_dir.join("databases");
        if src_dbs.exists() {
            copy_dir(&src_dbs, &dst_dbs)?;
        }

        let src_meta = source_data_dir.join("server.meta");
        let dst_meta = backup_dir.join("server.meta");
        if src_meta.exists() {
            copy_file(&src_meta, &dst_meta)?;
        }

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let manifest = BackupManifest {
            source_data_dir: source_data_dir.to_path_buf(),
            backup_lsn,
            timestamp,
            source_format_version,
        };
        write_manifest(&backup_dir.join("manifest.bin"), &manifest)?;
        Ok(manifest)
    }

    pub fn read_manifest(backup_dir: &Path) -> Result<BackupManifest> {
        read_manifest(&backup_dir.join("manifest.bin"))
    }

    pub fn restore_to(
        backup_dir: &Path,
        target_data_dir: &Path,
    ) -> Result<BackupManifest> {
        let manifest = Self::read_manifest(backup_dir)?;

        let staging = target_data_dir.with_file_name(match target_data_dir.file_name() {
            Some(n) => format!("{}.restore_staging", n.to_string_lossy()),
            None => "restore_staging".to_string(),
        });
        if staging.exists() {
            fs::remove_dir_all(&staging).map_err(CoreError::Io)?;
        }
        fs::create_dir_all(&staging).map_err(CoreError::Io)?;

        let copy_all = (|| -> Result<()> {
            for name in &["bytedb.wal", "server.meta"] {
                let src = backup_dir.join(name);
                if src.exists() {
                    copy_file(&src, &staging.join(name))?;
                }
            }
            for name in &["snapshots", "databases"] {
                let src = backup_dir.join(name);
                if src.exists() {
                    copy_dir(&src, &staging.join(name))?;
                }
            }
            Ok(())
        })();

        if let Err(e) = copy_all {
            let _ = fs::remove_dir_all(&staging);
            return Err(e);
        }

        if target_data_dir.exists() {
            for entry in fs::read_dir(target_data_dir).map_err(CoreError::Io)? {
                let p = entry.map_err(CoreError::Io)?.path();
                if p.is_dir() {
                    fs::remove_dir_all(&p).map_err(CoreError::Io)?;
                } else {
                    fs::remove_file(&p).map_err(CoreError::Io)?;
                }
            }
        } else {
            fs::create_dir_all(target_data_dir).map_err(CoreError::Io)?;
        }

        for entry in fs::read_dir(&staging).map_err(CoreError::Io)? {
            let entry = entry.map_err(CoreError::Io)?;
            let from = entry.path();
            let to = target_data_dir.join(entry.file_name());
            fs::rename(&from, &to).map_err(CoreError::Io)?;
        }
        let _ = fs::remove_dir_all(&staging);
        Ok(manifest)
    }
}

fn copy_file(src: &Path, dst: &Path) -> Result<()> {
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent).map_err(CoreError::Io)?;
    }
    let tmp = dst.with_extension("tmp.bkp");
    fs::copy(src, &tmp).map_err(CoreError::Io)?;
    if let Ok(f) = fs::File::open(&tmp) {
        let _ = f.sync_all();
    }
    fs::rename(&tmp, dst).map_err(CoreError::Io)?;
    Ok(())
}

fn copy_dir(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst).map_err(CoreError::Io)?;
    for entry in fs::read_dir(src).map_err(CoreError::Io)? {
        let entry = entry.map_err(CoreError::Io)?;
        let path = entry.path();
        let name = entry.file_name();
        let target = dst.join(&name);
        if path.is_dir() {
            copy_dir(&path, &target)?;
        } else {
            if path.extension().and_then(|s| s.to_str()) == Some("tmp") {
                continue;
            }
            copy_file(&path, &target)?;
        }
    }
    Ok(())
}

fn write_manifest(path: &Path, m: &BackupManifest) -> Result<()> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&BACKUP_MANIFEST_MAGIC);
    buf.extend_from_slice(&BACKUP_MANIFEST_VERSION.to_le_bytes());
    buf.extend_from_slice(&m.backup_lsn.to_le_bytes());
    buf.extend_from_slice(&m.timestamp.to_le_bytes());
    buf.extend_from_slice(&m.source_format_version.to_le_bytes());
    let src = m.source_data_dir.to_string_lossy();
    let bytes = src.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
    let crc = crc32fast::hash(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    let mut f = fs::File::create(path).map_err(CoreError::Io)?;
    f.write_all(&buf).map_err(CoreError::Io)?;
    f.sync_all().map_err(CoreError::Io)?;
    Ok(())
}

fn read_manifest(path: &Path) -> Result<BackupManifest> {
    let mut f = fs::File::open(path).map_err(CoreError::Io)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).map_err(CoreError::Io)?;
    if buf.len() < 4 + 4 + 8 + 8 + 4 + 4 + 4 {
        return Err(CoreError::Internal("backup manifest truncated".into()));
    }
    let crc_pos = buf.len() - 4;
    let stored = u32::from_le_bytes([buf[crc_pos], buf[crc_pos + 1], buf[crc_pos + 2], buf[crc_pos + 3]]);
    let payload = &buf[..crc_pos];
    if crc32fast::hash(payload) != stored {
        return Err(CoreError::Internal("backup manifest crc mismatch".into()));
    }
    if &payload[..4] != BACKUP_MANIFEST_MAGIC {
        return Err(CoreError::Internal("backup manifest bad magic".into()));
    }
    let version = u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);
    if version != BACKUP_MANIFEST_VERSION {
        return Err(CoreError::Internal(format!(
            "backup manifest unsupported version {}", version
        )));
    }
    let backup_lsn = u64::from_le_bytes(payload[8..16].try_into().unwrap());
    let timestamp = i64::from_le_bytes(payload[16..24].try_into().unwrap());
    let source_format_version = u32::from_le_bytes(payload[24..28].try_into().unwrap());
    let path_len = u32::from_le_bytes(payload[28..32].try_into().unwrap()) as usize;
    if payload.len() < 32 + path_len {
        return Err(CoreError::Internal("backup manifest path truncated".into()));
    }
    let path_bytes = &payload[32..32 + path_len];
    let source_data_dir = PathBuf::from(String::from_utf8_lossy(path_bytes).to_string());
    Ok(BackupManifest {
        source_data_dir,
        backup_lsn,
        timestamp,
        source_format_version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::log_record::LogRecord;
    use tempfile::tempdir;

    #[test]
    fn backup_round_trip_preserves_files() {
        let src = tempdir().unwrap();
        let bk = tempdir().unwrap();
        let dst = tempdir().unwrap();

        fs::create_dir_all(src.path().join("databases/main/tables")).unwrap();
        fs::write(src.path().join("databases/main/tables/a.tbl"), b"hello").unwrap();
        fs::create_dir_all(src.path().join("snapshots")).unwrap();
        fs::write(src.path().join("snapshots/snapshot_0000000000000001_1.bin"), b"snap").unwrap();

        let wal_path = src.path().join("bytedb.wal");
        let wal = LogManager::new(&wal_path).unwrap();
        wal.append(LogRecord::Begin { txn_id: 1 }).unwrap();
        wal.append(LogRecord::Commit { txn_id: 1 }).unwrap();
        wal.flush().unwrap();

        let m = Backup::create(src.path(), &wal, bk.path(), 2).unwrap();
        assert!(m.backup_lsn >= 2);

        let m2 = Backup::restore_to(bk.path(), dst.path()).unwrap();
        assert_eq!(m.backup_lsn, m2.backup_lsn);
        assert!(dst.path().join("bytedb.wal").exists());
        assert!(dst.path().join("databases/main/tables/a.tbl").exists());
        assert!(dst.path().join("snapshots/snapshot_0000000000000001_1.bin").exists());
        let body = fs::read(dst.path().join("databases/main/tables/a.tbl")).unwrap();
        assert_eq!(body, b"hello");
    }

    #[test]
    fn manifest_crc_detects_corruption() {
        let bk = tempdir().unwrap();
        let m = BackupManifest {
            source_data_dir: PathBuf::from("/tmp/src"),
            backup_lsn: 42,
            timestamp: 1,
            source_format_version: 2,
        };
        let mp = bk.path().join("manifest.bin");
        write_manifest(&mp, &m).unwrap();
        let mut data = fs::read(&mp).unwrap();
        data[10] ^= 0xff;
        fs::write(&mp, data).unwrap();
        let r = read_manifest(&mp);
        assert!(r.is_err());
    }

    #[test]
    fn failed_restore_preserves_existing_target() {
        let bk = tempdir().unwrap();
        let dst = tempdir().unwrap();

        // existing target data that must NOT be destroyed by a failed restore
        fs::write(dst.path().join("important.tbl"), b"keep me").unwrap();

        // backup dir has no manifest -> restore must fail before touching target
        let r = Backup::restore_to(bk.path(), dst.path());
        assert!(r.is_err(), "restore from invalid backup must fail");
        assert!(dst.path().join("important.tbl").exists(),
            "existing target data must survive a failed restore");
        assert_eq!(fs::read(dst.path().join("important.tbl")).unwrap(), b"keep me");
    }

    #[test]
    fn restore_replaces_target_atomically() {
        let src = tempdir().unwrap();
        let bk = tempdir().unwrap();
        let dst = tempdir().unwrap();

        fs::create_dir_all(src.path().join("databases/main/tables")).unwrap();
        fs::write(src.path().join("databases/main/tables/new.tbl"), b"new data").unwrap();
        let wal = LogManager::new(&src.path().join("bytedb.wal")).unwrap();
        wal.append(LogRecord::Commit { txn_id: 1 }).unwrap();
        wal.flush().unwrap();
        Backup::create(src.path(), &wal, bk.path(), 1).unwrap();

        fs::write(dst.path().join("stale.tbl"), b"old").unwrap();

        Backup::restore_to(bk.path(), dst.path()).unwrap();
        assert!(!dst.path().join("stale.tbl").exists(), "stale data must be cleared");
        assert!(dst.path().join("databases/main/tables/new.tbl").exists());
    }
}
