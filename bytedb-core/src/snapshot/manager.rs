use std::fs;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{CoreError, Result};
use super::format::*;
use super::binary;
use super::json;

pub struct SnapshotManager {
    data_dir: PathBuf,
    write_count: AtomicU64,
    write_threshold: u64,
    interval: Duration,
    last_snapshot_lsn: AtomicU64,
    format: SnapshotFormat,
}

impl SnapshotManager {
    pub fn new(data_dir: PathBuf, write_threshold: u64, interval_secs: u64, format: SnapshotFormat) -> Self {
        fs::create_dir_all(&data_dir).ok();
        SnapshotManager {
            data_dir,
            write_count: AtomicU64::new(0),
            write_threshold,
            interval: Duration::from_secs(interval_secs),
            last_snapshot_lsn: AtomicU64::new(0),
            format,
        }
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    pub fn format(&self) -> SnapshotFormat {
        self.format
    }

    pub fn record_write(&self) -> bool {
        let count = self.write_count.fetch_add(1, AtomicOrdering::Relaxed) + 1;
        count >= self.write_threshold
    }

    pub fn reset_write_count(&self) {
        self.write_count.store(0, AtomicOrdering::Relaxed);
    }

    pub fn last_lsn(&self) -> u64 {
        self.last_snapshot_lsn.load(AtomicOrdering::Relaxed)
    }

    pub fn save(&self, snapshot: &FullSnapshot) -> Result<PathBuf> {
        match self.format {
            SnapshotFormat::Binary => self.save_binary(snapshot),
            SnapshotFormat::Json => self.save_json(snapshot),
        }
    }

    pub fn save_binary(&self, snapshot: &FullSnapshot) -> Result<PathBuf> {
        let filename = format!("snapshot_{:016x}_{}.bin", snapshot.header.lsn, snapshot.header.timestamp);
        let path = self.data_dir.join(&filename);
        let tmp_path = path.with_extension("tmp");

        let file = fs::File::create(&tmp_path)
            .map_err(|e| CoreError::Internal(e.to_string()))?;
        let mut writer = BufWriter::new(file);
        binary::serialize_snapshot(snapshot, &mut writer)?;
        drop(writer);

        fs::rename(&tmp_path, &path)
            .map_err(|e| CoreError::Internal(e.to_string()))?;

        self.last_snapshot_lsn.store(snapshot.header.lsn, AtomicOrdering::Relaxed);
        self.reset_write_count();
        Ok(path)
    }

    pub fn save_json(&self, snapshot: &FullSnapshot) -> Result<PathBuf> {
        let filename = format!("snapshot_{:016x}_{}.json", snapshot.header.lsn, snapshot.header.timestamp);
        let path = self.data_dir.join(&filename);
        let tmp_path = path.with_extension("tmp");

        let file = fs::File::create(&tmp_path)
            .map_err(|e| CoreError::Internal(e.to_string()))?;
        let mut writer = BufWriter::new(file);
        json::serialize_snapshot(snapshot, &mut writer)?;
        drop(writer);

        fs::rename(&tmp_path, &path)
            .map_err(|e| CoreError::Internal(e.to_string()))?;

        self.last_snapshot_lsn.store(snapshot.header.lsn, AtomicOrdering::Relaxed);
        self.reset_write_count();
        Ok(path)
    }

    pub fn load_latest(&self) -> Result<Option<FullSnapshot>> {
        let bin_snapshot = self.find_latest_file("bin");
        let json_snapshot = self.find_latest_file("json");

        let latest = match (bin_snapshot, json_snapshot) {
            (Some(b), Some(j)) => {
                if b.0 >= j.0 { Some(("bin", b.1)) } else { Some(("json", j.1)) }
            }
            (Some(b), None) => Some(("bin", b.1)),
            (None, Some(j)) => Some(("json", j.1)),
            (None, None) => None,
        };

        match latest {
            Some(("bin", path)) => {
                let file = fs::File::open(&path)
                    .map_err(|e| CoreError::Internal(e.to_string()))?;
                let mut reader = std::io::BufReader::new(file);
                let snapshot = binary::deserialize_snapshot(&mut reader)?;
                self.last_snapshot_lsn.store(snapshot.header.lsn, AtomicOrdering::Relaxed);
                Ok(Some(snapshot))
            }
            Some(("json", path)) => {
                let file = fs::File::open(&path)
                    .map_err(|e| CoreError::Internal(e.to_string()))?;
                let mut reader = std::io::BufReader::new(file);
                let snapshot = json::deserialize_snapshot(&mut reader)?;
                self.last_snapshot_lsn.store(snapshot.header.lsn, AtomicOrdering::Relaxed);
                Ok(Some(snapshot))
            }
            _ => Ok(None),
        }
    }

    fn find_latest_file(&self, ext: &str) -> Option<(u64, PathBuf)> {
        let entries = fs::read_dir(&self.data_dir).ok()?;
        let mut latest: Option<(u64, PathBuf)> = None;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some(ext) {
                if let Some(name) = path.file_stem().and_then(|n| n.to_str()) {
                    if name.starts_with("snapshot_") {
                        let parts: Vec<&str> = name.splitn(3, '_').collect();
                        if parts.len() >= 2 {
                            if let Ok(lsn) = u64::from_str_radix(parts[1], 16) {
                                match &latest {
                                    Some((current_lsn, _)) if lsn <= *current_lsn => {}
                                    _ => { latest = Some((lsn, path)); }
                                }
                            }
                        }
                    }
                }
            }
        }

        latest
    }

    pub fn create_snapshot_header(&self, lsn: u64, table_count: u32) -> SnapshotHeader {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        SnapshotHeader {
            version: SNAPSHOT_VERSION,
            lsn,
            timestamp,
            table_count,
        }
    }
}
