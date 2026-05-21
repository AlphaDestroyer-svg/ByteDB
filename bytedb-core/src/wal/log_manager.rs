use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use parking_lot::{Condvar, Mutex};

use crate::error::{CoreError, Result};
use super::log_record::{LogRecord, Lsn};

const LOG_RECORD_HEADER_SIZE: usize = 8;

const DEFAULT_GROUP_COMMIT_DELAY_US: u64 = 0;

pub struct LogManager {
    log_path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    current_lsn: AtomicU64,
    flushed_lsn: AtomicU64,

    flush_cv: Condvar,
    flush_cv_lock: Mutex<()>,

    flushing: AtomicBool,

    group_commit_delay_us: AtomicU64,

    fsyncs: AtomicU64,
    commits_served: AtomicU64,
}

impl LogManager {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let log_path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&log_path)?;

        let file_len = file.metadata()?.len();
        let initial_lsn = if file_len > 0 {
            Self::count_records(&log_path)?
        } else {
            0
        };

        Ok(LogManager {
            log_path,
            writer: Mutex::new(BufWriter::new(file)),
            current_lsn: AtomicU64::new(initial_lsn),
            flushed_lsn: AtomicU64::new(initial_lsn),
            flush_cv: Condvar::new(),
            flush_cv_lock: Mutex::new(()),
            flushing: AtomicBool::new(false),
            group_commit_delay_us: AtomicU64::new(DEFAULT_GROUP_COMMIT_DELAY_US),
            fsyncs: AtomicU64::new(0),
            commits_served: AtomicU64::new(0),
        })
    }

    pub fn set_group_commit_delay_us(&self, us: u64) {
        self.group_commit_delay_us.store(us, Ordering::Relaxed);
    }

    pub fn group_commit_delay_us(&self) -> u64 {
        self.group_commit_delay_us.load(Ordering::Relaxed)
    }

    pub fn fsync_count(&self) -> u64 {
        self.fsyncs.load(Ordering::Relaxed)
    }

    pub fn commits_served(&self) -> u64 {
        self.commits_served.load(Ordering::Relaxed)
    }

    pub fn append(&self, record: LogRecord) -> Result<Lsn> {
        let lsn = self.current_lsn.fetch_add(1, Ordering::SeqCst) + 1;
        let data = record.serialize();
        let len = data.len() as u32;

        let mut writer = self.writer.lock();
        writer.write_all(&len.to_le_bytes())?;
        let checksum = crc32fast::hash(&data);
        writer.write_all(&checksum.to_le_bytes())?;
        writer.write_all(&data)?;

        Ok(lsn)
    }

    pub fn flush_through(&self, target_lsn: Lsn) -> Result<Lsn> {

        if self.flushed_lsn.load(Ordering::Acquire) >= target_lsn {
            self.commits_served.fetch_add(1, Ordering::Relaxed);
            return Ok(self.flushed_lsn.load(Ordering::Acquire));
        }

        let am_leader = self
            .flushing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();

        if !am_leader {

            let mut g = self.flush_cv_lock.lock();
            while self.flushed_lsn.load(Ordering::Acquire) < target_lsn {
                self.flush_cv.wait(&mut g);
            }
            self.commits_served.fetch_add(1, Ordering::Relaxed);
            return Ok(self.flushed_lsn.load(Ordering::Acquire));
        }

        let delay_us = self.group_commit_delay_us.load(Ordering::Relaxed);
        if delay_us > 0 {
            std::thread::sleep(Duration::from_micros(delay_us));
        }

        let snapshot_lsn = self.current_lsn.load(Ordering::SeqCst);

        let result = (|| -> Result<()> {
            let mut writer = self.writer.lock();
            writer.flush()?;
            writer.get_ref().sync_all()?;
            Ok(())
        })();

        match result {
            Ok(()) => {
                self.flushed_lsn.store(snapshot_lsn, Ordering::Release);
                self.fsyncs.fetch_add(1, Ordering::Relaxed);
                self.commits_served.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.flushing.store(false, Ordering::Release);
                let _g = self.flush_cv_lock.lock();
                self.flush_cv.notify_all();
                return Err(e);
            }
        }

        self.flushing.store(false, Ordering::Release);
        let _g = self.flush_cv_lock.lock();
        self.flush_cv.notify_all();

        Ok(self.flushed_lsn.load(Ordering::Acquire))
    }

    pub fn flush(&self) -> Result<Lsn> {
        let target = self.current_lsn.load(Ordering::SeqCst);
        self.flush_through(target)
    }

    pub fn current_lsn(&self) -> Lsn {
        self.current_lsn.load(Ordering::SeqCst)
    }

    pub fn flushed_lsn(&self) -> Lsn {
        self.flushed_lsn.load(Ordering::SeqCst)
    }

    pub fn read_all_records(&self) -> Result<Vec<(Lsn, LogRecord)>> {
        let mut file = File::open(&self.log_path)?;
        let mut records = Vec::new();
        let mut lsn: Lsn = 0;

        loop {
            let mut header = [0u8; LOG_RECORD_HEADER_SIZE];
            match file.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(CoreError::Io(e)),
            }

            let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
            let expected_checksum = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);

            let mut data = vec![0u8; len];
            file.read_exact(&mut data)?;

            let actual_checksum = crc32fast::hash(&data);
            if actual_checksum != expected_checksum {
                return Err(CoreError::WalCorrupted(lsn));
            }

            lsn += 1;
            if let Some(record) = LogRecord::deserialize(&data) {
                records.push((lsn, record));
            } else {
                return Err(CoreError::WalCorrupted(lsn));
            }
        }

        Ok(records)
    }

    fn count_records(path: &Path) -> Result<u64> {
        let mut file = File::open(path)?;
        let mut count: u64 = 0;

        loop {
            let mut header = [0u8; LOG_RECORD_HEADER_SIZE];
            match file.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(CoreError::Io(e)),
            }

            let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
            file.seek(SeekFrom::Current(len as i64))?;
            count += 1;
        }

        Ok(count)
    }

    pub fn truncate(&self) -> Result<()> {
        let mut writer = self.writer.lock();
        writer.flush()?;
        let file = writer.get_mut();
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        self.current_lsn.store(0, Ordering::SeqCst);
        self.flushed_lsn.store(0, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod group_commit_tests {
    use super::*;
    use crate::wal::log_record::LogRecord;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use tempfile::tempdir;

    fn fresh() -> (tempfile::TempDir, Arc<LogManager>) {
        let d = tempdir().unwrap();
        let p = d.path().join("wal.log");
        let lm = Arc::new(LogManager::new(&p).unwrap());
        (d, lm)
    }

    fn append_commit(lm: &LogManager) -> Lsn {
        lm.append(LogRecord::Commit { txn_id: 1 }).unwrap()
    }

    #[test]
    fn single_thread_flush_advances_flushed_lsn() {
        let (_d, lm) = fresh();
        let l1 = append_commit(&lm);
        let l2 = append_commit(&lm);
        let f = lm.flush().unwrap();
        assert!(f >= l2);
        assert!(f >= l1);
        assert_eq!(lm.fsync_count(), 1);
    }

    #[test]
    fn group_commit_batches_concurrent_flushes() {

        let (_d, lm) = fresh();

        lm.set_group_commit_delay_us(2_000);

        let total_committers: u64 = 32;
        let started = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for _ in 0..total_committers {
            let lm = Arc::clone(&lm);
            let started = Arc::clone(&started);
            handles.push(thread::spawn(move || {
                let lsn = append_commit(&lm);
                started.fetch_add(1, Ordering::Relaxed);
                lm.flush_through(lsn).unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(lm.commits_served(), total_committers);
        assert!(
            lm.fsync_count() < total_committers,
            "expected batching: got {} fsyncs for {} commits",
            lm.fsync_count(),
            total_committers
        );
    }

    #[test]
    fn flush_through_skips_when_already_durable() {
        let (_d, lm) = fresh();
        let l1 = append_commit(&lm);
        lm.flush_through(l1).unwrap();
        let fsyncs_before = lm.fsync_count();

        lm.flush_through(l1).unwrap();
        assert_eq!(lm.fsync_count(), fsyncs_before);
    }
}
