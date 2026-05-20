use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::Mutex;

use crate::error::{CoreError, Result};
use super::log_record::{LogRecord, Lsn};

const LOG_RECORD_HEADER_SIZE: usize = 8;

pub struct LogManager {
    log_path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    current_lsn: AtomicU64,
    flushed_lsn: AtomicU64,
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
        })
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

    pub fn flush(&self) -> Result<Lsn> {
        let mut writer = self.writer.lock();
        writer.flush()?;
        writer.get_ref().sync_all()?;
        let current = self.current_lsn.load(Ordering::SeqCst);
        self.flushed_lsn.store(current, Ordering::SeqCst);
        Ok(current)
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
