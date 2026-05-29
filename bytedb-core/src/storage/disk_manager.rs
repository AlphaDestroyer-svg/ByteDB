use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use parking_lot::Mutex;

use crate::error::{CoreError, Result};
use crate::storage::page::{Page, PageId, PAGE_SIZE};

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

pub struct DiskManager {
    db_path: PathBuf,
    file: File,
    write_lock: Mutex<()>,
    next_page_id: AtomicU32,
}

impl DiskManager {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let db_path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&db_path)?;

        let file_len = file.metadata()?.len();
        let num_pages = if file_len == 0 {
            1
        } else {
            (file_len as u32) / (PAGE_SIZE as u32)
        };

        Ok(DiskManager {
            db_path,
            file,
            write_lock: Mutex::new(()),
            next_page_id: AtomicU32::new(num_pages),
        })
    }

    #[cfg(unix)]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        self.file.read_exact_at(buf, offset)
    }

    #[cfg(unix)]
    fn write_at(&self, buf: &[u8], offset: u64) -> std::io::Result<()> {
        self.file.write_all_at(buf, offset)
    }

    #[cfg(windows)]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        let mut pos = 0;
        while pos < buf.len() {
            let n = self.file.seek_read(&mut buf[pos..], offset + pos as u64)?;
            if n == 0 {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof"));
            }
            pos += n;
        }
        Ok(())
    }

    #[cfg(windows)]
    fn write_at(&self, buf: &[u8], offset: u64) -> std::io::Result<()> {
        let mut pos = 0;
        while pos < buf.len() {
            let n = self.file.seek_write(&buf[pos..], offset + pos as u64)?;
            if n == 0 {
                return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "write zero"));
            }
            pos += n;
        }
        Ok(())
    }

    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        let mut page = Page::new(page_id);
        self.read_at(&mut page.data, offset)?;
        page.id = page_id;

        let stored = page.checksum();
        if stored != 0 {
            let computed = page.compute_checksum();
            if stored != computed {
                return Err(CoreError::ChecksumMismatch(page_id));
            }
        }
        Ok(page)
    }

    pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<()> {
        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        let mut buf = [0u8; PAGE_SIZE];
        buf.copy_from_slice(&page.data);
        let cs_offset = Page::checksum_offset();
        buf[cs_offset..cs_offset + 4].copy_from_slice(&0u32.to_le_bytes());
        let cs = Page::compute_checksum_bytes(&buf);
        buf[cs_offset..cs_offset + 4].copy_from_slice(&cs.to_le_bytes());
        self.write_at(&buf, offset)?;
        Ok(())
    }

    pub fn allocate_page(&self) -> Result<PageId> {
        let _g = self.write_lock.lock();
        let page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
        let page = Page::new(page_id);
        self.write_page(page_id, &page)?;
        Ok(page_id)
    }

    pub fn deallocate_page(&self, _page_id: PageId) -> Result<()> {
        Ok(())
    }

    pub fn fsync(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    pub fn num_pages(&self) -> u32 {
        self.next_page_id.load(Ordering::SeqCst)
    }

    pub fn path(&self) -> &Path {
        &self.db_path
    }

    #[doc(hidden)]
    pub fn corrupt_byte_for_test(&self, page_id: PageId, byte_index: usize) -> Result<()> {
        let offset = (page_id as u64) * (PAGE_SIZE as u64) + byte_index as u64;
        let mut b = [0u8; 1];
        self.read_at(&mut b, offset)?;
        b[0] ^= 0xFF;
        self.write_at(&b, offset)?;
        self.file.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod checksum_tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn write_then_read_succeeds_with_checksum() {
        let d = tempdir().unwrap();
        let dm = DiskManager::new(d.path().join("db.bin")).unwrap();
        let pid = dm.allocate_page().unwrap();
        let mut p = Page::new(pid);
        p.alloc_slot(b"hello").unwrap();
        dm.write_page(pid, &p).unwrap();
        let r = dm.read_page(pid).unwrap();
        assert_eq!(r.get_slot(0).unwrap(), b"hello");
    }

    #[test]
    fn corrupted_page_is_detected() {
        let d = tempdir().unwrap();
        let dm = DiskManager::new(d.path().join("db.bin")).unwrap();
        let pid = dm.allocate_page().unwrap();
        let mut p = Page::new(pid);
        p.alloc_slot(b"hello").unwrap();
        dm.write_page(pid, &p).unwrap();
        dm.corrupt_byte_for_test(pid, 100).unwrap();
        let err = dm.read_page(pid).unwrap_err();
        match err {
            CoreError::ChecksumMismatch(p) => assert_eq!(p, pid),
            other => panic!("expected ChecksumMismatch, got {:?}", other),
        }
    }
}
