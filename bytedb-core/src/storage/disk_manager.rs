use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use parking_lot::Mutex;

use crate::error::Result;
use crate::storage::page::{Page, PageId, PAGE_SIZE};

pub struct DiskManager {
    db_path: PathBuf,
    file: Mutex<File>,
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
            file: Mutex::new(file),
            next_page_id: AtomicU32::new(num_pages),
        })
    }

    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(offset))?;

        let mut page = Page::new(page_id);
        file.read_exact(&mut page.data)?;
        page.id = page_id;
        Ok(page)
    }

    pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<()> {
        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&page.data)?;
        Ok(())
    }

    pub fn allocate_page(&self) -> Result<PageId> {
        let page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
        let page = Page::new(page_id);
        self.write_page(page_id, &page)?;
        Ok(page_id)
    }

    pub fn deallocate_page(&self, _page_id: PageId) -> Result<()> {
        Ok(())
    }

    pub fn fsync(&self) -> Result<()> {
        let file = self.file.lock();
        file.sync_all()?;
        Ok(())
    }

    pub fn num_pages(&self) -> u32 {
        self.next_page_id.load(Ordering::SeqCst)
    }

    pub fn path(&self) -> &Path {
        &self.db_path
    }
}
