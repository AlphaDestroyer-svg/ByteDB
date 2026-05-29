use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::Mutex;

use crate::error::{CoreError, Result};
use crate::storage::disk_manager::DiskManager;
use crate::storage::page::PageId;
use crate::buffer::frame::BufferFrame;
use crate::buffer::replacer::LruKReplacer;

pub struct BufferPool {
    pool: Vec<Arc<BufferFrame>>,
    page_table: Mutex<HashMap<PageId, usize>>,
    free_list: Mutex<Vec<usize>>,
    replacer: LruKReplacer,
    disk_manager: Arc<DiskManager>,
    pool_size: usize,
}

impl BufferPool {
    pub fn new(pool_size: usize, disk_manager: Arc<DiskManager>) -> Self {
        let mut pool = Vec::with_capacity(pool_size);
        let mut free_list = Vec::with_capacity(pool_size);

        for i in 0..pool_size {
            pool.push(Arc::new(BufferFrame::new()));
            free_list.push(i);
        }

        BufferPool {
            pool,
            page_table: Mutex::new(HashMap::new()),
            free_list: Mutex::new(free_list),
            replacer: LruKReplacer::new(pool_size, 2),
            disk_manager,
            pool_size,
        }
    }

    pub fn fetch_page(&self, page_id: PageId) -> Result<Arc<BufferFrame>> {
        {
            let page_table = self.page_table.lock();
            if let Some(&frame_id) = page_table.get(&page_id) {
                let frame = &self.pool[frame_id];
                frame.pin();
                self.replacer.set_evictable(frame_id, false);
                self.replacer.record_access(frame_id);
                return Ok(Arc::clone(frame));
            }
        }

        let page = self.disk_manager.read_page(page_id)?;
        let frame_id = self.get_free_frame()?;
        let frame = &self.pool[frame_id];
        frame.reset(page);

        let mut page_table = self.page_table.lock();
        if let Some(&existing_frame_id) = page_table.get(&page_id) {
            self.free_list.lock().push(frame_id);
            let existing = &self.pool[existing_frame_id];
            existing.pin();
            self.replacer.set_evictable(existing_frame_id, false);
            self.replacer.record_access(existing_frame_id);
            return Ok(Arc::clone(existing));
        }
        self.replacer.record_access(frame_id);
        self.replacer.set_evictable(frame_id, false);
        page_table.insert(page_id, frame_id);

        Ok(Arc::clone(frame))
    }

    pub fn new_page(&self) -> Result<Arc<BufferFrame>> {
        let frame_id = self.get_free_frame()?;
        let page_id = self.disk_manager.allocate_page()?;

        let frame = &self.pool[frame_id];
        let page = crate::storage::page::Page::new(page_id);
        frame.reset(page);
        frame.mark_dirty();

        self.replacer.record_access(frame_id);
        self.replacer.set_evictable(frame_id, false);
        self.page_table.lock().insert(page_id, frame_id);

        Ok(Arc::clone(frame))
    }

    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> Result<()> {
        let page_table = self.page_table.lock();
        if let Some(&frame_id) = page_table.get(&page_id) {
            let frame = &self.pool[frame_id];
            if is_dirty {
                frame.mark_dirty();
            }
            frame.unpin();
            if !frame.is_pinned() {
                self.replacer.set_evictable(frame_id, true);
            }
        }
        Ok(())
    }

    pub fn flush_page(&self, page_id: PageId) -> Result<()> {
        let page_table = self.page_table.lock();
        if let Some(&frame_id) = page_table.get(&page_id) {
            let frame = &self.pool[frame_id];
            let page = frame.page.read();
            self.disk_manager.write_page(page_id, &*page)?;
            frame.is_dirty.store(false, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }

    pub fn flush_all(&self) -> Result<()> {
        let page_table = self.page_table.lock();
        for (&page_id, &frame_id) in page_table.iter() {
            let frame = &self.pool[frame_id];
            if frame.is_dirty.load(std::sync::atomic::Ordering::SeqCst) {
                let page = frame.page.read();
                self.disk_manager.write_page(page_id, &*page)?;
                frame.is_dirty.store(false, std::sync::atomic::Ordering::SeqCst);
            }
        }
        self.disk_manager.fsync()?;
        Ok(())
    }

    fn get_free_frame(&self) -> Result<usize> {
        let mut free_list = self.free_list.lock();
        if let Some(frame_id) = free_list.pop() {
            return Ok(frame_id);
        }
        drop(free_list);

        if let Some(frame_id) = self.replacer.evict() {
            let frame = &self.pool[frame_id];
            let old_page_id = frame.get_page_id();

            if frame.is_dirty.load(std::sync::atomic::Ordering::SeqCst) {
                let page = frame.page.read();
                self.disk_manager.write_page(old_page_id, &*page)?;
            }

            self.page_table.lock().remove(&old_page_id);
            return Ok(frame_id);
        }

        Err(CoreError::BufferPoolFull)
    }

    pub fn pool_size(&self) -> usize {
        self.pool_size
    }
}
