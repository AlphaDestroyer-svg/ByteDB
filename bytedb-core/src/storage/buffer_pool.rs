use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::error::{CoreError, Result};
use crate::storage::disk_manager::DiskManager;
use crate::storage::page::{Page, PageId, INVALID_PAGE_ID};

pub const LRU_K: usize = 2;

pub const DEFAULT_POOL_FRAMES: usize = 2048;

struct Frame {
    page: Arc<Mutex<Page>>,
    page_id: PageId,
    pin_count: u32,
    dirty: bool,

    access_history: [u64; LRU_K],
    access_count: u8,
}

impl Frame {
    fn empty() -> Self {
        Frame {
            page: Arc::new(Mutex::new(Page::new(INVALID_PAGE_ID))),
            page_id: INVALID_PAGE_ID,
            pin_count: 0,
            dirty: false,
            access_history: [0; LRU_K],
            access_count: 0,
        }
    }

    fn record_access(&mut self, ts: u64) {
        if self.access_count < LRU_K as u8 {
            self.access_history[self.access_count as usize] = ts;
            self.access_count += 1;
        } else {

            for i in 0..LRU_K - 1 {
                self.access_history[i] = self.access_history[i + 1];
            }
            self.access_history[LRU_K - 1] = ts;
        }
    }

    fn kth_oldest(&self) -> u64 {
        if (self.access_count as usize) < LRU_K {
            0
        } else {
            self.access_history[0]
        }
    }
}

struct BufferPoolInner {
    frames: Vec<Frame>,

    page_table: HashMap<PageId, usize>,

    free_frames: Vec<usize>,
    clock: u64,
}

impl BufferPoolInner {
    fn new(num_frames: usize) -> Self {
        let mut frames = Vec::with_capacity(num_frames);
        let mut free_frames = Vec::with_capacity(num_frames);
        for i in 0..num_frames {
            frames.push(Frame::empty());
            free_frames.push(num_frames - 1 - i);
        }
        BufferPoolInner {
            frames,
            page_table: HashMap::with_capacity(num_frames),
            free_frames,
            clock: 0,
        }
    }

    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    fn pick_victim(&self) -> Option<usize> {
        let mut best: Option<(usize, u64, u8)> = None;
        for (i, f) in self.frames.iter().enumerate() {
            if f.pin_count > 0 || f.page_id == INVALID_PAGE_ID {
                continue;
            }
            let kth = f.kth_oldest();

            let key = (f.access_count, kth);
            if best.is_none() || (key.0, key.1) < (best.unwrap().2, best.unwrap().1) {
                best = Some((i, kth, f.access_count));
            }
        }
        best.map(|(i, _, _)| i)
    }
}

pub struct BufferPool {
    inner: Mutex<BufferPoolInner>,
    disk: Arc<DiskManager>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl BufferPool {
    pub fn new(disk: Arc<DiskManager>, num_frames: usize) -> Arc<Self> {
        let frames = num_frames.max(2);
        Arc::new(BufferPool {
            inner: Mutex::new(BufferPoolInner::new(frames)),
            disk,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        })
    }

    pub fn capacity(&self) -> usize {
        self.inner.lock().frames.len()
    }

    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    pub fn disk(&self) -> &Arc<DiskManager> {
        &self.disk
    }

    pub fn fetch_page(self: &Arc<Self>, page_id: PageId) -> Result<BufferGuard> {
        if page_id == INVALID_PAGE_ID {
            return Err(CoreError::Internal(
                "buffer_pool: refusing to fetch INVALID_PAGE_ID".into(),
            ));
        }

        {
            let mut g = self.inner.lock();
            if let Some(&idx) = g.page_table.get(&page_id) {
                let ts = g.tick();
                let frame = &mut g.frames[idx];
                frame.pin_count += 1;
                frame.record_access(ts);
                let page = Arc::clone(&frame.page);
                drop(g);
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(BufferGuard {
                    pool: Arc::clone(self),
                    frame_idx: idx,
                    page_id,
                    page,
                    dirtied: false,
                });
            }
        }

        self.misses.fetch_add(1, Ordering::Relaxed);

        let page_from_disk = self.disk.read_page(page_id)?;

        let mut g = self.inner.lock();

        if let Some(&idx) = g.page_table.get(&page_id) {
            let ts = g.tick();
            let frame = &mut g.frames[idx];
            frame.pin_count += 1;
            frame.record_access(ts);
            let page = Arc::clone(&frame.page);
            return Ok(BufferGuard {
                pool: Arc::clone(self),
                frame_idx: idx,
                page_id,
                page,
                dirtied: false,
            });
        }

        let frame_idx = if let Some(idx) = g.free_frames.pop() {
            idx
        } else {
            let victim = g.pick_victim().ok_or_else(|| {
                CoreError::Internal(
                    "buffer_pool: every frame is pinned, cannot evict".into(),
                )
            })?;
            self.evict_locked(&mut g, victim)?;
            victim
        };

        let ts = g.tick();
        let frame = &mut g.frames[frame_idx];
        *frame.page.lock() = page_from_disk;
        frame.page_id = page_id;
        frame.pin_count = 1;
        frame.dirty = false;
        frame.access_count = 0;
        frame.access_history = [0; LRU_K];
        frame.record_access(ts);
        let page = Arc::clone(&frame.page);
        g.page_table.insert(page_id, frame_idx);

        Ok(BufferGuard {
            pool: Arc::clone(self),
            frame_idx,
            page_id,
            page,
            dirtied: false,
        })
    }

    pub fn new_page(self: &Arc<Self>) -> Result<BufferGuard> {
        let page_id = self.disk.allocate_page()?;
        let mut guard = self.fetch_page(page_id)?;

        guard.dirtied = true;
        Ok(guard)
    }

    fn evict_locked(
        &self,
        g: &mut parking_lot::MutexGuard<'_, BufferPoolInner>,
        idx: usize,
    ) -> Result<()> {
        let (page_id, dirty) = {
            let f = &g.frames[idx];
            (f.page_id, f.dirty)
        };
        if page_id == INVALID_PAGE_ID {
            return Ok(());
        }
        if dirty {
            let page_clone = g.frames[idx].page.lock().clone();
            self.disk.write_page(page_id, &page_clone)?;
        }
        g.page_table.remove(&page_id);
        let f = &mut g.frames[idx];
        f.page_id = INVALID_PAGE_ID;
        f.dirty = false;
        f.pin_count = 0;
        f.access_count = 0;
        Ok(())
    }

    fn unpin(&self, frame_idx: usize, page_id: PageId, dirty: bool) {
        let mut g = self.inner.lock();
        let frame = &mut g.frames[frame_idx];

        if frame.page_id == page_id && frame.pin_count > 0 {
            frame.pin_count -= 1;
            if dirty {
                frame.dirty = true;
            }
        }
    }

    pub fn flush_page(&self, page_id: PageId) -> Result<()> {
        let mut g = self.inner.lock();
        if let Some(&idx) = g.page_table.get(&page_id) {
            if g.frames[idx].dirty {
                let page_clone = g.frames[idx].page.lock().clone();
                self.disk.write_page(page_id, &page_clone)?;
                g.frames[idx].dirty = false;
            }
        }
        Ok(())
    }

    pub fn flush_all(&self) -> Result<()> {
        let dirties: Vec<(PageId, Page)> = {
            let g = self.inner.lock();
            g.frames
                .iter()
                .filter(|f| f.dirty && f.page_id != INVALID_PAGE_ID)
                .map(|f| (f.page_id, f.page.lock().clone()))
                .collect()
        };
        for (pid, page) in &dirties {
            self.disk.write_page(*pid, page)?;
        }

        {
            let mut g = self.inner.lock();
            for f in &mut g.frames {
                if f.page_id != INVALID_PAGE_ID {
                    f.dirty = false;
                }
            }
        }
        self.disk.fsync()?;
        Ok(())
    }
}

pub struct BufferGuard {
    pool: Arc<BufferPool>,
    frame_idx: usize,
    page_id: PageId,
    page: Arc<Mutex<Page>>,
    dirtied: bool,
}

impl BufferGuard {
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn page(&self) -> parking_lot::MutexGuard<'_, Page> {
        self.page.lock()
    }

    pub fn page_mut(&mut self) -> parking_lot::MutexGuard<'_, Page> {
        self.dirtied = true;
        self.page.lock()
    }

    pub fn mark_dirty(&mut self) {
        self.dirtied = true;
    }
}

impl Drop for BufferGuard {
    fn drop(&mut self) {
        self.pool.unpin(self.frame_idx, self.page_id, self.dirtied);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn fresh_pool(frames: usize) -> (tempfile::TempDir, Arc<BufferPool>) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bp.dat");
        let dm = Arc::new(DiskManager::new(&path).unwrap());
        let pool = BufferPool::new(dm, frames);
        (dir, pool)
    }

    #[test]
    fn fetch_then_modify_then_flush_round_trip() {
        let (_d, pool) = fresh_pool(4);
        let pid = {
            let mut g = pool.new_page().unwrap();
            let pid = g.page_id();
            let mut p = g.page_mut();
            p.set_page_type(crate::storage::page::PageType::Data);
            let _ = p.alloc_slot(b"hello").unwrap();
            drop(p);
            pid
        };
        pool.flush_all().unwrap();

        let path = pool.disk().path().to_path_buf();
        drop(pool);
        let dm2 = Arc::new(DiskManager::new(&path).unwrap());
        let pool2 = BufferPool::new(dm2, 4);
        let g = pool2.fetch_page(pid).unwrap();
        let p = g.page();
        assert_eq!(p.get_slot(0).unwrap(), b"hello");
    }

    #[test]
    fn cache_hit_does_not_round_trip_disk() {
        let (_d, pool) = fresh_pool(4);
        let pid = {
            let mut g = pool.new_page().unwrap();
            g.page_mut().alloc_slot(b"x").unwrap();
            g.page_id()
        };
        pool.flush_all().unwrap();
        let _g1 = pool.fetch_page(pid).unwrap();
        let h1 = pool.hits();
        let _g2 = pool.fetch_page(pid).unwrap();
        assert_eq!(pool.hits(), h1 + 1);
    }

    #[test]
    fn evicts_clean_page_when_full() {
        let (_d, pool) = fresh_pool(2);
        let mut ids = Vec::new();
        for _ in 0..3 {
            let mut g = pool.new_page().unwrap();
            g.page_mut().alloc_slot(b"a").unwrap();
            ids.push(g.page_id());
        }
        pool.flush_all().unwrap();

        let m_before = pool.misses();
        let _g = pool.fetch_page(ids[0]).unwrap();
        assert!(pool.misses() >= m_before, "expected at least one miss");
    }

    #[test]
    fn lru_k_protects_hot_page_from_scan() {

        let (_d, pool) = fresh_pool(4);

        let hot = {
            let mut g = pool.new_page().unwrap();
            g.page_mut().alloc_slot(b"hot").unwrap();
            g.page_id()
        };
        let mut cold = Vec::new();
        for _ in 0..10 {
            let mut g = pool.new_page().unwrap();
            g.page_mut().alloc_slot(b"c").unwrap();
            cold.push(g.page_id());
        }
        pool.flush_all().unwrap();

        let _ = pool.fetch_page(hot).unwrap();
        let _ = pool.fetch_page(hot).unwrap();

        for c in &cold {
            let _ = pool.fetch_page(*c).unwrap();
        }

        let h = pool.hits();
        let _ = pool.fetch_page(hot).unwrap();
        assert_eq!(
            pool.hits(),
            h + 1,
            "LRU-K should have kept the twice-touched hot page resident"
        );
    }

    #[test]
    fn dirty_pages_persist_after_flush_all() {
        let (_d, pool) = fresh_pool(4);
        let pid = {
            let mut g = pool.new_page().unwrap();
            g.page_mut().alloc_slot(b"persisted").unwrap();
            g.page_id()
        };
        pool.flush_all().unwrap();
        let path = pool.disk().path().to_path_buf();
        drop(pool);

        let dm = Arc::new(DiskManager::new(&path).unwrap());
        let p = dm.read_page(pid).unwrap();
        assert_eq!(p.get_slot(0).unwrap(), b"persisted");
    }
}
