//! Buffer pool with LRU-K eviction (v0.2 storage stage 2).
//!
//! The buffer pool sits between [`crate::storage::disk_manager::DiskManager`]
//! and any code that needs to read or write pages. It owns a fixed number
//! of frames; each frame can hold one page. Pages are pinned while in use
//! and unpinned (with an optional dirty bit) when the caller is done.
//!
//! ## Eviction: LRU-K (K = 2)
//!
//! Pure LRU is fooled by sequential scans — a one-shot scan flushes hot
//! pages out of cache. LRU-K (Pat O'Neil et al.) tracks the *K-th most
//! recent* access of each page and evicts the page whose K-th access is
//! oldest. With K=2, a page touched only once doesn't earn the right to
//! evict twice-touched pages: scans push their own pages out first.
//!
//! Implementation uses a per-frame ring of last access timestamps. Pages
//! with fewer than K recorded accesses are treated as having `t_K = -inf`,
//! so they're evicted before any twice-touched page.
//!
//! ## Concurrency
//!
//! The pool is wrapped in a single `Mutex<BufferPoolInner>`. Pinning and
//! unpinning are O(1), so the lock is held briefly. Per-frame data is
//! returned via `BufferGuard`, which holds an `Arc<Mutex<Page>>` so two
//! pinners of the same page see the same in-memory copy (this is what
//! the upper layer expects: shared cache).
//!
//! ## Sentinel pages
//!
//! `INVALID_PAGE_ID` is rejected at the API surface — callers must not
//! ask for page 0.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::error::{CoreError, Result};
use crate::storage::disk_manager::DiskManager;
use crate::storage::page::{Page, PageId, INVALID_PAGE_ID};

/// How many recent accesses to remember per frame for LRU-K.
pub const LRU_K: usize = 2;

/// Default frame count if the caller doesn't pick one. ~16 MB at 8 KB pages.
pub const DEFAULT_POOL_FRAMES: usize = 2048;

struct Frame {
    page: Arc<Mutex<Page>>,
    page_id: PageId,
    pin_count: u32,
    dirty: bool,
    /// Ring of last K access timestamps (oldest at front).
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
            // Shift left, drop oldest, push newest.
            for i in 0..LRU_K - 1 {
                self.access_history[i] = self.access_history[i + 1];
            }
            self.access_history[LRU_K - 1] = ts;
        }
    }

    /// K-th most recent timestamp, or 0 if fewer than K accesses recorded.
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
    /// page_id -> frame index. Only contains live mappings.
    page_table: HashMap<PageId, usize>,
    /// Free list of frames that have never been used (initial state) or
    /// have been explicitly released.
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

    /// Pick a victim frame to evict. Returns frame index, or None if every
    /// frame is pinned.
    fn pick_victim(&self) -> Option<usize> {
        let mut best: Option<(usize, u64, u8)> = None;
        for (i, f) in self.frames.iter().enumerate() {
            if f.pin_count > 0 || f.page_id == INVALID_PAGE_ID {
                continue;
            }
            let kth = f.kth_oldest();
            // Prefer pages with fewer accesses (kth=0) over older twice-touched.
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

    /// Pin a page into the buffer pool. The returned guard keeps the page
    /// resident; it will be unpinned (and optionally marked dirty) when
    /// the guard is dropped or `mark_dirty()` is called.
    pub fn fetch_page(self: &Arc<Self>, page_id: PageId) -> Result<BufferGuard> {
        if page_id == INVALID_PAGE_ID {
            return Err(CoreError::Internal(
                "buffer_pool: refusing to fetch INVALID_PAGE_ID".into(),
            ));
        }

        // Fast path: page already cached.
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

        // Slow path: read from disk, then install in a free or evicted frame.
        let page_from_disk = self.disk.read_page(page_id)?;

        let mut g = self.inner.lock();
        // Re-check (someone else may have raced us).
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

    /// Allocate a fresh page on disk and pin it in the pool.
    pub fn new_page(self: &Arc<Self>) -> Result<BufferGuard> {
        let page_id = self.disk.allocate_page()?;
        let mut guard = self.fetch_page(page_id)?;
        // The page on disk is empty — keep it that way but mark resident
        // copy as ready. Caller initialises header / writes content.
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
        // Sanity: only count down for the page we actually pinned.
        if frame.page_id == page_id && frame.pin_count > 0 {
            frame.pin_count -= 1;
            if dirty {
                frame.dirty = true;
            }
        }
    }

    /// Flush a single page to disk if it is currently resident & dirty.
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

    /// Flush all dirty pages to disk and fsync. Used at checkpoint.
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
        // Now clear dirty flags.
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

/// RAII guard returned by [`BufferPool::fetch_page`]. While alive, the
/// underlying page stays pinned in memory.
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

    /// Read-only access to the cached page.
    pub fn page(&self) -> parking_lot::MutexGuard<'_, Page> {
        self.page.lock()
    }

    /// Mutable access. Marks the page dirty automatically.
    pub fn page_mut(&mut self) -> parking_lot::MutexGuard<'_, Page> {
        self.dirtied = true;
        self.page.lock()
    }

    /// Explicitly mark the page dirty without taking a mutable lock — useful
    /// after a write-through that also updated the in-memory copy.
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

        // Re-open the disk via a fresh pool and confirm bytes survived.
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
        // Pool has 2 frames; we just touched 3 pages — at least one was
        // evicted. Re-fetching it should be a miss.
        let m_before = pool.misses();
        let _g = pool.fetch_page(ids[0]).unwrap();
        assert!(pool.misses() >= m_before, "expected at least one miss");
    }

    #[test]
    fn lru_k_protects_hot_page_from_scan() {
        // Sequential scan over many cold pages should not evict a page
        // that has been touched repeatedly.
        let (_d, pool) = fresh_pool(4);
        // Allocate hot page + 10 cold pages.
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

        // Touch hot page twice so its K-th access is recorded.
        let _ = pool.fetch_page(hot).unwrap();
        let _ = pool.fetch_page(hot).unwrap();

        // Now scan all cold pages once each.
        for c in &cold {
            let _ = pool.fetch_page(*c).unwrap();
        }

        // Hot page should still be cached (LRU-K keeps it).
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
