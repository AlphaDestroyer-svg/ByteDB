use std::collections::HashMap;
use parking_lot::RwLock;

use crate::storage::page::PageId;

const NUM_BUCKETS: usize = 16;

pub struct FreeSpaceMap {
    page_size: usize,
    pages: RwLock<HashMap<PageId, usize>>,
    buckets: RwLock<Vec<Vec<PageId>>>,
}

impl FreeSpaceMap {
    pub fn new(page_size: usize) -> Self {
        let mut buckets = Vec::with_capacity(NUM_BUCKETS);
        for _ in 0..NUM_BUCKETS {
            buckets.push(Vec::new());
        }
        FreeSpaceMap {
            page_size,
            pages: RwLock::new(HashMap::new()),
            buckets: RwLock::new(buckets),
        }
    }

    fn bucket_index(&self, free_bytes: usize) -> usize {
        if self.page_size == 0 { return 0; }
        let cap = self.page_size;
        let pct = (free_bytes * NUM_BUCKETS) / cap.max(1);
        pct.min(NUM_BUCKETS - 1)
    }

    pub fn record(&self, page_id: PageId, free_bytes: usize) {
        let new_idx = self.bucket_index(free_bytes);
        let mut pages = self.pages.write();
        let mut buckets = self.buckets.write();
        if let Some(old_free) = pages.insert(page_id, free_bytes) {
            let old_idx = self.bucket_index(old_free);
            if old_idx != new_idx {
                if let Some(pos) = buckets[old_idx].iter().position(|p| *p == page_id) {
                    buckets[old_idx].swap_remove(pos);
                }
                buckets[new_idx].push(page_id);
            }
        } else {
            buckets[new_idx].push(page_id);
        }
    }

    pub fn forget(&self, page_id: PageId) {
        let mut pages = self.pages.write();
        let mut buckets = self.buckets.write();
        if let Some(free) = pages.remove(&page_id) {
            let idx = self.bucket_index(free);
            if let Some(pos) = buckets[idx].iter().position(|p| *p == page_id) {
                buckets[idx].swap_remove(pos);
            }
        }
    }

    pub fn find_with_at_least(&self, needed_bytes: usize) -> Option<PageId> {
        let pages = self.pages.read();
        let buckets = self.buckets.read();
        let start = self.bucket_index(needed_bytes);
        for idx in start..NUM_BUCKETS {
            for pid in buckets[idx].iter().rev() {
                if let Some(free) = pages.get(pid) {
                    if *free >= needed_bytes {
                        return Some(*pid);
                    }
                }
            }
        }
        None
    }

    pub fn page_count(&self) -> usize {
        self.pages.read().len()
    }

    pub fn free_bytes_for(&self, page_id: PageId) -> Option<usize> {
        self.pages.read().get(&page_id).copied()
    }

    pub fn total_free_bytes(&self) -> u64 {
        self.pages.read().values().map(|v| *v as u64).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fsm_records_and_finds() {
        let fsm = FreeSpaceMap::new(8192);
        fsm.record(1, 100);
        fsm.record(2, 4000);
        fsm.record(3, 7000);
        let p = fsm.find_with_at_least(500).unwrap();
        assert!(p == 2 || p == 3);
        let p2 = fsm.find_with_at_least(5000).unwrap();
        assert_eq!(p2, 3);
        assert!(fsm.find_with_at_least(8000).is_none());
    }

    #[test]
    fn fsm_updates_bucket_on_change() {
        let fsm = FreeSpaceMap::new(8192);
        fsm.record(7, 100);
        assert!(fsm.find_with_at_least(2000).is_none());
        fsm.record(7, 5000);
        assert_eq!(fsm.find_with_at_least(2000), Some(7));
        assert_eq!(fsm.page_count(), 1);
    }

    #[test]
    fn fsm_forget_removes_page() {
        let fsm = FreeSpaceMap::new(8192);
        fsm.record(9, 6000);
        assert_eq!(fsm.find_with_at_least(1000), Some(9));
        fsm.forget(9);
        assert!(fsm.find_with_at_least(1000).is_none());
        assert_eq!(fsm.page_count(), 0);
    }

    #[test]
    fn fsm_total_free_bytes() {
        let fsm = FreeSpaceMap::new(8192);
        fsm.record(1, 1000);
        fsm.record(2, 2000);
        fsm.record(3, 3000);
        assert_eq!(fsm.total_free_bytes(), 6000);
    }
}
