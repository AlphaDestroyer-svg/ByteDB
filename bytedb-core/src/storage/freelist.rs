use std::collections::VecDeque;
use parking_lot::Mutex;

use crate::storage::page::PageId;

pub struct FreeList {
    free_pages: Mutex<VecDeque<PageId>>,
}

impl FreeList {
    pub fn new() -> Self {
        FreeList {
            free_pages: Mutex::new(VecDeque::new()),
        }
    }

    pub fn push(&self, page_id: PageId) {
        self.free_pages.lock().push_back(page_id);
    }

    pub fn pop(&self) -> Option<PageId> {
        self.free_pages.lock().pop_front()
    }

    pub fn len(&self) -> usize {
        self.free_pages.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.free_pages.lock().is_empty()
    }
}

impl Default for FreeList {
    fn default() -> Self {
        Self::new()
    }
}
