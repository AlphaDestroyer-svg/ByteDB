use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use parking_lot::RwLock;

use crate::storage::page::{Page, PageId};

pub struct BufferFrame {
    pub page: RwLock<Page>,
    pub page_id: AtomicU32,
    pub pin_count: AtomicU32,
    pub is_dirty: AtomicBool,
}

impl BufferFrame {
    pub fn new() -> Self {
        BufferFrame {
            page: RwLock::new(Page::new(0)),
            page_id: AtomicU32::new(0),
            pin_count: AtomicU32::new(0),
            is_dirty: AtomicBool::new(false),
        }
    }

    pub fn reset(&self, page: Page) {
        let page_id = page.id;
        *self.page.write() = page;
        self.page_id.store(page_id, Ordering::SeqCst);
        self.pin_count.store(1, Ordering::SeqCst);
        self.is_dirty.store(false, Ordering::SeqCst);
    }

    pub fn pin(&self) {
        self.pin_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn unpin(&self) {
        self.pin_count.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn is_pinned(&self) -> bool {
        self.pin_count.load(Ordering::SeqCst) > 0
    }

    pub fn mark_dirty(&self) {
        self.is_dirty.store(true, Ordering::SeqCst);
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id.load(Ordering::SeqCst)
    }
}
