//! Periodically flushes dirty pages from the buffer pool to disk.
//!
//! Bounds the maximum amount of dirty data the database can lose on a
//! crash to roughly `period` worth of writes (after WAL replay covers
//! the rest). Independent from the WAL flusher: the WAL is the
//! durability source, the page flusher only reduces recovery time.

use std::sync::Arc;
use std::time::Duration;

use crate::storage::buffer_pool::BufferPool;
use super::{spawn_periodic, WorkerHandle};

pub struct PageFlusherConfig {
    pub period: Duration,
}

impl Default for PageFlusherConfig {
    fn default() -> Self {
        Self { period: Duration::from_secs(5) }
    }
}

pub fn start(pool: Arc<BufferPool>, cfg: PageFlusherConfig) -> WorkerHandle {
    spawn_periodic("page-flusher", cfg.period, move || {
        // Errors during background flush are logged-and-continue; the
        // next tick or the synchronous checkpoint will retry.
        if let Err(e) = pool.flush_all() {
            eprintln!("page-flusher: flush_all failed: {}", e);
        }
    })
}
