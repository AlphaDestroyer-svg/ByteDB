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

        if let Err(e) = pool.flush_all() {
            eprintln!("page-flusher: flush_all failed: {}", e);
        }
    })
}
