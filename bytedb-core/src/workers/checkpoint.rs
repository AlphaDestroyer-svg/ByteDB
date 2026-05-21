use std::sync::Arc;
use std::time::Duration;

use crate::storage::buffer_pool::BufferPool;
use crate::wal::log_manager::LogManager;
use super::{spawn_periodic, WorkerHandle};

pub struct CheckpointConfig {
    pub period: Duration,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self { period: Duration::from_secs(30) }
    }
}

pub fn start(
    pool: Arc<BufferPool>,
    wal: Arc<LogManager>,
    cfg: CheckpointConfig,
) -> WorkerHandle {
    spawn_periodic("checkpoint", cfg.period, move || {
        if let Err(e) = pool.flush_all() {
            eprintln!("checkpoint: flush_all failed: {}", e);
            return;
        }
        if let Err(e) = wal.flush() {
            eprintln!("checkpoint: wal flush failed: {}", e);
        }
    })
}
