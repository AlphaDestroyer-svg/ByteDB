//! Checkpoint thread.
//!
//! At each tick:
//!   1. Flushes the buffer pool so all data pages catch up.
//!   2. Calls `LogManager::flush` so the WAL is durable.
//!   3. Writes a CHECKPOINT log record (when supported by callers).
//!
//! Truncating the WAL past the checkpoint is the caller's responsibility
//! — it depends on still-active transactions, which live above
//! `bytedb-core`. We just publish the durable point.

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
