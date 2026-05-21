use std::sync::Arc;
use std::time::Duration;

use crate::wal::log_manager::LogManager;
use super::{spawn_periodic, WorkerHandle};

pub struct WalFlusherConfig {
    pub period: Duration,
}

impl Default for WalFlusherConfig {
    fn default() -> Self {
        Self { period: Duration::from_millis(200) }
    }
}

pub fn start(wal: Arc<LogManager>, cfg: WalFlusherConfig) -> WorkerHandle {
    spawn_periodic("wal-flusher", cfg.period, move || {
        if let Err(e) = wal.flush() {
            eprintln!("wal-flusher: flush failed: {}", e);
        }
    })
}
