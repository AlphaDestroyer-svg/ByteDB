//! Background vacuum / MVCC garbage collector.
//!
//! Stage 4 just stands up the worker scaffolding with a pluggable
//! callback; the real GC pass that prunes versions older than
//! `oldest_active_xid` lands in stage 5.

use std::sync::Arc;
use std::time::Duration;

use super::{spawn_periodic, WorkerHandle};

pub struct VacuumConfig {
    pub period: Duration,
}

impl Default for VacuumConfig {
    fn default() -> Self {
        Self { period: Duration::from_secs(60) }
    }
}

pub trait VacuumPass: Send + Sync + 'static {
    fn run(&self);
}

pub fn start(pass: Arc<dyn VacuumPass>, cfg: VacuumConfig) -> WorkerHandle {
    spawn_periodic("vacuum", cfg.period, move || {
        pass.run();
    })
}
