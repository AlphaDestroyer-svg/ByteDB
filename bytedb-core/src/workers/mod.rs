use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

pub mod page_flusher;
pub mod wal_flusher;
pub mod checkpoint;
pub mod vacuum;

pub struct ShutdownLatch {
    flag: AtomicBool,
    cv: Condvar,
    lock: Mutex<()>,
}

impl ShutdownLatch {
    pub fn new() -> Arc<Self> {
        Arc::new(ShutdownLatch {
            flag: AtomicBool::new(false),
            cv: Condvar::new(),
            lock: Mutex::new(()),
        })
    }

    pub fn signal(&self) {
        self.flag.store(true, Ordering::Release);
        let _g = self.lock.lock();
        self.cv.notify_all();
    }

    pub fn is_shutdown(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    pub fn wait_until(&self, dur: Duration) -> bool {
        let mut g = self.lock.lock();
        if self.flag.load(Ordering::Acquire) {
            return true;
        }
        let _ = self.cv.wait_for(&mut g, dur);
        self.flag.load(Ordering::Acquire)
    }
}

impl Default for ShutdownLatch {
    fn default() -> Self {
        Self {
            flag: AtomicBool::new(false),
            cv: Condvar::new(),
            lock: Mutex::new(()),
        }
    }
}

pub struct WorkerHandle {
    name: String,
    latch: Arc<ShutdownLatch>,
    handle: Option<JoinHandle<()>>,
}

impl WorkerHandle {
    pub fn name(&self) -> &str { &self.name }

    pub fn shutdown(&mut self) {
        self.latch.signal();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }

    pub fn is_running(&self) -> bool {
        self.handle
            .as_ref()
            .map(|h| !h.is_finished())
            .unwrap_or(false)
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub fn spawn_periodic<F>(
    name: impl Into<String>,
    period: Duration,
    mut tick: F,
) -> WorkerHandle
where
    F: FnMut() + Send + 'static,
{
    let name = name.into();
    let latch = ShutdownLatch::new();
    let latch_clone = Arc::clone(&latch);
    let thread_name = name.clone();
    let handle = thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let mut next_tick = Instant::now() + period;
            while !latch_clone.is_shutdown() {
                let now = Instant::now();
                let wait = next_tick.saturating_duration_since(now);
                if wait.is_zero() {
                    tick();
                    next_tick = Instant::now() + period;
                } else if latch_clone.wait_until(wait) {
                    break;
                } else {
                    tick();
                    next_tick = Instant::now() + period;
                }
            }
        })
        .expect("spawn worker thread");

    WorkerHandle {
        name,
        latch,
        handle: Some(handle),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn periodic_worker_ticks_and_stops_on_shutdown() {
        let counter = Arc::new(AtomicU64::new(0));
        let c2 = Arc::clone(&counter);
        let mut h = spawn_periodic(
            "test-worker",
            Duration::from_millis(10),
            move || {
                c2.fetch_add(1, Ordering::Relaxed);
            },
        );
        thread::sleep(Duration::from_millis(80));
        h.shutdown();
        let n = counter.load(Ordering::Relaxed);
        assert!(n >= 3, "worker should have ticked at least a few times, got {}", n);
    }

    #[test]
    fn shutdown_returns_promptly() {
        let mut h = spawn_periodic(
            "slow-tick",
            Duration::from_secs(60),
            || {},
        );
        let start = Instant::now();
        h.shutdown();
        assert!(start.elapsed() < Duration::from_secs(1));
    }
}
