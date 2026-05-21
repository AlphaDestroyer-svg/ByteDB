use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::error::{QueryError, Result};

#[derive(Debug, Clone, Copy)]
pub struct ResourceLimits {
    pub max_memory_bytes: Option<u64>,
    pub max_temp_spill_bytes: Option<u64>,
    pub max_scan_rows: Option<u64>,
}

impl ResourceLimits {
    pub const UNLIMITED: ResourceLimits = ResourceLimits {
        max_memory_bytes: None,
        max_temp_spill_bytes: None,
        max_scan_rows: None,
    };

    pub fn with_memory(mut self, bytes: u64) -> Self {
        self.max_memory_bytes = Some(bytes);
        self
    }
    pub fn with_temp_spill(mut self, bytes: u64) -> Self {
        self.max_temp_spill_bytes = Some(bytes);
        self
    }
    pub fn with_scan_rows(mut self, rows: u64) -> Self {
        self.max_scan_rows = Some(rows);
        self
    }
}

impl Default for ResourceLimits {
    fn default() -> Self {
        ResourceLimits::UNLIMITED
    }
}

#[derive(Debug, Default, Clone)]
pub struct ResourceUsageSnapshot {
    pub memory_bytes: u64,
    pub temp_spill_bytes: u64,
    pub scan_rows: u64,
}

#[derive(Debug)]
struct ResourceCounters {
    memory_bytes: AtomicU64,
    temp_spill_bytes: AtomicU64,
    scan_rows: AtomicU64,
}

impl ResourceCounters {
    fn new() -> Self {
        ResourceCounters {
            memory_bytes: AtomicU64::new(0),
            temp_spill_bytes: AtomicU64::new(0),
            scan_rows: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> ResourceUsageSnapshot {
        ResourceUsageSnapshot {
            memory_bytes: self.memory_bytes.load(Ordering::Relaxed),
            temp_spill_bytes: self.temp_spill_bytes.load(Ordering::Relaxed),
            scan_rows: self.scan_rows.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
pub struct QueryContext {
    cancelled: AtomicBool,
    deadline: Option<Instant>,
    timeout: Option<Duration>,
    limits: ResourceLimits,
    counters: ResourceCounters,
}

impl QueryContext {
    pub fn new() -> Arc<Self> {
        Arc::new(QueryContext {
            cancelled: AtomicBool::new(false),
            deadline: None,
            timeout: None,
            limits: ResourceLimits::UNLIMITED,
            counters: ResourceCounters::new(),
        })
    }

    pub fn with_timeout(timeout: Duration) -> Arc<Self> {
        Arc::new(QueryContext {
            cancelled: AtomicBool::new(false),
            deadline: Some(Instant::now() + timeout),
            timeout: Some(timeout),
            limits: ResourceLimits::UNLIMITED,
            counters: ResourceCounters::new(),
        })
    }

    pub fn with_limits(limits: ResourceLimits) -> Arc<Self> {
        Arc::new(QueryContext {
            cancelled: AtomicBool::new(false),
            deadline: None,
            timeout: None,
            limits,
            counters: ResourceCounters::new(),
        })
    }

    pub fn builder() -> QueryContextBuilder {
        QueryContextBuilder::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn limits(&self) -> ResourceLimits {
        self.limits
    }

    pub fn usage(&self) -> ResourceUsageSnapshot {
        self.counters.snapshot()
    }

    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            return Err(QueryError::Cancelled);
        }
        if let Some(dl) = self.deadline {
            if Instant::now() >= dl {
                let ms = self.timeout.map(|d| d.as_millis() as u64).unwrap_or(0);
                return Err(QueryError::QueryTimeout(ms));
            }
        }
        Ok(())
    }

    pub fn account_scan_row(&self) -> Result<()> {
        let n = self.counters.scan_rows.fetch_add(1, Ordering::Relaxed) + 1;
        if let Some(max) = self.limits.max_scan_rows {
            if n > max {
                return Err(QueryError::ResourceLimit(format!(
                    "max_scan_rows exceeded: {} > {}",
                    n, max
                )));
            }
        }
        Ok(())
    }

    pub fn account_scan_rows(&self, rows: u64) -> Result<()> {
        let n = self.counters.scan_rows.fetch_add(rows, Ordering::Relaxed) + rows;
        if let Some(max) = self.limits.max_scan_rows {
            if n > max {
                return Err(QueryError::ResourceLimit(format!(
                    "max_scan_rows exceeded: {} > {}",
                    n, max
                )));
            }
        }
        Ok(())
    }

    pub fn account_memory(&self, bytes: u64) -> Result<()> {
        let n = self.counters.memory_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if let Some(max) = self.limits.max_memory_bytes {
            if n > max {
                return Err(QueryError::ResourceLimit(format!(
                    "max_memory_bytes exceeded: {} > {}",
                    n, max
                )));
            }
        }
        Ok(())
    }

    pub fn release_memory(&self, bytes: u64) {
        let cur = self.counters.memory_bytes.load(Ordering::Relaxed);
        let new = cur.saturating_sub(bytes);
        self.counters.memory_bytes.store(new, Ordering::Relaxed);
    }

    pub fn account_temp_spill(&self, bytes: u64) -> Result<()> {
        let n = self.counters.temp_spill_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if let Some(max) = self.limits.max_temp_spill_bytes {
            if n > max {
                return Err(QueryError::ResourceLimit(format!(
                    "max_temp_spill_bytes exceeded: {} > {}",
                    n, max
                )));
            }
        }
        Ok(())
    }

    pub fn check_every(&self, counter: &mut u64, period: u64) -> Result<()> {
        *counter = counter.wrapping_add(1);
        if *counter % period.max(1) == 0 {
            self.check()?;
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct QueryContextBuilder {
    timeout: Option<Duration>,
    limits: ResourceLimits,
}

impl QueryContextBuilder {
    pub fn timeout(mut self, t: Duration) -> Self {
        self.timeout = Some(t);
        self
    }
    pub fn max_memory_bytes(mut self, bytes: u64) -> Self {
        self.limits.max_memory_bytes = Some(bytes);
        self
    }
    pub fn max_temp_spill_bytes(mut self, bytes: u64) -> Self {
        self.limits.max_temp_spill_bytes = Some(bytes);
        self
    }
    pub fn max_scan_rows(mut self, rows: u64) -> Self {
        self.limits.max_scan_rows = Some(rows);
        self
    }
    pub fn limits(mut self, l: ResourceLimits) -> Self {
        self.limits = l;
        self
    }
    pub fn build(self) -> Arc<QueryContext> {
        let now = Instant::now();
        Arc::new(QueryContext {
            cancelled: AtomicBool::new(false),
            deadline: self.timeout.map(|d| now + d),
            timeout: self.timeout,
            limits: self.limits,
            counters: ResourceCounters::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_flips_check() {
        let ctx = QueryContext::new();
        assert!(ctx.check().is_ok());
        ctx.cancel();
        assert!(matches!(ctx.check(), Err(QueryError::Cancelled)));
    }

    #[test]
    fn deadline_fires() {
        let ctx = QueryContext::with_timeout(Duration::from_millis(20));
        std::thread::sleep(Duration::from_millis(40));
        match ctx.check() {
            Err(QueryError::QueryTimeout(_)) => {}
            other => panic!("expected QueryTimeout, got {:?}", other),
        }
    }

    #[test]
    fn scan_rows_limit() {
        let ctx = QueryContext::builder().max_scan_rows(3).build();
        assert!(ctx.account_scan_row().is_ok());
        assert!(ctx.account_scan_row().is_ok());
        assert!(ctx.account_scan_row().is_ok());
        assert!(matches!(ctx.account_scan_row(), Err(QueryError::ResourceLimit(_))));
        assert_eq!(ctx.usage().scan_rows, 4);
    }

    #[test]
    fn memory_limit() {
        let ctx = QueryContext::builder().max_memory_bytes(100).build();
        assert!(ctx.account_memory(40).is_ok());
        assert!(ctx.account_memory(40).is_ok());
        assert!(matches!(ctx.account_memory(40), Err(QueryError::ResourceLimit(_))));
    }

    #[test]
    fn release_memory_lowers_usage() {
        let ctx = QueryContext::builder().max_memory_bytes(100).build();
        ctx.account_memory(80).unwrap();
        ctx.release_memory(50);
        assert_eq!(ctx.usage().memory_bytes, 30);
        assert!(ctx.account_memory(60).is_ok());
    }

    #[test]
    fn temp_spill_limit() {
        let ctx = QueryContext::builder().max_temp_spill_bytes(50).build();
        assert!(ctx.account_temp_spill(30).is_ok());
        assert!(matches!(ctx.account_temp_spill(30), Err(QueryError::ResourceLimit(_))));
    }

    #[test]
    fn check_every_periodic() {
        let ctx = QueryContext::new();
        let mut c = 0u64;
        for _ in 0..100 {
            ctx.check_every(&mut c, 16).unwrap();
        }
        ctx.cancel();
        let mut hit = false;
        for _ in 0..100 {
            if ctx.check_every(&mut c, 16).is_err() {
                hit = true;
                break;
            }
        }
        assert!(hit);
    }
}
