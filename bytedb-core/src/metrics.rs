use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use parking_lot::Mutex;

pub const DEFAULT_LATENCY_WINDOW: usize = 4096;

#[derive(Debug)]
pub struct LatencyHistogram {
    samples: Mutex<Vec<u64>>,
    cursor: AtomicU64,
    capacity: usize,
    total_count: AtomicU64,
    total_micros: AtomicU64,
    window_start: Mutex<Instant>,
}

impl LatencyHistogram {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(8);
        LatencyHistogram {
            samples: Mutex::new(Vec::with_capacity(cap)),
            cursor: AtomicU64::new(0),
            capacity: cap,
            total_count: AtomicU64::new(0),
            total_micros: AtomicU64::new(0),
            window_start: Mutex::new(Instant::now()),
        }
    }

    pub fn record_micros(&self, micros: u64) {
        self.total_count.fetch_add(1, Ordering::Relaxed);
        self.total_micros.fetch_add(micros, Ordering::Relaxed);
        let mut s = self.samples.lock();
        if s.len() < self.capacity {
            s.push(micros);
        } else {
            let idx = (self.cursor.fetch_add(1, Ordering::Relaxed) as usize) % self.capacity;
            s[idx] = micros;
        }
    }

    pub fn record(&self, d: Duration) {
        self.record_micros(d.as_micros() as u64);
    }

    pub fn total_count(&self) -> u64 {
        self.total_count.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> LatencySnapshot {
        let s = self.samples.lock();
        let mut sorted: Vec<u64> = s.clone();
        drop(s);
        sorted.sort_unstable();
        let n = sorted.len();
        let pick = |q: f64| -> u64 {
            if n == 0 { return 0; }
            let idx = ((n as f64 - 1.0) * q).round() as usize;
            sorted[idx.min(n - 1)]
        };
        let total = self.total_count.load(Ordering::Relaxed);
        let total_us = self.total_micros.load(Ordering::Relaxed);
        let mean = if total > 0 { total_us / total } else { 0 };
        LatencySnapshot {
            samples: n as u64,
            total_count: total,
            mean_micros: mean,
            p50_micros: pick(0.50),
            p95_micros: pick(0.95),
            p99_micros: pick(0.99),
            max_micros: sorted.last().copied().unwrap_or(0),
        }
    }

    pub fn qps(&self) -> f64 {
        let count = self.total_count.load(Ordering::Relaxed);
        let start = *self.window_start.lock();
        let elapsed = start.elapsed().as_secs_f64();
        if elapsed <= 0.0 { 0.0 } else { count as f64 / elapsed }
    }

    pub fn reset_window(&self) {
        *self.window_start.lock() = Instant::now();
        self.total_count.store(0, Ordering::Relaxed);
        self.total_micros.store(0, Ordering::Relaxed);
        self.samples.lock().clear();
        self.cursor.store(0, Ordering::Relaxed);
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        LatencyHistogram::new(DEFAULT_LATENCY_WINDOW)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LatencySnapshot {
    pub samples: u64,
    pub total_count: u64,
    pub mean_micros: u64,
    pub p50_micros: u64,
    pub p95_micros: u64,
    pub p99_micros: u64,
    pub max_micros: u64,
}

#[derive(Debug, Default)]
pub struct GcMetrics {
    pub runs: AtomicU64,
    pub versions_removed: AtomicU64,
    pub keys_removed: AtomicU64,
    pub total_pause_micros: AtomicU64,
    pub last_pause_micros: AtomicU64,
}

impl GcMetrics {
    pub fn new() -> Self { Self::default() }

    pub fn record_run(&self, pause: Duration, versions: u64, keys: u64) {
        self.runs.fetch_add(1, Ordering::Relaxed);
        let us = pause.as_micros() as u64;
        self.total_pause_micros.fetch_add(us, Ordering::Relaxed);
        self.last_pause_micros.store(us, Ordering::Relaxed);
        self.versions_removed.fetch_add(versions, Ordering::Relaxed);
        self.keys_removed.fetch_add(keys, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> GcSnapshot {
        GcSnapshot {
            runs: self.runs.load(Ordering::Relaxed),
            versions_removed: self.versions_removed.load(Ordering::Relaxed),
            keys_removed: self.keys_removed.load(Ordering::Relaxed),
            total_pause_micros: self.total_pause_micros.load(Ordering::Relaxed),
            last_pause_micros: self.last_pause_micros.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GcSnapshot {
    pub runs: u64,
    pub versions_removed: u64,
    pub keys_removed: u64,
    pub total_pause_micros: u64,
    pub last_pause_micros: u64,
}

#[derive(Debug, Default)]
pub struct DeadTupleMetrics {
    pub live_versions: AtomicU64,
    pub dead_versions: AtomicU64,
}

impl DeadTupleMetrics {
    pub fn record(&self, live: u64, dead: u64) {
        self.live_versions.store(live, Ordering::Relaxed);
        self.dead_versions.store(dead, Ordering::Relaxed);
    }

    pub fn ratio(&self) -> f64 {
        let live = self.live_versions.load(Ordering::Relaxed);
        let dead = self.dead_versions.load(Ordering::Relaxed);
        let total = live + dead;
        if total == 0 { 0.0 } else { dead as f64 / total as f64 }
    }

    pub fn snapshot(&self) -> DeadTupleSnapshot {
        DeadTupleSnapshot {
            live_versions: self.live_versions.load(Ordering::Relaxed),
            dead_versions: self.dead_versions.load(Ordering::Relaxed),
            ratio: self.ratio(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DeadTupleSnapshot {
    pub live_versions: u64,
    pub dead_versions: u64,
    pub ratio: f64,
}

pub struct Timer {
    start: Instant,
}

impl Timer {
    pub fn start() -> Self { Timer { start: Instant::now() } }
    pub fn elapsed(&self) -> Duration { self.start.elapsed() }
    pub fn elapsed_micros(&self) -> u64 { self.start.elapsed().as_micros() as u64 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_percentiles() {
        let h = LatencyHistogram::new(1024);
        for i in 1..=100u64 {
            h.record_micros(i);
        }
        let snap = h.snapshot();
        assert_eq!(snap.total_count, 100);
        assert_eq!(snap.samples, 100);
        assert!(snap.p50_micros >= 50 && snap.p50_micros <= 51);
        assert!(snap.p95_micros >= 95 && snap.p95_micros <= 96);
        assert!(snap.p99_micros >= 99 && snap.p99_micros <= 100);
        assert_eq!(snap.max_micros, 100);
    }

    #[test]
    fn histogram_ring_buffer_evicts() {
        let h = LatencyHistogram::new(16);
        for i in 0..1000u64 {
            h.record_micros(i);
        }
        let snap = h.snapshot();
        assert_eq!(snap.samples, 16);
        assert_eq!(snap.total_count, 1000);
    }

    #[test]
    fn gc_metrics_record() {
        let g = GcMetrics::new();
        g.record_run(Duration::from_micros(120), 5, 2);
        g.record_run(Duration::from_micros(80), 3, 1);
        let s = g.snapshot();
        assert_eq!(s.runs, 2);
        assert_eq!(s.versions_removed, 8);
        assert_eq!(s.last_pause_micros, 80);
        assert_eq!(s.total_pause_micros, 200);
    }

    #[test]
    fn dead_tuple_ratio() {
        let d = DeadTupleMetrics::default();
        d.record(70, 30);
        let s = d.snapshot();
        assert!((s.ratio - 0.30).abs() < 1e-9);
    }
}
