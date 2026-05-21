use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

static ENABLED: AtomicBool = AtomicBool::new(false);
static SEED: AtomicU32 = AtomicU32::new(0xC0FFEE);
static WAL_FLUSH_DELAY_PCT: AtomicU32 = AtomicU32::new(0);
static WAL_FLUSH_DELAY_MAX_US: AtomicU32 = AtomicU32::new(0);
static SPLIT_DELAY_PCT: AtomicU32 = AtomicU32::new(0);
static SPLIT_DELAY_MAX_US: AtomicU32 = AtomicU32::new(0);
static INDEX_TRAVERSAL_DELAY_PCT: AtomicU32 = AtomicU32::new(0);
static INDEX_TRAVERSAL_DELAY_MAX_US: AtomicU32 = AtomicU32::new(0);
static SKIP_FSYNC_PCT: AtomicU32 = AtomicU32::new(0);
static PARTIAL_WRITE_PCT: AtomicU32 = AtomicU32::new(0);

pub fn enable() {
    ENABLED.store(true, Ordering::Relaxed);
}

pub fn disable() {
    ENABLED.store(false, Ordering::Relaxed);
}

pub fn is_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

pub fn configure_from_env() {
    if std::env::var("CHAOS").ok().as_deref() != Some("1") {
        return;
    }
    enable();
    if let Some(v) = env_u32("CHAOS_SEED") { SEED.store(v, Ordering::Relaxed); }
    if let Some(v) = env_u32("CHAOS_WAL_FLUSH_PCT") { WAL_FLUSH_DELAY_PCT.store(v, Ordering::Relaxed); }
    if let Some(v) = env_u32("CHAOS_WAL_FLUSH_MAX_US") { WAL_FLUSH_DELAY_MAX_US.store(v, Ordering::Relaxed); }
    if let Some(v) = env_u32("CHAOS_SPLIT_PCT") { SPLIT_DELAY_PCT.store(v, Ordering::Relaxed); }
    if let Some(v) = env_u32("CHAOS_SPLIT_MAX_US") { SPLIT_DELAY_MAX_US.store(v, Ordering::Relaxed); }
    if let Some(v) = env_u32("CHAOS_INDEX_PCT") { INDEX_TRAVERSAL_DELAY_PCT.store(v, Ordering::Relaxed); }
    if let Some(v) = env_u32("CHAOS_INDEX_MAX_US") { INDEX_TRAVERSAL_DELAY_MAX_US.store(v, Ordering::Relaxed); }
    if let Some(v) = env_u32("CHAOS_SKIP_FSYNC_PCT") { SKIP_FSYNC_PCT.store(v, Ordering::Relaxed); }
    if let Some(v) = env_u32("CHAOS_PARTIAL_WRITE_PCT") { PARTIAL_WRITE_PCT.store(v, Ordering::Relaxed); }
    eprintln!(
        "[chaos] enabled: wal_flush={}%/{}us split={}%/{}us index={}%/{}us skip_fsync={}% partial_write={}%",
        WAL_FLUSH_DELAY_PCT.load(Ordering::Relaxed),
        WAL_FLUSH_DELAY_MAX_US.load(Ordering::Relaxed),
        SPLIT_DELAY_PCT.load(Ordering::Relaxed),
        SPLIT_DELAY_MAX_US.load(Ordering::Relaxed),
        INDEX_TRAVERSAL_DELAY_PCT.load(Ordering::Relaxed),
        INDEX_TRAVERSAL_DELAY_MAX_US.load(Ordering::Relaxed),
        SKIP_FSYNC_PCT.load(Ordering::Relaxed),
        PARTIAL_WRITE_PCT.load(Ordering::Relaxed),
    );
}

fn env_u32(key: &str) -> Option<u32> {
    std::env::var(key).ok().and_then(|s| s.parse().ok())
}

fn next_rand() -> u32 {
    let mut s = SEED.load(Ordering::Relaxed);
    s ^= s << 13;
    s ^= s >> 17;
    s ^= s << 5;
    if s == 0 { s = 0x1; }
    SEED.store(s, Ordering::Relaxed);
    s
}

fn maybe_sleep(pct: u32, max_us: u32) {
    if pct == 0 || max_us == 0 { return; }
    let r = next_rand();
    if (r % 100) >= pct { return; }
    let dur = (r % max_us) as u64;
    if dur > 0 {
        std::thread::sleep(Duration::from_micros(dur));
    }
}

pub fn wal_flush_hook() {
    if !is_enabled() { return; }
    maybe_sleep(
        WAL_FLUSH_DELAY_PCT.load(Ordering::Relaxed),
        WAL_FLUSH_DELAY_MAX_US.load(Ordering::Relaxed),
    );
}

pub fn split_hook() {
    if !is_enabled() { return; }
    maybe_sleep(
        SPLIT_DELAY_PCT.load(Ordering::Relaxed),
        SPLIT_DELAY_MAX_US.load(Ordering::Relaxed),
    );
}

pub fn index_traversal_hook() {
    if !is_enabled() { return; }
    maybe_sleep(
        INDEX_TRAVERSAL_DELAY_PCT.load(Ordering::Relaxed),
        INDEX_TRAVERSAL_DELAY_MAX_US.load(Ordering::Relaxed),
    );
}

pub fn should_skip_fsync() -> bool {
    if !is_enabled() { return false; }
    let pct = SKIP_FSYNC_PCT.load(Ordering::Relaxed);
    if pct == 0 { return false; }
    (next_rand() % 100) < pct
}

pub fn should_partial_write() -> bool {
    if !is_enabled() { return false; }
    let pct = PARTIAL_WRITE_PCT.load(Ordering::Relaxed);
    if pct == 0 { return false; }
    (next_rand() % 100) < pct
}
