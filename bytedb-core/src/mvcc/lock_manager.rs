use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use parking_lot::{Condvar, Mutex};

use crate::error::{CoreError, Result};
use super::transaction::TxnId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    Shared,
    Exclusive,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LockKey {
    pub table: String,
    pub key: Vec<u8>,
}

impl LockKey {
    pub fn new(table: impl Into<String>, key: impl Into<Vec<u8>>) -> Self {
        LockKey { table: table.into(), key: key.into() }
    }
}

#[derive(Debug, Clone, Copy)]
struct Holder {
    txn: TxnId,
    mode: LockMode,
}

#[derive(Debug)]
struct WaitEntry {
    txn: TxnId,
    mode: LockMode,
}

#[derive(Debug, Default)]
struct LockState {
    holders: Vec<Holder>,
    waiters: VecDeque<WaitEntry>,
}

impl LockState {
    fn is_compatible(&self, mode: LockMode, requesting: TxnId) -> bool {
        if self.holders.is_empty() {
            return true;
        }
        if self.holders.iter().all(|h| h.txn == requesting) {
            return true;
        }
        match mode {
            LockMode::Shared => self.holders.iter().all(|h| h.mode == LockMode::Shared),
            LockMode::Exclusive => false,
        }
    }

    fn add_holder(&mut self, txn: TxnId, mode: LockMode) {
        if let Some(h) = self.holders.iter_mut().find(|h| h.txn == txn) {
            if mode == LockMode::Exclusive {
                h.mode = LockMode::Exclusive;
            }
            return;
        }
        self.holders.push(Holder { txn, mode });
    }

    fn remove_holder(&mut self, txn: TxnId) -> bool {
        let before = self.holders.len();
        self.holders.retain(|h| h.txn != txn);
        self.holders.len() != before
    }
}

#[derive(Debug, Default, Clone)]
pub struct LockMetrics {
    pub acquires: u64,
    pub releases: u64,
    pub waits: u64,
    pub timeouts: u64,
    pub deadlocks: u64,
    pub total_wait_micros: u64,
}

pub struct LockManager {
    locks: Mutex<HashMap<LockKey, LockState>>,
    cv: Condvar,
    metrics: Mutex<LockMetrics>,
    default_timeout: Mutex<Duration>,
    deadlock_check_interval: AtomicU64,
}

impl LockManager {
    pub fn new() -> Self {
        LockManager {
            locks: Mutex::new(HashMap::new()),
            cv: Condvar::new(),
            metrics: Mutex::new(LockMetrics::default()),
            default_timeout: Mutex::new(Duration::from_secs(5)),
            deadlock_check_interval: AtomicU64::new(50),
        }
    }

    pub fn set_default_timeout(&self, d: Duration) {
        *self.default_timeout.lock() = d;
    }

    pub fn default_timeout(&self) -> Duration {
        *self.default_timeout.lock()
    }

    pub fn set_deadlock_check_interval_ms(&self, ms: u64) {
        self.deadlock_check_interval.store(ms, Ordering::Relaxed);
    }

    pub fn metrics(&self) -> LockMetrics {
        self.metrics.lock().clone()
    }

    pub fn acquire(&self, txn: TxnId, key: LockKey, mode: LockMode) -> Result<()> {
        self.acquire_with_timeout(txn, key, mode, self.default_timeout())
    }

    pub fn acquire_with_timeout(
        &self,
        txn: TxnId,
        key: LockKey,
        mode: LockMode,
        timeout: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let check_interval = Duration::from_millis(
            self.deadlock_check_interval.load(Ordering::Relaxed),
        );
        let mut guard = self.locks.lock();

        loop {
            let granted = {
                let state = guard.entry(key.clone()).or_default();
                if state.is_compatible(mode, txn) {
                    state.add_holder(txn, mode);
                    true
                } else {
                    false
                }
            };
            if granted {
                self.metrics.lock().acquires += 1;
                let waited = start.elapsed();
                if waited > Duration::from_micros(0) {
                    self.metrics.lock().total_wait_micros += waited.as_micros() as u64;
                }
                return Ok(());
            }

            {
                let state = guard.get_mut(&key).expect("entry just inserted");
                if !state.waiters.iter().any(|w| w.txn == txn) {
                    state.waiters.push_back(WaitEntry { txn, mode });
                    self.metrics.lock().waits += 1;
                }
            }

            if self.detect_deadlock(&guard, txn) {
                self.remove_waiter_locked(&mut guard, &key, txn);
                self.metrics.lock().deadlocks += 1;
                return Err(CoreError::Deadlock);
            }

            let elapsed = start.elapsed();
            if elapsed >= timeout {
                self.remove_waiter_locked(&mut guard, &key, txn);
                self.metrics.lock().timeouts += 1;
                self.metrics.lock().total_wait_micros += elapsed.as_micros() as u64;
                return Err(CoreError::LockTimeout(txn));
            }
            let remaining = timeout - elapsed;
            let sleep_for = remaining.min(check_interval);
            self.cv.wait_for(&mut guard, sleep_for);
        }
    }

    fn remove_waiter_locked(
        &self,
        guard: &mut parking_lot::MutexGuard<HashMap<LockKey, LockState>>,
        key: &LockKey,
        txn: TxnId,
    ) {
        if let Some(state) = guard.get_mut(key) {
            state.waiters.retain(|w| w.txn != txn);
        }
    }

    pub fn release(&self, txn: TxnId, key: &LockKey) {
        let mut guard = self.locks.lock();
        let mut empty = false;
        if let Some(state) = guard.get_mut(key) {
            if state.remove_holder(txn) {
                self.metrics.lock().releases += 1;
            }
            self.grant_pending(state);
            empty = state.holders.is_empty() && state.waiters.is_empty();
        }
        if empty {
            guard.remove(key);
        }
        self.cv.notify_all();
    }

    pub fn release_all(&self, txn: TxnId) {
        let mut guard = self.locks.lock();
        let mut to_drop: Vec<LockKey> = Vec::new();
        for (k, state) in guard.iter_mut() {
            if state.remove_holder(txn) {
                self.metrics.lock().releases += 1;
            }
            state.waiters.retain(|w| w.txn != txn);
            self.grant_pending(state);
            if state.holders.is_empty() && state.waiters.is_empty() {
                to_drop.push(k.clone());
            }
        }
        for k in to_drop {
            guard.remove(&k);
        }
        self.cv.notify_all();
    }

    fn grant_pending(&self, state: &mut LockState) {
        while let Some(front) = state.waiters.front() {
            let txn = front.txn;
            let mode = front.mode;
            let compatible = if state.holders.is_empty() {
                true
            } else if state.holders.iter().all(|h| h.txn == txn) {
                true
            } else {
                match mode {
                    LockMode::Shared => state.holders.iter().all(|h| h.mode == LockMode::Shared),
                    LockMode::Exclusive => false,
                }
            };
            if compatible {
                state.waiters.pop_front();
                state.add_holder(txn, mode);
            } else {
                break;
            }
        }
    }

    fn detect_deadlock(
        &self,
        guard: &parking_lot::MutexGuard<HashMap<LockKey, LockState>>,
        starting: TxnId,
    ) -> bool {
        let mut waits_for: HashMap<TxnId, HashSet<TxnId>> = HashMap::new();
        for state in guard.values() {
            for waiter in &state.waiters {
                let entry = waits_for.entry(waiter.txn).or_default();
                for h in &state.holders {
                    if h.txn != waiter.txn {
                        entry.insert(h.txn);
                    }
                }
            }
        }
        let mut visited: HashSet<TxnId> = HashSet::new();
        let mut stack: Vec<TxnId> = vec![starting];
        let mut on_path: HashSet<TxnId> = HashSet::new();
        on_path.insert(starting);

        while let Some(node) = stack.last().copied() {
            let neighbors = waits_for.get(&node).cloned().unwrap_or_default();
            let unvisited = neighbors.iter().find(|n| !visited.contains(n));
            match unvisited {
                Some(&next) => {
                    if on_path.contains(&next) {
                        return true;
                    }
                    on_path.insert(next);
                    stack.push(next);
                }
                None => {
                    visited.insert(node);
                    on_path.remove(&node);
                    stack.pop();
                }
            }
        }
        false
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn k(t: &str, key: &[u8]) -> LockKey {
        LockKey::new(t.to_string(), key.to_vec())
    }

    #[test]
    fn shared_locks_compatible() {
        let lm = LockManager::new();
        lm.acquire(1, k("t", b"a"), LockMode::Shared).unwrap();
        lm.acquire(2, k("t", b"a"), LockMode::Shared).unwrap();
        let m = lm.metrics();
        assert_eq!(m.acquires, 2);
        assert_eq!(m.waits, 0);
    }

    #[test]
    fn exclusive_blocks_then_grants_after_release() {
        let lm = Arc::new(LockManager::new());
        lm.acquire(1, k("t", b"a"), LockMode::Exclusive).unwrap();
        let lm2 = Arc::clone(&lm);
        let h = thread::spawn(move || {
            lm2.acquire_with_timeout(2, k("t", b"a"), LockMode::Exclusive, Duration::from_secs(2))
                .unwrap();
        });
        thread::sleep(Duration::from_millis(50));
        lm.release(1, &k("t", b"a"));
        h.join().unwrap();
        let m = lm.metrics();
        assert!(m.waits >= 1);
        assert_eq!(m.acquires, 2);
    }

    #[test]
    fn lock_timeout_fires() {
        let lm = LockManager::new();
        lm.acquire(1, k("t", b"a"), LockMode::Exclusive).unwrap();
        let err = lm
            .acquire_with_timeout(2, k("t", b"a"), LockMode::Exclusive, Duration::from_millis(80))
            .unwrap_err();
        match err {
            CoreError::LockTimeout(t) => assert_eq!(t, 2),
            other => panic!("expected LockTimeout, got {:?}", other),
        }
        assert_eq!(lm.metrics().timeouts, 1);
    }

    #[test]
    fn deadlock_detected() {
        let lm = Arc::new(LockManager::new());
        lm.acquire(1, k("t", b"a"), LockMode::Exclusive).unwrap();
        lm.acquire(2, k("t", b"b"), LockMode::Exclusive).unwrap();

        let lm1 = Arc::clone(&lm);
        let h1 = thread::spawn(move || {
            lm1.acquire_with_timeout(
                1,
                k("t", b"b"),
                LockMode::Exclusive,
                Duration::from_secs(3),
            )
        });
        thread::sleep(Duration::from_millis(50));
        let res2 = lm.acquire_with_timeout(
            2,
            k("t", b"a"),
            LockMode::Exclusive,
            Duration::from_secs(3),
        );
        assert!(matches!(res2, Err(CoreError::Deadlock)));
        lm.release(2, &k("t", b"b"));
        let _ = h1.join().unwrap();
        assert!(lm.metrics().deadlocks >= 1);
    }

    #[test]
    fn release_all_clears_holds_and_wakes_waiters() {
        let lm = Arc::new(LockManager::new());
        lm.acquire(1, k("t", b"a"), LockMode::Exclusive).unwrap();
        let lm2 = Arc::clone(&lm);
        let h = thread::spawn(move || {
            lm2.acquire_with_timeout(
                2,
                k("t", b"a"),
                LockMode::Exclusive,
                Duration::from_secs(2),
            )
            .unwrap();
        });
        thread::sleep(Duration::from_millis(40));
        lm.release_all(1);
        h.join().unwrap();
    }
}
