//! Background vacuum / MVCC garbage collector.
//!
//! Periodically asks the [`TransactionManager`] for the oldest active
//! start timestamp and tells every registered [`VersionStore`] to drop
//! versions invisible to all live snapshots.
//!
//! The set of version stores is provided as a closure rather than an
//! owned list so callers (the query engine) can add and drop tables
//! without notifying us.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::mvcc::transaction::TransactionManager;
use crate::mvcc::version_store::VersionStore;
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

/// Simple MVCC vacuum that GCs every store returned by `stores_fn` on
/// each tick using the txn manager's oldest active timestamp.
pub struct MvccVacuum {
    txn_manager: Arc<TransactionManager>,
    stores_fn: Box<dyn Fn() -> Vec<Arc<VersionStore>> + Send + Sync + 'static>,
    versions_removed: AtomicU64,
    keys_removed: AtomicU64,
    runs: AtomicU64,
}

impl MvccVacuum {
    pub fn new<F>(txn_manager: Arc<TransactionManager>, stores_fn: F) -> Arc<Self>
    where
        F: Fn() -> Vec<Arc<VersionStore>> + Send + Sync + 'static,
    {
        Arc::new(MvccVacuum {
            txn_manager,
            stores_fn: Box::new(stores_fn),
            versions_removed: AtomicU64::new(0),
            keys_removed: AtomicU64::new(0),
            runs: AtomicU64::new(0),
        })
    }

    pub fn versions_removed(&self) -> u64 {
        self.versions_removed.load(Ordering::Relaxed)
    }
    pub fn keys_removed(&self) -> u64 {
        self.keys_removed.load(Ordering::Relaxed)
    }
    pub fn runs(&self) -> u64 {
        self.runs.load(Ordering::Relaxed)
    }

    /// Execute one vacuum pass synchronously. Useful for tests and
    /// explicit VACUUM commands.
    pub fn run_once(&self) {
        let oldest = self.txn_manager.oldest_active_start_ts();
        for store in (self.stores_fn)() {
            let stats = store.gc_with_aborted(oldest, &[]);
            self.versions_removed
                .fetch_add(stats.versions_removed, Ordering::Relaxed);
            self.keys_removed
                .fetch_add(stats.keys_removed, Ordering::Relaxed);
        }
        self.runs.fetch_add(1, Ordering::Relaxed);
    }
}

impl VacuumPass for MvccVacuum {
    fn run(&self) {
        self.run_once();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::transaction::IsolationLevel;
    use crate::tuple::tuple::Tuple;
    use crate::tuple::value::Value;

    #[test]
    fn vacuum_drops_invisible_tombstone() {
        let tm = Arc::new(TransactionManager::new());
        let store = Arc::new(VersionStore::new());

        // T1 inserts a row, then T2 deletes it, both commit.
        let t1 = tm.begin(IsolationLevel::ReadCommitted);
        let t1_ts = 1;
        store.insert(
            b"k".to_vec(),
            Tuple::new(vec![Value::Int64(1)]),
            t1,
            t1_ts,
        );
        tm.commit(t1).unwrap();

        let t2 = tm.begin(IsolationLevel::ReadCommitted);
        let t2_ts = 2;
        assert!(store.delete(b"k", t2, t2_ts));
        tm.commit(t2).unwrap();

        assert_eq!(store.total_versions(), 1);

        // No live snapshots remain — vacuum should drop the tombstoned
        // version (and the key, since the chain becomes empty).
        let stores = vec![Arc::clone(&store)];
        let stores_fn = move || stores.clone();
        let v = MvccVacuum::new(Arc::clone(&tm), stores_fn);
        v.run_once();
        assert_eq!(v.versions_removed(), 1);
        assert_eq!(v.keys_removed(), 1);
        assert_eq!(store.total_versions(), 0);
    }

    #[test]
    fn vacuum_keeps_versions_visible_to_active_txn() {
        let tm = Arc::new(TransactionManager::new());
        let store = Arc::new(VersionStore::new());

        let t1 = tm.begin(IsolationLevel::ReadCommitted);
        store.insert(
            b"k".to_vec(),
            Tuple::new(vec![Value::Int64(1)]),
            t1,
            1,
        );
        tm.commit(t1).unwrap();

        // Long-running snapshot starts BEFORE the delete — it must still
        // be able to see the row, which means the tombstone we're about
        // to write cannot be reaped while T3 is alive.
        let _t3 = tm.begin(IsolationLevel::Serializable);
        let t3_start = tm.oldest_active_start_ts();

        let t2 = tm.begin(IsolationLevel::ReadCommitted);
        // Tombstone deleted_ts must be > t3_start so the GC predicate
        // (deleted_ts < oldest_active_ts) leaves it alone.
        store.delete(b"k", t2, t3_start + 10);
        tm.commit(t2).unwrap();

        let stores = vec![Arc::clone(&store)];
        let stores_fn = move || stores.clone();
        let v = MvccVacuum::new(Arc::clone(&tm), stores_fn);
        v.run_once();

        // T3 might still need to see the row at its snapshot, so the
        // tombstone must NOT be reaped yet.
        assert_eq!(store.total_versions(), 1);
        assert_eq!(v.versions_removed(), 0);
    }
}
