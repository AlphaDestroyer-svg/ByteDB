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

        let _t3 = tm.begin(IsolationLevel::Serializable);
        let t3_start = tm.oldest_active_start_ts();

        let t2 = tm.begin(IsolationLevel::ReadCommitted);

        store.delete(b"k", t2, t3_start + 10);
        tm.commit(t2).unwrap();

        let stores = vec![Arc::clone(&store)];
        let stores_fn = move || stores.clone();
        let v = MvccVacuum::new(Arc::clone(&tm), stores_fn);
        v.run_once();

        assert_eq!(store.total_versions(), 1);
        assert_eq!(v.versions_removed(), 0);
    }
}
