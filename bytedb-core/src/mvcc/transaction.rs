use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};

use crate::error::{CoreError, Result};

pub type TxnId = u64;
pub type Timestamp = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    Active,
    Committed,
    Aborted,
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub id: TxnId,
    pub isolation: IsolationLevel,
    pub start_ts: Timestamp,
    pub commit_ts: Option<Timestamp>,
    pub status: TxnStatus,
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub txn_id: TxnId,
    pub start_ts: Timestamp,
    pub active_txns: Vec<TxnId>,
}

pub struct TransactionManager {
    next_txn_id: AtomicU64,
    next_timestamp: AtomicU64,
    active_txns: RwLock<HashMap<TxnId, Transaction>>,
    committed_txns: RwLock<HashMap<TxnId, Timestamp>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            next_txn_id: AtomicU64::new(1),
            next_timestamp: AtomicU64::new(1),
            active_txns: RwLock::new(HashMap::new()),
            committed_txns: RwLock::new(HashMap::new()),
        }
    }

    pub fn begin(&self, isolation: IsolationLevel) -> TxnId {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let start_ts = self.next_timestamp.fetch_add(1, Ordering::SeqCst);

        let txn = Transaction {
            id: txn_id,
            isolation,
            start_ts,
            commit_ts: None,
            status: TxnStatus::Active,
        };

        self.active_txns.write().insert(txn_id, txn);
        txn_id
    }

    pub fn commit(&self, txn_id: TxnId) -> Result<Timestamp> {
        let mut active = self.active_txns.write();
        let txn = active.get_mut(&txn_id)
            .ok_or(CoreError::TransactionNotFound(txn_id))?;

        if txn.status != TxnStatus::Active {
            return Err(CoreError::TransactionAborted(txn_id));
        }

        let commit_ts = self.next_timestamp.fetch_add(1, Ordering::SeqCst);
        txn.status = TxnStatus::Committed;
        txn.commit_ts = Some(commit_ts);

        active.remove(&txn_id);
        self.committed_txns.write().insert(txn_id, commit_ts);

        Ok(commit_ts)
    }

    pub fn abort(&self, txn_id: TxnId) -> Result<()> {
        let mut active = self.active_txns.write();
        let txn = active.get_mut(&txn_id)
            .ok_or(CoreError::TransactionNotFound(txn_id))?;

        txn.status = TxnStatus::Aborted;
        active.remove(&txn_id);
        Ok(())
    }

    pub fn get_snapshot(&self, txn_id: TxnId) -> Result<Snapshot> {
        let active = self.active_txns.read();
        let txn = active.get(&txn_id)
            .ok_or(CoreError::TransactionNotFound(txn_id))?;

        let active_txns: Vec<TxnId> = active.keys().copied().collect();

        Ok(Snapshot {
            txn_id,
            start_ts: txn.start_ts,
            active_txns,
        })
    }

    pub fn is_committed(&self, txn_id: TxnId) -> bool {
        self.committed_txns.read().contains_key(&txn_id)
    }

    pub fn get_commit_ts(&self, txn_id: TxnId) -> Option<Timestamp> {
        self.committed_txns.read().get(&txn_id).copied()
    }

    pub fn is_active(&self, txn_id: TxnId) -> bool {
        self.active_txns.read().contains_key(&txn_id)
    }

    pub fn active_count(&self) -> usize {
        self.active_txns.read().len()
    }

    /// The oldest start timestamp of any currently-active transaction.
    /// MVCC versions whose `deleted_ts` is older than this are no longer
    /// visible to anyone and may be garbage-collected.
    ///
    /// Returns the next timestamp (i.e. "all versions are GC-eligible")
    /// when there are no active transactions.
    pub fn oldest_active_start_ts(&self) -> Timestamp {
        let active = self.active_txns.read();
        active
            .values()
            .map(|t| t.start_ts)
            .min()
            .unwrap_or_else(|| self.next_timestamp.load(Ordering::Relaxed))
    }

    /// Snapshot all currently-active transaction ids. Useful for the
    /// vacuum pass to know which creator txns to keep around.
    pub fn active_txn_ids(&self) -> Vec<TxnId> {
        self.active_txns.read().keys().copied().collect()
    }

    /// Whether a transaction was aborted (and thus its created versions
    /// are dead immediately).
    pub fn is_aborted(&self, txn_id: TxnId) -> bool {
        // We currently drop aborted txns from active without recording
        // them in committed_txns. So "not active and not committed" =>
        // aborted-or-never-existed. For GC purposes that's fine: a txn
        // that never existed has no versions to consider.
        !self.is_active(txn_id) && !self.is_committed(txn_id)
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}
