use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use std::time::{Duration, Instant};
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
    pub started_at: Instant,
    pub deadline: Option<Instant>,
    pub snapshot_active: Vec<TxnId>,
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
    default_txn_timeout: RwLock<Option<Duration>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            next_txn_id: AtomicU64::new(1),
            next_timestamp: AtomicU64::new(1),
            active_txns: RwLock::new(HashMap::new()),
            committed_txns: RwLock::new(HashMap::new()),
            default_txn_timeout: RwLock::new(None),
        }
    }

    pub fn set_default_txn_timeout(&self, d: Option<Duration>) {
        *self.default_txn_timeout.write() = d;
    }

    pub fn default_txn_timeout(&self) -> Option<Duration> {
        *self.default_txn_timeout.read()
    }

    pub fn begin(&self, isolation: IsolationLevel) -> TxnId {
        self.begin_with_timeout(isolation, self.default_txn_timeout())
    }

    pub fn begin_with_timeout(
        &self,
        isolation: IsolationLevel,
        timeout: Option<Duration>,
    ) -> TxnId {
        let now = Instant::now();
        let mut active = self.active_txns.write();
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let start_ts = self.next_timestamp.fetch_add(1, Ordering::SeqCst);
        let snapshot_active: Vec<TxnId> = active.keys().copied().collect();

        let txn = Transaction {
            id: txn_id,
            isolation,
            start_ts,
            commit_ts: None,
            status: TxnStatus::Active,
            started_at: now,
            deadline: timeout.map(|d| now + d),
            snapshot_active,
        };

        active.insert(txn_id, txn);
        txn_id
    }

    pub fn check_deadline(&self, txn_id: TxnId) -> Result<()> {
        let active = self.active_txns.read();
        let txn = active.get(&txn_id)
            .ok_or(CoreError::TransactionNotFound(txn_id))?;
        if let Some(dl) = txn.deadline {
            if Instant::now() >= dl {
                return Err(CoreError::TransactionTimeout(txn_id));
            }
        }
        Ok(())
    }

    pub fn deadline(&self, txn_id: TxnId) -> Option<Instant> {
        self.active_txns.read().get(&txn_id).and_then(|t| t.deadline)
    }

    pub fn timed_out_txns(&self) -> Vec<TxnId> {
        let now = Instant::now();
        self.active_txns
            .read()
            .values()
            .filter(|t| t.deadline.map(|d| now >= d).unwrap_or(false))
            .map(|t| t.id)
            .collect()
    }

    pub fn commit(&self, txn_id: TxnId) -> Result<Timestamp> {
        let commit_ts = {
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
            commit_ts
        };
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

        Ok(Snapshot {
            txn_id,
            start_ts: txn.start_ts,
            active_txns: txn.snapshot_active.clone(),
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

    pub fn oldest_active_start_ts(&self) -> Timestamp {
        let active = self.active_txns.read();
        active
            .values()
            .map(|t| t.start_ts)
            .min()
            .unwrap_or_else(|| self.next_timestamp.load(Ordering::Relaxed))
    }

    pub fn active_txn_ids(&self) -> Vec<TxnId> {
        self.active_txns.read().keys().copied().collect()
    }

    pub fn gc_committed(&self, oldest_active_ts: Timestamp) -> usize {
        let mut committed = self.committed_txns.write();
        let before = committed.len();
        committed.retain(|_, &mut ts| ts >= oldest_active_ts);
        before - committed.len()
    }

    pub fn committed_count(&self) -> usize {
        self.committed_txns.read().len()
    }

    pub fn is_aborted(&self, txn_id: TxnId) -> bool {

        !self.is_active(txn_id) && !self.is_committed(txn_id)
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}
