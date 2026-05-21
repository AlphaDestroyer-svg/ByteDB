use std::collections::HashMap;
use parking_lot::RwLock;

use super::transaction::{TxnId, Timestamp};
use crate::tuple::tuple::Tuple;

#[derive(Debug, Clone)]
pub struct VersionedTuple {
    pub data: Tuple,
    pub created_by: TxnId,
    pub deleted_by: Option<TxnId>,
    pub created_ts: Timestamp,
    pub deleted_ts: Option<Timestamp>,
}

pub struct VersionStore {
    versions: RwLock<HashMap<Vec<u8>, Vec<VersionedTuple>>>,
}

impl VersionStore {
    pub fn new() -> Self {
        VersionStore {
            versions: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert(&self, key: Vec<u8>, tuple: Tuple, txn_id: TxnId, ts: Timestamp) {
        let versioned = VersionedTuple {
            data: tuple,
            created_by: txn_id,
            deleted_by: None,
            created_ts: ts,
            deleted_ts: None,
        };

        let mut versions = self.versions.write();
        versions.entry(key).or_insert_with(Vec::new).push(versioned);
    }

    pub fn delete(&self, key: &[u8], txn_id: TxnId, ts: Timestamp) -> bool {
        let mut versions = self.versions.write();
        if let Some(chain) = versions.get_mut(key) {
            for version in chain.iter_mut().rev() {
                if version.deleted_by.is_none() {
                    version.deleted_by = Some(txn_id);
                    version.deleted_ts = Some(ts);
                    return true;
                }
            }
        }
        false
    }

    pub fn get_visible(&self, key: &[u8], txn_id: TxnId, snapshot_ts: Timestamp, active_txns: &[TxnId]) -> Option<Tuple> {
        let versions = self.versions.read();
        if let Some(chain) = versions.get(key) {
            for version in chain.iter().rev() {
                if is_visible(version, txn_id, snapshot_ts, active_txns) {
                    return Some(version.data.clone());
                }
            }
        }
        None
    }

    pub fn get_all_visible(&self, txn_id: TxnId, snapshot_ts: Timestamp, active_txns: &[TxnId]) -> Vec<(Vec<u8>, Tuple)> {
        let versions = self.versions.read();
        let mut results = Vec::new();

        for (key, chain) in versions.iter() {
            for version in chain.iter().rev() {
                if is_visible(version, txn_id, snapshot_ts, active_txns) {
                    results.push((key.clone(), version.data.clone()));
                    break;
                }
            }
        }

        results
    }

    pub fn gc(&self, oldest_active_ts: Timestamp) {
        self.gc_with_aborted(oldest_active_ts, &[]);
    }

    pub fn gc_with_aborted(
        &self,
        oldest_active_ts: Timestamp,
        aborted_txns: &[TxnId],
    ) -> GcStats {
        let mut versions = self.versions.write();
        let mut versions_removed: u64 = 0;
        let mut keys_removed: u64 = 0;
        versions.retain(|_, chain| {
            let before = chain.len();
            chain.retain(|v| {

                if aborted_txns.contains(&v.created_by) {
                    return false;
                }

                if let Some(deleted_ts) = v.deleted_ts {
                    if deleted_ts < oldest_active_ts {
                        return false;
                    }
                }
                true
            });
            versions_removed += (before - chain.len()) as u64;
            if chain.is_empty() {
                keys_removed += 1;
                false
            } else {
                true
            }
        });
        GcStats {
            versions_removed,
            keys_removed,
        }
    }

    pub fn key_count(&self) -> usize {
        self.versions.read().len()
    }

    pub fn total_versions(&self) -> usize {
        self.versions.read().values().map(|c| c.len()).sum()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GcStats {
    pub versions_removed: u64,
    pub keys_removed: u64,
}

fn is_visible(version: &VersionedTuple, txn_id: TxnId, snapshot_ts: Timestamp, active_txns: &[TxnId]) -> bool {
    if version.created_by == txn_id {
        return version.deleted_by.is_none() || version.deleted_by == Some(txn_id);
    }

    if active_txns.contains(&version.created_by) {
        return false;
    }

    if version.created_ts > snapshot_ts {
        return false;
    }

    if let Some(deleted_by) = version.deleted_by {
        if deleted_by == txn_id {
            return false;
        }
        if !active_txns.contains(&deleted_by) {
            if let Some(deleted_ts) = version.deleted_ts {
                if deleted_ts <= snapshot_ts {
                    return false;
                }
            }
        }
    }

    true
}

impl Default for VersionStore {
    fn default() -> Self {
        Self::new()
    }
}
