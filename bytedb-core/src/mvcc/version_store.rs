use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use parking_lot::RwLock;
use ahash::RandomState;

use super::transaction::{TxnId, Timestamp};
use crate::tuple::tuple::Tuple;

const SHARD_COUNT: usize = 64;

#[derive(Debug, Clone)]
pub struct VersionedTuple {
    pub data: Tuple,
    pub created_by: TxnId,
    pub deleted_by: Option<TxnId>,
    pub created_ts: Timestamp,
    pub deleted_ts: Option<Timestamp>,
}

pub struct VersionStore {
    shards: Vec<RwLock<HashMap<Vec<u8>, Vec<VersionedTuple>, RandomState>>>,
    approx_keys: AtomicUsize,
}

impl VersionStore {
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards.push(RwLock::new(HashMap::with_hasher(RandomState::new())));
        }
        VersionStore { shards, approx_keys: AtomicUsize::new(0) }
    }

    pub fn is_empty_fast(&self) -> bool {
        self.approx_keys.load(Ordering::Relaxed) == 0
    }

    fn shard_index(key: &[u8]) -> usize {
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in key {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        (h as usize) & (SHARD_COUNT - 1)
    }

    fn shard_for(&self, key: &[u8]) -> &RwLock<HashMap<Vec<u8>, Vec<VersionedTuple>, RandomState>> {
        &self.shards[Self::shard_index(key)]
    }

    pub fn insert(&self, key: Vec<u8>, tuple: Tuple, txn_id: TxnId, ts: Timestamp) {
        let versioned = VersionedTuple {
            data: tuple,
            created_by: txn_id,
            deleted_by: None,
            created_ts: ts,
            deleted_ts: None,
        };
        let mut shard = self.shard_for(&key).write();
        let was_empty = !shard.contains_key(&key);
        shard.entry(key).or_insert_with(Vec::new).push(versioned);
        if was_empty {
            self.approx_keys.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn delete(&self, key: &[u8], txn_id: TxnId, ts: Timestamp) -> bool {
        let mut shard = self.shard_for(key).write();
        if let Some(chain) = shard.get_mut(key) {
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

    pub fn try_delete(&self, key: &[u8], txn_id: TxnId, ts: Timestamp, snapshot_ts: Timestamp, active_txns: &[TxnId]) -> Result<bool, crate::error::CoreError> {
        let mut shard = self.shard_for(key).write();
        if let Some(chain) = shard.get_mut(key) {
            if let Some(latest) = chain.last_mut() {
                if let Some(deleter) = latest.deleted_by {
                    if deleter != txn_id && active_txns.contains(&deleter) {
                        return Err(crate::error::CoreError::WriteConflict);
                    }
                    if deleter != txn_id {
                        return Ok(false);
                    }
                }
                if latest.created_by != txn_id && active_txns.contains(&latest.created_by) {
                    return Err(crate::error::CoreError::WriteConflict);
                }
                if latest.created_by != txn_id && latest.created_ts > snapshot_ts {
                    return Err(crate::error::CoreError::WriteConflict);
                }
                if let Some(dts) = latest.deleted_ts {
                    if latest.deleted_by != Some(txn_id) && dts > snapshot_ts {
                        return Err(crate::error::CoreError::WriteConflict);
                    }
                }
                latest.deleted_by = Some(txn_id);
                latest.deleted_ts = Some(ts);
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn try_update(&self, key: Vec<u8>, new_tuple: Tuple, txn_id: TxnId, ts: Timestamp, snapshot_ts: Timestamp, active_txns: &[TxnId]) -> Result<(), crate::error::CoreError> {
        let mut shard = self.shard_for(&key).write();
        let was_empty_chain = !shard.contains_key(&key);
        let chain = shard.entry(key).or_insert_with(Vec::new);
        if let Some(latest) = chain.last() {
            if let Some(deleter) = latest.deleted_by {
                if deleter != txn_id && active_txns.contains(&deleter) {
                    return Err(crate::error::CoreError::WriteConflict);
                }
            }
            if latest.created_by != txn_id && active_txns.contains(&latest.created_by) {
                return Err(crate::error::CoreError::WriteConflict);
            }
            if latest.created_by != txn_id && latest.created_ts > snapshot_ts {
                return Err(crate::error::CoreError::WriteConflict);
            }
            if let Some(dts) = latest.deleted_ts {
                if latest.deleted_by != Some(txn_id) && dts > snapshot_ts {
                    return Err(crate::error::CoreError::WriteConflict);
                }
            }
        }
        chain.push(VersionedTuple {
            data: new_tuple,
            created_by: txn_id,
            deleted_by: None,
            created_ts: ts,
            deleted_ts: None,
        });
        if was_empty_chain {
            self.approx_keys.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn get_visible(&self, key: &[u8], txn_id: TxnId, snapshot_ts: Timestamp, active_txns: &[TxnId]) -> Option<Tuple> {
        let shard = self.shard_for(key).read();
        if let Some(chain) = shard.get(key) {
            for version in chain.iter().rev() {
                if is_visible(version, txn_id, snapshot_ts, active_txns) {
                    return Some(version.data.clone());
                }
            }
        }
        None
    }

    pub fn lookup_for_read(&self, key: &[u8], txn_id: TxnId, snapshot_ts: Timestamp, active_txns: &[TxnId]) -> ReadResult {
        let shard = self.shard_for(key).read();
        match shard.get(key) {
            None => ReadResult::NoVersions,
            Some(chain) if chain.is_empty() => ReadResult::NoVersions,
            Some(chain) => {
                for version in chain.iter().rev() {
                    if is_visible(version, txn_id, snapshot_ts, active_txns) {
                        return ReadResult::Visible(version.data.clone());
                    }
                }
                ReadResult::Tombstone
            }
        }
    }

    pub fn get_all_visible(&self, txn_id: TxnId, snapshot_ts: Timestamp, active_txns: &[TxnId]) -> Vec<(Vec<u8>, Tuple)> {
        let mut results = Vec::new();
        for shard_lock in &self.shards {
            let shard = shard_lock.read();
            for (key, chain) in shard.iter() {
                for version in chain.iter().rev() {
                    if is_visible(version, txn_id, snapshot_ts, active_txns) {
                        results.push((key.clone(), version.data.clone()));
                        break;
                    }
                }
            }
        }
        results
    }

    pub fn snapshot_resolved(&self, txn_id: TxnId, snapshot_ts: Timestamp, active_txns: &[TxnId]) -> HashMap<Vec<u8>, Option<Tuple>, RandomState> {
        let mut out: HashMap<Vec<u8>, Option<Tuple>, RandomState> = HashMap::with_hasher(RandomState::new());
        for shard_lock in &self.shards {
            let shard = shard_lock.read();
            for (key, chain) in shard.iter() {
                let mut found_visible = false;
                for version in chain.iter().rev() {
                    if is_visible(version, txn_id, snapshot_ts, active_txns) {
                        out.insert(key.clone(), Some(version.data.clone()));
                        found_visible = true;
                        break;
                    }
                }
                if !found_visible {
                    out.insert(key.clone(), None);
                }
            }
        }
        out
    }

    pub fn gc(&self, oldest_active_ts: Timestamp) {
        self.gc_with_aborted(oldest_active_ts, &[]);
    }

    pub fn gc_with_aborted(
        &self,
        oldest_active_ts: Timestamp,
        aborted_txns: &[TxnId],
    ) -> GcStats {
        let mut versions_removed: u64 = 0;
        let mut keys_removed: u64 = 0;
        for shard_lock in &self.shards {
            let mut shard = shard_lock.write();
            shard.retain(|_, chain| {
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
        }
        if keys_removed > 0 {
            self.approx_keys.fetch_sub(keys_removed as usize, Ordering::Relaxed);
        }
        GcStats {
            versions_removed,
            keys_removed,
        }
    }

    pub fn rollback_txn(&self, txn_id: TxnId) {
        let mut removed_keys: usize = 0;
        for shard_lock in &self.shards {
            let mut shard = shard_lock.write();
            shard.retain(|_, chain| {
                for v in chain.iter_mut() {
                    if v.deleted_by == Some(txn_id) {
                        v.deleted_by = None;
                        v.deleted_ts = None;
                    }
                }
                chain.retain(|v| v.created_by != txn_id);
                if chain.is_empty() {
                    removed_keys += 1;
                    false
                } else {
                    true
                }
            });
        }
        if removed_keys > 0 {
            self.approx_keys.fetch_sub(removed_keys, Ordering::Relaxed);
        }
    }

    pub fn key_count(&self) -> usize {
        self.approx_keys.load(Ordering::Relaxed)
    }

    pub fn total_versions(&self) -> usize {
        self.shards.iter()
            .map(|s| s.read().values().map(|c| c.len()).sum::<usize>())
            .sum()
    }

    pub fn live_dead_counts(&self, oldest_active_ts: Timestamp) -> (u64, u64) {
        let mut live: u64 = 0;
        let mut dead: u64 = 0;
        for shard_lock in &self.shards {
            let shard = shard_lock.read();
            for chain in shard.values() {
                for v in chain.iter() {
                    let is_dead = match v.deleted_ts {
                        Some(dts) => dts < oldest_active_ts,
                        None => false,
                    };
                    if is_dead { dead += 1; } else { live += 1; }
                }
            }
        }
        (live, dead)
    }

    pub fn for_each_visible<F>(&self, txn_id: TxnId, snapshot_ts: Timestamp, active_txns: &[TxnId], mut f: F)
    where
        F: FnMut(&[u8], &Tuple) -> bool,
    {
        for shard_lock in &self.shards {
            let shard = shard_lock.read();
            for (key, chain) in shard.iter() {
                for version in chain.iter().rev() {
                    if is_visible(version, txn_id, snapshot_ts, active_txns) {
                        if !f(key, &version.data) {
                            return;
                        }
                        break;
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GcStats {
    pub versions_removed: u64,
    pub keys_removed: u64,
}

#[derive(Debug, Clone)]
pub enum ReadResult {
    Visible(Tuple),
    Tombstone,
    NoVersions,
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
