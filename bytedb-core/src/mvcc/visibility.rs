use super::transaction::{Snapshot, TxnId};
use super::version_store::VersionedTuple;

pub fn is_tuple_visible(
    version: &VersionedTuple,
    snapshot: &Snapshot,
) -> bool {
    if version.created_by == snapshot.txn_id {
        return version.deleted_by.is_none() || version.deleted_by != Some(snapshot.txn_id);
    }

    if snapshot.active_txns.contains(&version.created_by) {
        return false;
    }

    if version.created_ts > snapshot.start_ts {
        return false;
    }

    if let Some(deleted_by) = version.deleted_by {
        if deleted_by == snapshot.txn_id {
            return false;
        }
        if !snapshot.active_txns.contains(&deleted_by) {
            if let Some(deleted_ts) = version.deleted_ts {
                if deleted_ts <= snapshot.start_ts {
                    return false;
                }
            }
        }
    }

    true
}

pub fn check_write_conflict(
    version: &VersionedTuple,
    txn_id: TxnId,
    snapshot: &Snapshot,
) -> bool {
    if let Some(deleted_by) = version.deleted_by {
        if deleted_by != txn_id && snapshot.active_txns.contains(&deleted_by) {
            return true;
        }
    }

    if version.created_by != txn_id
        && snapshot.active_txns.contains(&version.created_by)
        && version.created_ts > snapshot.start_ts
    {
        return true;
    }

    false
}
