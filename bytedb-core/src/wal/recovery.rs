use std::collections::HashSet;

use crate::error::Result;
use super::log_record::{LogRecord, Lsn, TxnId};
use super::log_manager::LogManager;

pub struct RecoveryManager;

impl RecoveryManager {
    pub fn recover(log_manager: &LogManager) -> Result<RecoveryResult> {
        Self::recover_inner(log_manager, None)
    }

    pub fn recover_to_lsn(log_manager: &LogManager, target_lsn: Lsn) -> Result<RecoveryResult> {
        Self::recover_inner(log_manager, Some(target_lsn))
    }

    fn recover_inner(log_manager: &LogManager, target_lsn: Option<Lsn>) -> Result<RecoveryResult> {
        let all = log_manager.read_all_records()?;
        let records: Vec<(Lsn, LogRecord)> = match target_lsn {
            Some(t) => all.into_iter().filter(|(lsn, _)| *lsn <= t).collect(),
            None => all,
        };
        Self::analyze_records(records)
    }

    fn analyze_records(records: Vec<(Lsn, LogRecord)>) -> Result<RecoveryResult> {
        let mut active_txns: HashSet<TxnId> = HashSet::new();
        let mut committed_txns: HashSet<TxnId> = HashSet::new();
        let mut _aborted_txns: HashSet<TxnId> = HashSet::new();
        let mut redo_records: Vec<(Lsn, LogRecord)> = Vec::new();
        let mut undo_records: Vec<(Lsn, LogRecord)> = Vec::new();

        for (_lsn, record) in &records {
            match record {
                LogRecord::Begin { txn_id } => {
                    active_txns.insert(*txn_id);
                }
                LogRecord::Commit { txn_id } => {
                    active_txns.remove(txn_id);
                    committed_txns.insert(*txn_id);
                }
                LogRecord::Abort { txn_id } => {
                    active_txns.remove(txn_id);
                    _aborted_txns.insert(*txn_id);
                }
                LogRecord::Checkpoint { active_txns: checkpoint_txns } => {
                    active_txns.clear();
                    for txn_id in checkpoint_txns {
                        active_txns.insert(*txn_id);
                    }
                }
                _ => {}
            }
        }

        for (lsn, record) in &records {
            if let Some(txn_id) = record.txn_id() {
                if committed_txns.contains(&txn_id) {
                    match record {
                        LogRecord::Insert { .. }
                        | LogRecord::Update { .. }
                        | LogRecord::Delete { .. } => {
                            redo_records.push((*lsn, record.clone()));
                        }
                        _ => {}
                    }
                }
            }
        }

        for (lsn, record) in records.iter().rev() {
            if let Some(txn_id) = record.txn_id() {
                if active_txns.contains(&txn_id) {
                    match record {
                        LogRecord::Insert { .. }
                        | LogRecord::Update { .. }
                        | LogRecord::Delete { .. } => {
                            undo_records.push((*lsn, record.clone()));
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(RecoveryResult {
            redo_records,
            undo_records,
            committed_txns,
            aborted_txns: active_txns.into_iter().collect(),
        })
    }
}

#[derive(Debug)]
pub struct RecoveryResult {
    pub redo_records: Vec<(Lsn, LogRecord)>,
    pub undo_records: Vec<(Lsn, LogRecord)>,
    pub committed_txns: HashSet<TxnId>,
    pub aborted_txns: HashSet<TxnId>,
}

#[cfg(test)]
mod pitr_tests {
    use super::*;
    use crate::wal::log_manager::LogManager;
    use tempfile::tempdir;

    #[test]
    fn recover_to_lsn_truncates_replay() {
        let d = tempdir().unwrap();
        let p = d.path().join("wal.log");
        let lm = LogManager::new(&p).unwrap();

        lm.append(LogRecord::Begin { txn_id: 1 }).unwrap();
        lm.append(LogRecord::Insert { txn_id: 1, table_id: 1, page_id: 0, slot: 0, data: b"a".to_vec() }).unwrap();
        lm.append(LogRecord::Commit { txn_id: 1 }).unwrap();
        lm.append(LogRecord::Begin { txn_id: 2 }).unwrap();
        lm.append(LogRecord::Insert { txn_id: 2, table_id: 1, page_id: 0, slot: 1, data: b"b".to_vec() }).unwrap();
        let cutoff = lm.append(LogRecord::Commit { txn_id: 2 }).unwrap();
        lm.append(LogRecord::Begin { txn_id: 3 }).unwrap();
        lm.append(LogRecord::Insert { txn_id: 3, table_id: 1, page_id: 0, slot: 2, data: b"c".to_vec() }).unwrap();
        lm.append(LogRecord::Commit { txn_id: 3 }).unwrap();
        lm.flush().unwrap();

        let full = RecoveryManager::recover(&lm).unwrap();
        assert_eq!(full.committed_txns.len(), 3);

        let pitr = RecoveryManager::recover_to_lsn(&lm, cutoff).unwrap();
        assert_eq!(pitr.committed_txns.len(), 2);
        assert!(pitr.committed_txns.contains(&1));
        assert!(pitr.committed_txns.contains(&2));
        assert!(!pitr.committed_txns.contains(&3));
    }

    #[test]
    fn recover_to_lsn_zero_yields_nothing() {
        let d = tempdir().unwrap();
        let p = d.path().join("wal.log");
        let lm = LogManager::new(&p).unwrap();
        lm.append(LogRecord::Begin { txn_id: 1 }).unwrap();
        lm.append(LogRecord::Commit { txn_id: 1 }).unwrap();
        lm.flush().unwrap();
        let r = RecoveryManager::recover_to_lsn(&lm, 0).unwrap();
        assert!(r.committed_txns.is_empty());
    }
}
