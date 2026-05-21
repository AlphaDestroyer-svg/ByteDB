use std::collections::HashSet;

use crate::error::Result;
use super::log_record::{LogRecord, Lsn, TxnId};
use super::log_manager::LogManager;

pub struct RecoveryManager;

impl RecoveryManager {
    pub fn recover(log_manager: &LogManager) -> Result<RecoveryResult> {
        let records = log_manager.read_all_records()?;

        let mut active_txns: HashSet<TxnId> = HashSet::new();
        let mut committed_txns: HashSet<TxnId> = HashSet::new();
        let mut aborted_txns: HashSet<TxnId> = HashSet::new();
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
                    aborted_txns.insert(*txn_id);
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
