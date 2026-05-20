use serde::{Serialize, Deserialize};
use crate::storage::page::PageId;

pub type Lsn = u64;
pub type TxnId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogRecord {
    Begin {
        txn_id: TxnId,
    },
    Insert {
        txn_id: TxnId,
        table_id: u32,
        page_id: PageId,
        slot: u16,
        data: Vec<u8>,
    },
    Update {
        txn_id: TxnId,
        table_id: u32,
        page_id: PageId,
        slot: u16,
        old_data: Vec<u8>,
        new_data: Vec<u8>,
    },
    Delete {
        txn_id: TxnId,
        table_id: u32,
        page_id: PageId,
        slot: u16,
        old_data: Vec<u8>,
    },
    Commit {
        txn_id: TxnId,
    },
    Abort {
        txn_id: TxnId,
    },
    Checkpoint {
        active_txns: Vec<TxnId>,
    },
}

impl LogRecord {
    pub fn txn_id(&self) -> Option<TxnId> {
        match self {
            LogRecord::Begin { txn_id } => Some(*txn_id),
            LogRecord::Insert { txn_id, .. } => Some(*txn_id),
            LogRecord::Update { txn_id, .. } => Some(*txn_id),
            LogRecord::Delete { txn_id, .. } => Some(*txn_id),
            LogRecord::Commit { txn_id } => Some(*txn_id),
            LogRecord::Abort { txn_id } => Some(*txn_id),
            LogRecord::Checkpoint { .. } => None,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    pub fn deserialize(data: &[u8]) -> Option<Self> {
        serde_json::from_slice(data).ok()
    }
}
