use serde::{Serialize, Deserialize};
use crate::tuple::schema::Schema;

pub const SNAPSHOT_MAGIC: [u8; 4] = *b"BSDB";
pub const SNAPSHOT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotFormat {
    Binary,
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotHeader {
    pub version: u32,
    pub lsn: u64,
    pub timestamp: i64,
    pub table_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSnapshot {
    pub name: String,
    pub table_id: u32,
    pub schema: Schema,
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullSnapshot {
    pub header: SnapshotHeader,
    pub tables: Vec<TableSnapshot>,
}
