use serde::{Serialize, Deserialize};
use bytedb_core::tuple::value::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Authenticate { username: String, password: String },
    Query { sql: String, txn_id: Option<u64> },
    Ping,
    Disconnect,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    AuthOk { session_id: u64 },
    AuthFail { reason: String },
    ResultSet { columns: Vec<String>, rows: Vec<Vec<Value>> },
    Modified { count: u64 },
    Ok { message: String },
    Error { code: u32, message: String },
    Pong,
}

impl Request {
    #[allow(dead_code)]
    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    pub fn deserialize(data: &[u8]) -> Option<Self> {
        serde_json::from_slice(data).ok()
    }
}

impl Response {
    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    #[allow(dead_code)]
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        serde_json::from_slice(data).ok()
    }
}
