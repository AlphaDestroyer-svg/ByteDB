use std::sync::Arc;
use bytedb_core::index::btree::BPlusTree;
use crate::error::{QueryError, Result};

pub struct KvEngine {
    tree: Arc<BPlusTree>,
}

impl KvEngine {
    pub fn new() -> Self {
        KvEngine {
            tree: Arc::new(BPlusTree::new("kv_store", 256)),
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let result = self.tree.search(key.as_bytes())
            .map_err(|e| QueryError::Execution(e.to_string()))?;
        Ok(result.map(|v| String::from_utf8_lossy(&v).to_string()))
    }

    pub fn set(&self, key: &str, value: &str) -> Result<()> {
        self.tree.insert(key.as_bytes().to_vec(), value.as_bytes().to_vec())
            .map_err(|e| QueryError::Execution(e.to_string()))
    }

    pub fn delete(&self, key: &str) -> Result<bool> {
        self.tree.delete(key.as_bytes())
            .map_err(|e| QueryError::Execution(e.to_string()))
    }

    pub fn scan(&self, start: &str, end: &str) -> Result<Vec<(String, String)>> {
        let results = self.tree.range_scan(start.as_bytes(), end.as_bytes())
            .map_err(|e| QueryError::Execution(e.to_string()))?;
        Ok(results.into_iter().map(|(k, v)| {
            (String::from_utf8_lossy(&k).to_string(), String::from_utf8_lossy(&v).to_string())
        }).collect())
    }
}

impl Default for KvEngine {
    fn default() -> Self {
        Self::new()
    }
}
