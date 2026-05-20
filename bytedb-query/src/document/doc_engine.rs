use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::RwLock;
use serde_json::Value as JsonValue;

use crate::error::{QueryError, Result};
use super::json_path::evaluate_path;

pub struct DocEngine {
    collections: RwLock<HashMap<String, Vec<Document>>>,
    next_id: AtomicU64,
}

#[derive(Debug, Clone)]
struct Document {
    _id: String,
    data: JsonValue,
}

impl DocEngine {
    pub fn new() -> Self {
        DocEngine {
            collections: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    pub fn insert(&self, collection: &str, doc_json: &str) -> Result<String> {
        let mut data: JsonValue = serde_json::from_str(doc_json)
            .map_err(|e| QueryError::Execution(format!("Invalid JSON: {}", e)))?;

        let id = format!("doc_{}", self.next_id.fetch_add(1, Ordering::SeqCst));

        if let Some(obj) = data.as_object_mut() {
            obj.insert("_id".to_string(), JsonValue::String(id.clone()));
        }

        let doc = Document { _id: id.clone(), data };
        self.collections.write()
            .entry(collection.to_string())
            .or_insert_with(Vec::new)
            .push(doc);

        Ok(id)
    }

    pub fn find(&self, collection: &str, filter: Option<&dyn Fn(&JsonValue) -> bool>) -> Result<Vec<JsonValue>> {
        let collections = self.collections.read();
        let docs = collections.get(collection);

        match docs {
            Some(docs) => {
                let results: Vec<JsonValue> = docs.iter()
                    .filter(|d| {
                        if let Some(f) = filter {
                            f(&d.data)
                        } else {
                            true
                        }
                    })
                    .map(|d| d.data.clone())
                    .collect();
                Ok(results)
            }
            None => Ok(Vec::new()),
        }
    }

    pub fn find_by_path(&self, collection: &str, path: &str, expected: &JsonValue) -> Result<Vec<JsonValue>> {
        let collections = self.collections.read();
        let docs = collections.get(collection);

        match docs {
            Some(docs) => {
                let results: Vec<JsonValue> = docs.iter()
                    .filter(|d| {
                        if let Some(val) = evaluate_path(&d.data, path) {
                            &val == expected
                        } else {
                            false
                        }
                    })
                    .map(|d| d.data.clone())
                    .collect();
                Ok(results)
            }
            None => Ok(Vec::new()),
        }
    }

    pub fn update_by_path(&self, collection: &str, filter_path: &str, filter_value: &JsonValue, set_path: &str, set_value: JsonValue) -> Result<u64> {
        let mut collections = self.collections.write();
        let docs = collections.get_mut(collection);

        match docs {
            Some(docs) => {
                let mut count = 0u64;
                for doc in docs.iter_mut() {
                    if let Some(val) = evaluate_path(&doc.data, filter_path) {
                        if &val == filter_value {
                            if let Some(obj) = doc.data.as_object_mut() {
                                obj.insert(set_path.to_string(), set_value.clone());
                                count += 1;
                            }
                        }
                    }
                }
                Ok(count)
            }
            None => Ok(0),
        }
    }

    pub fn delete_by_path(&self, collection: &str, path: &str, expected: &JsonValue) -> Result<u64> {
        let mut collections = self.collections.write();
        let docs = collections.get_mut(collection);

        match docs {
            Some(docs) => {
                let before = docs.len();
                docs.retain(|d| {
                    if let Some(val) = evaluate_path(&d.data, path) {
                        &val != expected
                    } else {
                        true
                    }
                });
                Ok((before - docs.len()) as u64)
            }
            None => Ok(0),
        }
    }

    pub fn find_all(&self, collection: &str) -> Result<Vec<JsonValue>> {
        self.find(collection, None)
    }

    pub fn count(&self, collection: &str) -> usize {
        self.collections.read()
            .get(collection)
            .map(|d| d.len())
            .unwrap_or(0)
    }

    pub fn list_collections(&self) -> Vec<String> {
        self.collections.read().keys().cloned().collect()
    }
}

impl Default for DocEngine {
    fn default() -> Self {
        Self::new()
    }
}
