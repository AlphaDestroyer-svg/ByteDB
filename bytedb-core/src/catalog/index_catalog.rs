use std::collections::HashMap;
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMeta {
    pub name: String,
    pub index_id: u32,
    pub table_name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

pub struct IndexCatalog {
    indexes: RwLock<HashMap<String, IndexMeta>>,
    next_id: std::sync::atomic::AtomicU32,
}

impl IndexCatalog {
    pub fn new() -> Self {
        IndexCatalog {
            indexes: RwLock::new(HashMap::new()),
            next_id: std::sync::atomic::AtomicU32::new(1),
        }
    }

    pub fn create_index(&self, name: String, table_name: String, columns: Vec<String>, unique: bool) -> IndexMeta {
        let index_id = self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let meta = IndexMeta {
            name: name.clone(),
            index_id,
            table_name,
            columns,
            unique,
        };
        self.indexes.write().insert(name, meta.clone());
        meta
    }

    pub fn get_index(&self, name: &str) -> Option<IndexMeta> {
        self.indexes.read().get(name).cloned()
    }

    pub fn get_table_indexes(&self, table_name: &str) -> Vec<IndexMeta> {
        self.indexes.read()
            .values()
            .filter(|idx| idx.table_name == table_name)
            .cloned()
            .collect()
    }

    pub fn drop_index(&self, name: &str) -> bool {
        self.indexes.write().remove(name).is_some()
    }
}

impl Default for IndexCatalog {
    fn default() -> Self {
        Self::new()
    }
}
