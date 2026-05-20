use std::collections::HashMap;
use parking_lot::RwLock;

use crate::error::{CoreError, Result};
use super::table::TableMeta;

pub struct Database {
    name: String,
    tables: RwLock<HashMap<String, TableMeta>>,
}

impl Database {
    pub fn new(name: impl Into<String>) -> Self {
        Database {
            name: name.into(),
            tables: RwLock::new(HashMap::new()),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn create_table(&self, meta: TableMeta) -> Result<()> {
        let mut tables = self.tables.write();
        if tables.contains_key(&meta.name) {
            return Err(CoreError::TableAlreadyExists(meta.name.clone()));
        }
        tables.insert(meta.name.clone(), meta);
        Ok(())
    }

    pub fn drop_table(&self, name: &str) -> Result<()> {
        let mut tables = self.tables.write();
        if tables.remove(name).is_none() {
            return Err(CoreError::TableNotFound(name.into()));
        }
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Result<TableMeta> {
        let tables = self.tables.read();
        tables.get(name)
            .cloned()
            .ok_or_else(|| CoreError::TableNotFound(name.into()))
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.read().contains_key(name)
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }
}
