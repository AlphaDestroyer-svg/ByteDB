use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;

use bytedb_core::dbstore::{DbCatalog, DatabaseRegistry, IndexDef, TableCatalog, TableFile};
use bytedb_core::tuple::schema::Schema;
use bytedb_core::error::Result as CoreResult;

pub struct DiskStore {
    registry: Arc<DatabaseRegistry>,

    catalog: RwLock<DbCatalog>,
    current_db: RwLock<String>,
}

impl DiskStore {
    pub fn open(root: PathBuf, default_db: &str) -> CoreResult<Arc<Self>> {
        let registry = Arc::new(DatabaseRegistry::open(root, default_db)?);
        let catalog = DbCatalog::load(&registry.db_dir(default_db))?;
        Ok(Arc::new(DiskStore {
            registry,
            catalog: RwLock::new(catalog),
            current_db: RwLock::new(default_db.to_string()),
        }))
    }

    pub fn registry(&self) -> &DatabaseRegistry { &self.registry }
    pub fn current_db(&self) -> String { self.current_db.read().clone() }
    pub fn db_dir(&self) -> PathBuf {
        self.registry.db_dir(&self.current_db.read())
    }

    pub fn list_tables(&self) -> Vec<TableCatalog> {
        self.catalog.read().tables.clone()
    }

    pub fn create_database(&self, name: &str) -> CoreResult<bool> {
        self.registry.create(name)
    }

    pub fn drop_database(&self, name: &str) -> CoreResult<bool> {
        self.registry.drop_db(name)
    }

    pub fn switch_database(&self, name: &str) -> CoreResult<DbCatalog> {
        if !self.registry.contains(name) {
            return Err(bytedb_core::error::CoreError::Internal(
                format!("Database '{}' not found", name),
            ));
        }
        let cat = DbCatalog::load(&self.registry.db_dir(name))?;
        *self.current_db.write() = name.to_string();
        *self.catalog.write() = cat.clone();
        Ok(cat)
    }

    pub fn upsert_table(
        &self,
        name: &str,
        table_id: u32,
        schema: &Schema,
        sequences: Vec<(String, i64)>,
    ) -> CoreResult<()> {
        let mut cat = self.catalog.write();
        let indexes = cat
            .tables
            .iter()
            .find(|t| t.name == name)
            .map(|t| t.indexes.clone())
            .unwrap_or_default();
        let entry = TableCatalog {
            name: name.to_string(),
            table_id,
            schema: schema.clone(),
            sequences,
            indexes,
        };
        cat.upsert(entry);
        cat.save(&self.db_dir())?;
        Ok(())
    }

    pub fn upsert_table_indexes(&self, table: &str, indexes: Vec<IndexDef>) -> CoreResult<()> {
        let mut cat = self.catalog.write();
        if let Some(t) = cat.tables.iter_mut().find(|t| t.name == table) {
            t.indexes = indexes;
            cat.save(&self.db_dir())?;
        }
        Ok(())
    }

    pub fn drop_table(&self, name: &str) -> CoreResult<()> {
        let mut cat = self.catalog.write();
        let removed = cat.remove(name);
        if removed {
            cat.save(&self.db_dir())?;
            TableFile::delete(&self.db_dir(), name)?;
        }
        Ok(())
    }

    pub fn flush_table_data(
        &self,
        table: &str,
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> CoreResult<()> {
        TableFile::save(&self.db_dir(), table, entries)?;
        Ok(())
    }

    pub fn flush_table_sequences(
        &self,
        table: &str,
        sequences: Vec<(String, i64)>,
    ) -> CoreResult<()> {
        let mut cat = self.catalog.write();
        if let Some(t) = cat.tables.iter_mut().find(|t| t.name == table) {
            t.sequences = sequences;
            cat.save(&self.db_dir())?;
        }
        Ok(())
    }

    pub fn load_table_data(&self, table: &str) -> CoreResult<Vec<(Vec<u8>, Vec<u8>)>> {
        TableFile::load(&self.db_dir(), table)
    }
}
