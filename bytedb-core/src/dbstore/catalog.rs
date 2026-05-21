use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{CoreError, Result};
use crate::tuple::schema::Schema;

const CATALOG_MAGIC: [u8; 4] = *b"BCAT";
const CATALOG_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCatalog {
    pub name: String,
    pub table_id: u32,
    pub schema: Schema,

    #[serde(default)]
    pub sequences: Vec<(String, i64)>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DbCatalog {
    pub tables: Vec<TableCatalog>,
}

impl DbCatalog {
    pub fn empty() -> Self { DbCatalog { tables: Vec::new() } }

    pub fn path(db_dir: &Path) -> PathBuf {
        db_dir.join("catalog.bin")
    }

    pub fn load(db_dir: &Path) -> Result<Self> {
        let path = Self::path(db_dir);
        if !path.exists() {
            return Ok(DbCatalog::empty());
        }
        let mut file = fs::File::open(&path).map_err(CoreError::Io)?;
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic).map_err(CoreError::Io)?;
        if magic != CATALOG_MAGIC {
            return Err(CoreError::Internal(format!(
                "catalog at {:?}: bad magic", path
            )));
        }
        let mut buf4 = [0u8; 4];
        file.read_exact(&mut buf4).map_err(CoreError::Io)?;
        let version = u32::from_le_bytes(buf4);
        if version > CATALOG_VERSION {
            return Err(CoreError::Internal(format!(
                "catalog at {:?}: unsupported version {}", path, version
            )));
        }
        file.read_exact(&mut buf4).map_err(CoreError::Io)?;
        let payload_len = u32::from_le_bytes(buf4) as usize;
        let mut buf = vec![0u8; payload_len];
        file.read_exact(&mut buf).map_err(CoreError::Io)?;
        let cat: DbCatalog = serde_json::from_slice(&buf)
            .map_err(|e| CoreError::Internal(format!("catalog parse: {}", e)))?;
        Ok(cat)
    }

    pub fn save(&self, db_dir: &Path) -> Result<()> {
        fs::create_dir_all(db_dir).map_err(CoreError::Io)?;
        let path = Self::path(db_dir);
        let tmp = path.with_extension("tmp");
        let payload = serde_json::to_vec(self)
            .map_err(|e| CoreError::Internal(format!("catalog encode: {}", e)))?;
        let mut file = fs::File::create(&tmp).map_err(CoreError::Io)?;
        file.write_all(&CATALOG_MAGIC).map_err(CoreError::Io)?;
        file.write_all(&CATALOG_VERSION.to_le_bytes()).map_err(CoreError::Io)?;
        file.write_all(&(payload.len() as u32).to_le_bytes()).map_err(CoreError::Io)?;
        file.write_all(&payload).map_err(CoreError::Io)?;
        file.sync_all().map_err(CoreError::Io)?;
        drop(file);
        fs::rename(&tmp, &path).map_err(CoreError::Io)?;
        Ok(())
    }

    pub fn upsert(&mut self, t: TableCatalog) {
        if let Some(slot) = self.tables.iter_mut().find(|x| x.name == t.name) {
            *slot = t;
        } else {
            self.tables.push(t);
        }
    }

    pub fn remove(&mut self, name: &str) -> bool {
        let before = self.tables.len();
        self.tables.retain(|t| t.name != name);
        self.tables.len() != before
    }
}
