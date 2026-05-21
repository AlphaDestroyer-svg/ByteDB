use std::collections::BTreeSet;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{CoreError, Result};

const SERVER_META_MAGIC: [u8; 4] = *b"BSRV";
const SERVER_META_VERSION: u32 = 1;

pub struct DatabaseRegistry {
    root: PathBuf,
    databases: parking_lot::RwLock<BTreeSet<String>>,
    default_db: String,
}

impl DatabaseRegistry {

    pub fn open(root: PathBuf, default_db: &str) -> Result<Self> {
        fs::create_dir_all(&root).map_err(CoreError::Io)?;
        fs::create_dir_all(root.join("databases")).map_err(CoreError::Io)?;

        let meta_path = root.join("server.meta");
        let mut databases = if meta_path.exists() {
            Self::read_meta(&meta_path)?
        } else {
            BTreeSet::new()
        };
        databases.insert(default_db.to_string());

        let registry = DatabaseRegistry {
            root,
            databases: parking_lot::RwLock::new(databases),
            default_db: default_db.to_string(),
        };

        registry.ensure_db_dir(default_db)?;
        registry.write_meta()?;
        Ok(registry)
    }

    pub fn root(&self) -> &Path { &self.root }
    pub fn default_db(&self) -> &str { &self.default_db }

    pub fn list(&self) -> Vec<String> {
        self.databases.read().iter().cloned().collect()
    }

    pub fn contains(&self, name: &str) -> bool {
        self.databases.read().contains(name)
    }

    pub fn db_dir(&self, name: &str) -> PathBuf {
        self.root.join("databases").join(sanitize(name))
    }

    pub fn create(&self, name: &str) -> Result<bool> {
        if name.is_empty() || name.len() > 128 {
            return Err(CoreError::Internal(format!(
                "invalid database name '{}': must be 1..=128 chars",
                name
            )));
        }
        let mut dbs = self.databases.write();
        if dbs.contains(name) {
            return Ok(false);
        }
        let dir = self.root.join("databases").join(sanitize(name));
        fs::create_dir_all(dir.join("tables")).map_err(CoreError::Io)?;
        dbs.insert(name.to_string());
        drop(dbs);
        self.write_meta()?;
        Ok(true)
    }

    pub fn drop_db(&self, name: &str) -> Result<bool> {
        if name == self.default_db {
            return Err(CoreError::Internal(format!(
                "cannot drop default database '{}'",
                name
            )));
        }
        let mut dbs = self.databases.write();
        if !dbs.remove(name) {
            return Ok(false);
        }
        let dir = self.root.join("databases").join(sanitize(name));
        if dir.exists() {
            fs::remove_dir_all(&dir).map_err(CoreError::Io)?;
        }
        drop(dbs);
        self.write_meta()?;
        Ok(true)
    }

    fn ensure_db_dir(&self, name: &str) -> Result<()> {
        let dir = self.db_dir(name);
        fs::create_dir_all(dir.join("tables")).map_err(CoreError::Io)?;
        Ok(())
    }

    fn read_meta(path: &Path) -> Result<BTreeSet<String>> {
        let mut file = fs::File::open(path).map_err(CoreError::Io)?;
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic).map_err(CoreError::Io)?;
        if magic != SERVER_META_MAGIC {
            return Err(CoreError::Internal("server.meta: bad magic".into()));
        }
        let mut buf4 = [0u8; 4];
        file.read_exact(&mut buf4).map_err(CoreError::Io)?;
        let version = u32::from_le_bytes(buf4);
        if version > SERVER_META_VERSION {
            return Err(CoreError::Internal(format!(
                "server.meta: unsupported version {}",
                version
            )));
        }
        file.read_exact(&mut buf4).map_err(CoreError::Io)?;
        let count = u32::from_le_bytes(buf4) as usize;
        let mut out = BTreeSet::new();
        for _ in 0..count {
            file.read_exact(&mut buf4).map_err(CoreError::Io)?;
            let nl = u32::from_le_bytes(buf4) as usize;
            let mut nb = vec![0u8; nl];
            file.read_exact(&mut nb).map_err(CoreError::Io)?;
            let n = String::from_utf8(nb)
                .map_err(|e| CoreError::Internal(e.to_string()))?;
            out.insert(n);
        }
        Ok(out)
    }

    fn write_meta(&self) -> Result<()> {
        let dbs = self.databases.read();
        let path = self.root.join("server.meta");
        let tmp = path.with_extension("tmp");
        let mut file = fs::File::create(&tmp).map_err(CoreError::Io)?;
        file.write_all(&SERVER_META_MAGIC).map_err(CoreError::Io)?;
        file.write_all(&SERVER_META_VERSION.to_le_bytes()).map_err(CoreError::Io)?;
        file.write_all(&(dbs.len() as u32).to_le_bytes()).map_err(CoreError::Io)?;
        for n in dbs.iter() {
            let nb = n.as_bytes();
            file.write_all(&(nb.len() as u32).to_le_bytes()).map_err(CoreError::Io)?;
            file.write_all(nb).map_err(CoreError::Io)?;
        }
        file.sync_all().map_err(CoreError::Io)?;
        drop(file);
        fs::rename(&tmp, &path).map_err(CoreError::Io)?;
        Ok(())
    }
}

fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' | '.' => c,
            _ => '_',
        })
        .collect()
}
