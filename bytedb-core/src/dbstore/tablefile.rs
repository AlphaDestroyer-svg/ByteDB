use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{CoreError, Result};

const TABLE_MAGIC: [u8; 4] = *b"BTBL";
const TABLE_VERSION: u32 = 2;
const TABLE_VERSION_LEGACY: u32 = 1;

pub struct TableFile;

impl TableFile {
    pub fn path(db_dir: &Path, table_name: &str) -> PathBuf {
        db_dir.join("tables").join(format!("{}.tbl", sanitize(table_name)))
    }

    pub fn load(db_dir: &Path, table_name: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let path = Self::path(db_dir, table_name);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = fs::File::open(&path).map_err(CoreError::Io)?;
        let mut reader = BufReader::new(file);

        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic).map_err(CoreError::Io)?;
        if magic != TABLE_MAGIC {
            return Err(CoreError::Internal(format!(
                "table file {:?}: bad magic",
                path
            )));
        }
        let mut buf4 = [0u8; 4];
        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf4).map_err(CoreError::Io)?;
        let version = u32::from_le_bytes(buf4);
        if version != TABLE_VERSION && version != TABLE_VERSION_LEGACY {
            return Err(CoreError::Internal(format!(
                "table file {:?}: unsupported version {}",
                path, version
            )));
        }

        let mut payload = Vec::new();
        reader.read_to_end(&mut payload).map_err(CoreError::Io)?;

        if version == TABLE_VERSION {
            if payload.len() < 4 {
                return Err(CoreError::Internal(format!(
                    "table file {:?}: truncated trailer",
                    path
                )));
            }
            let trailer_pos = payload.len() - 4;
            let stored = u32::from_le_bytes([
                payload[trailer_pos], payload[trailer_pos + 1],
                payload[trailer_pos + 2], payload[trailer_pos + 3],
            ]);
            payload.truncate(trailer_pos);
            let actual = crc32fast::hash(&payload);
            if stored != actual {
                return Err(CoreError::Internal(format!(
                    "table file {:?}: checksum mismatch (corruption)",
                    path
                )));
            }
        }

        let mut cur = 0usize;
        if payload.len() < cur + 8 {
            return Err(CoreError::Internal(format!("table file {:?}: truncated", path)));
        }
        buf8.copy_from_slice(&payload[cur..cur + 8]);
        cur += 8;
        let count = u64::from_le_bytes(buf8) as usize;

        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            if payload.len() < cur + 4 {
                return Err(CoreError::Internal(format!("table file {:?}: truncated", path)));
            }
            buf4.copy_from_slice(&payload[cur..cur + 4]);
            cur += 4;
            let kl = u32::from_le_bytes(buf4) as usize;
            if payload.len() < cur + kl {
                return Err(CoreError::Internal(format!("table file {:?}: truncated", path)));
            }
            let k = payload[cur..cur + kl].to_vec();
            cur += kl;

            if payload.len() < cur + 4 {
                return Err(CoreError::Internal(format!("table file {:?}: truncated", path)));
            }
            buf4.copy_from_slice(&payload[cur..cur + 4]);
            cur += 4;
            let vl = u32::from_le_bytes(buf4) as usize;
            if payload.len() < cur + vl {
                return Err(CoreError::Internal(format!("table file {:?}: truncated", path)));
            }
            let v = payload[cur..cur + vl].to_vec();
            cur += vl;

            out.push((k, v));
        }
        Ok(out)
    }

    pub fn save(
        db_dir: &Path,
        table_name: &str,
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<PathBuf> {
        let dir = db_dir.join("tables");
        fs::create_dir_all(&dir).map_err(CoreError::Io)?;
        let path = Self::path(db_dir, table_name);
        let tmp = path.with_extension("tbl.tmp");
        let file = fs::File::create(&tmp).map_err(CoreError::Io)?;
        let mut writer = BufWriter::new(file);

        writer.write_all(&TABLE_MAGIC).map_err(CoreError::Io)?;
        writer.write_all(&TABLE_VERSION.to_le_bytes()).map_err(CoreError::Io)?;

        let mut payload = Vec::new();
        payload.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        for (k, v) in entries {
            payload.extend_from_slice(&(k.len() as u32).to_le_bytes());
            payload.extend_from_slice(k);
            payload.extend_from_slice(&(v.len() as u32).to_le_bytes());
            payload.extend_from_slice(v);
        }
        let checksum = crc32fast::hash(&payload);
        writer.write_all(&payload).map_err(CoreError::Io)?;
        writer.write_all(&checksum.to_le_bytes()).map_err(CoreError::Io)?;
        writer.flush().map_err(CoreError::Io)?;
        let inner = writer.into_inner()
            .map_err(|e| CoreError::Internal(format!("flush table: {}", e)))?;
        inner.sync_all().map_err(CoreError::Io)?;
        fs::rename(&tmp, &path).map_err(CoreError::Io)?;
        Ok(path)
    }

    pub fn delete(db_dir: &Path, table_name: &str) -> Result<()> {
        let path = Self::path(db_dir, table_name);
        if path.exists() {
            fs::remove_file(&path).map_err(CoreError::Io)?;
        }
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
