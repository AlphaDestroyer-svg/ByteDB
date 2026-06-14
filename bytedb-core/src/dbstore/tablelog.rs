use std::fs;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{CoreError, Result};

const LOG_MAGIC: [u8; 4] = *b"BTLG";
const LOG_VERSION: u32 = 1;

pub const OP_PUT: u8 = 0;
pub const OP_DEL: u8 = 1;

pub struct LogDelta {
    pub op: u8,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub struct TableLog;

impl TableLog {
    pub fn path(db_dir: &Path, table_name: &str) -> PathBuf {
        db_dir.join("tables").join(format!("{}.log", sanitize(table_name)))
    }

    pub fn append(db_dir: &Path, table_name: &str, deltas: &[(u8, Vec<u8>, Vec<u8>)]) -> Result<()> {
        if deltas.is_empty() {
            return Ok(());
        }
        let dir = db_dir.join("tables");
        fs::create_dir_all(&dir).map_err(CoreError::Io)?;
        let path = Self::path(db_dir, table_name);
        let fresh = !path.exists() || fs::metadata(&path).map(|m| m.len() == 0).unwrap_or(true);

        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(CoreError::Io)?;

        let mut buf = Vec::new();
        if fresh {
            buf.extend_from_slice(&LOG_MAGIC);
            buf.extend_from_slice(&LOG_VERSION.to_le_bytes());
        }
        for (op, key, value) in deltas {
            let mut body = Vec::with_capacity(9 + key.len() + value.len());
            body.push(*op);
            body.extend_from_slice(&(key.len() as u32).to_le_bytes());
            body.extend_from_slice(key);
            body.extend_from_slice(&(value.len() as u32).to_le_bytes());
            body.extend_from_slice(value);
            let crc = crc32fast::hash(&body);
            buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
            buf.extend_from_slice(&crc.to_le_bytes());
            buf.extend_from_slice(&body);
        }
        file.write_all(&buf).map_err(CoreError::Io)?;
        file.sync_all().map_err(CoreError::Io)?;
        Ok(())
    }

    pub fn load(db_dir: &Path, table_name: &str) -> Result<Vec<LogDelta>> {
        let path = Self::path(db_dir, table_name);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = fs::File::open(&path).map_err(CoreError::Io)?;
        let mut reader = BufReader::new(file);
        let mut all = Vec::new();
        reader.read_to_end(&mut all).map_err(CoreError::Io)?;

        if all.len() < 8 {
            return Ok(Vec::new());
        }
        if all[0..4] != LOG_MAGIC {
            return Err(CoreError::Internal(format!("table log {:?}: bad magic", path)));
        }
        let version = u32::from_le_bytes([all[4], all[5], all[6], all[7]]);
        if version != LOG_VERSION {
            return Err(CoreError::Internal(format!(
                "table log {:?}: unsupported version {}",
                path, version
            )));
        }

        let mut cur = 8usize;
        let mut out = Vec::new();
        while cur + 8 <= all.len() {
            let len = u32::from_le_bytes([all[cur], all[cur + 1], all[cur + 2], all[cur + 3]]) as usize;
            let crc = u32::from_le_bytes([all[cur + 4], all[cur + 5], all[cur + 6], all[cur + 7]]);
            let body_start = cur + 8;
            if body_start + len > all.len() {
                break;
            }
            let body = &all[body_start..body_start + len];
            if crc32fast::hash(body) != crc {
                break;
            }
            if let Some(delta) = decode_body(body) {
                out.push(delta);
            } else {
                break;
            }
            cur = body_start + len;
        }
        Ok(out)
    }

    pub fn truncate(db_dir: &Path, table_name: &str) -> Result<()> {
        let path = Self::path(db_dir, table_name);
        if path.exists() {
            fs::remove_file(&path).map_err(CoreError::Io)?;
        }
        Ok(())
    }

    pub fn size_bytes(db_dir: &Path, table_name: &str) -> u64 {
        fs::metadata(Self::path(db_dir, table_name)).map(|m| m.len()).unwrap_or(0)
    }

    pub fn delete(db_dir: &Path, table_name: &str) -> Result<()> {
        Self::truncate(db_dir, table_name)
    }

    pub fn fold(base: Vec<(Vec<u8>, Vec<u8>)>, deltas: Vec<LogDelta>) -> Vec<(Vec<u8>, Vec<u8>)> {
        if deltas.is_empty() {
            return base;
        }
        let mut map: std::collections::HashMap<Vec<u8>, Vec<u8>, ahash::RandomState> =
            std::collections::HashMap::with_capacity_and_hasher(base.len(), ahash::RandomState::new());
        for (k, v) in base {
            map.insert(k, v);
        }
        for d in deltas {
            match d.op {
                OP_DEL => {
                    map.remove(&d.key);
                }
                _ => {
                    map.insert(d.key, d.value);
                }
            }
        }
        map.into_iter().collect()
    }
}

fn decode_body(body: &[u8]) -> Option<LogDelta> {
    if body.is_empty() {
        return None;
    }
    let op = body[0];
    let mut cur = 1usize;
    if cur + 4 > body.len() {
        return None;
    }
    let kl = u32::from_le_bytes([body[cur], body[cur + 1], body[cur + 2], body[cur + 3]]) as usize;
    cur += 4;
    if cur + kl > body.len() {
        return None;
    }
    let key = body[cur..cur + kl].to_vec();
    cur += kl;
    if cur + 4 > body.len() {
        return None;
    }
    let vl = u32::from_le_bytes([body[cur], body[cur + 1], body[cur + 2], body[cur + 3]]) as usize;
    cur += 4;
    if cur + vl > body.len() {
        return None;
    }
    let value = body[cur..cur + vl].to_vec();
    Some(LogDelta { op, key, value })
}

fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' | '.' => c,
            _ => '_',
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> PathBuf {
        let mut d = std::env::temp_dir();
        d.push(format!("bytedb_tablelog_{}_{}", tag, std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    fn put(k: &[u8], v: &[u8]) -> (u8, Vec<u8>, Vec<u8>) {
        (OP_PUT, k.to_vec(), v.to_vec())
    }
    fn del(k: &[u8]) -> (u8, Vec<u8>, Vec<u8>) {
        (OP_DEL, k.to_vec(), Vec::new())
    }

    #[test]
    fn append_and_load_roundtrip() {
        let d = tmp("rt");
        TableLog::append(&d, "t", &[put(b"a", b"1"), put(b"b", b"2")]).unwrap();
        TableLog::append(&d, "t", &[del(b"a"), put(b"c", b"3")]).unwrap();
        let deltas = TableLog::load(&d, "t").unwrap();
        assert_eq!(deltas.len(), 4);
        assert_eq!(deltas[0].op, OP_PUT);
        assert_eq!(deltas[2].op, OP_DEL);
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn fold_applies_put_and_del() {
        let d = tmp("fold");
        let base = vec![(b"a".to_vec(), b"1".to_vec()), (b"b".to_vec(), b"2".to_vec())];
        TableLog::append(&d, "t", &[put(b"b", b"22"), del(b"a"), put(b"c", b"3")]).unwrap();
        let deltas = TableLog::load(&d, "t").unwrap();
        let mut folded = TableLog::fold(base, deltas);
        folded.sort();
        assert_eq!(folded, vec![(b"b".to_vec(), b"22".to_vec()), (b"c".to_vec(), b"3".to_vec())]);
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn torn_tail_is_truncated() {
        let d = tmp("torn");
        TableLog::append(&d, "t", &[put(b"a", b"1"), put(b"b", b"2")]).unwrap();
        let path = TableLog::path(&d, "t");
        let mut bytes = fs::read(&path).unwrap();
        bytes.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        fs::write(&path, &bytes).unwrap();
        let deltas = TableLog::load(&d, "t").unwrap();
        assert_eq!(deltas.len(), 2);
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn truncate_clears_log() {
        let d = tmp("trunc");
        TableLog::append(&d, "t", &[put(b"a", b"1")]).unwrap();
        TableLog::truncate(&d, "t").unwrap();
        assert!(TableLog::load(&d, "t").unwrap().is_empty());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn missing_log_loads_empty() {
        let d = tmp("missing");
        assert!(TableLog::load(&d, "nope").unwrap().is_empty());
    }
}
