use std::sync::Arc;

use crate::error::{CoreError, Result};
use crate::index::btree::BPlusTree;
use crate::index::order_key::encode_okey;
use crate::tuple::value::Value;

pub struct SecondaryIndex {
    pub name: String,
    pub columns: Vec<usize>,
    pub unique: bool,
    tree: Arc<BPlusTree>,
}

impl SecondaryIndex {
    pub fn new(name: impl Into<String>, columns: Vec<usize>, unique: bool) -> Self {
        let name = name.into();
        SecondaryIndex {
            tree: Arc::new(BPlusTree::new(format!("{}_sec", name), 128)),
            name,
            columns,
            unique,
        }
    }

    fn key_prefix(&self, col_values: &[&Value]) -> Vec<u8> {
        encode_okey(col_values)
    }

    fn composite(&self, col_values: &[&Value], pk: &[u8]) -> Vec<u8> {
        let mut k = self.key_prefix(col_values);
        k.extend_from_slice(pk);
        k
    }

    pub fn insert(&self, col_values: &[&Value], pk: &[u8]) -> Result<()> {
        if self.unique {
            let prefix = self.key_prefix(col_values);
            let existing = self.tree.prefix_scan(&prefix)?;
            if existing.iter().any(|(_, v)| v.as_slice() != pk) {
                return Err(CoreError::Internal(format!(
                    "unique index {} violated",
                    self.name
                )));
            }
        }
        let key = self.composite(col_values, pk);
        self.tree.insert(key, pk.to_vec())
    }

    pub fn remove(&self, col_values: &[&Value], pk: &[u8]) -> Result<()> {
        let key = self.composite(col_values, pk);
        self.tree.delete(&key)?;
        Ok(())
    }

    pub fn lookup_eq(&self, col_values: &[&Value]) -> Result<Vec<Vec<u8>>> {
        let prefix = self.key_prefix(col_values);
        let entries = self.tree.prefix_scan(&prefix)?;
        Ok(entries.into_iter().map(|(_, v)| v).collect())
    }

    pub fn lookup_range(&self, lo: Option<&Value>, hi: Option<&Value>) -> Result<Vec<Vec<u8>>> {
        let start = match lo {
            Some(v) => encode_okey(&[v]),
            None => Vec::new(),
        };
        let end = match hi {
            Some(v) => prefix_successor(&encode_okey(&[v])).unwrap_or_else(|| vec![0xFF]),
            None => vec![0xFF],
        };
        let entries = self.tree.range_scan(&start, &end)?;
        Ok(entries.into_iter().map(|(_, v)| v).collect())
    }

    pub fn len(&self) -> usize {
        self.tree.approx_len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(&last) = out.last() {
        if last == 0xFF {
            out.pop();
        } else {
            *out.last_mut().unwrap() = last + 1;
            return Some(out);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn val(n: i64) -> Value {
        Value::Int64(n)
    }

    #[test]
    fn equality_lookup_returns_matching_pks() {
        let idx = SecondaryIndex::new("ix", vec![1], false);
        idx.insert(&[&val(10)], b"pk1").unwrap();
        idx.insert(&[&val(10)], b"pk2").unwrap();
        idx.insert(&[&val(20)], b"pk3").unwrap();

        let mut got = idx.lookup_eq(&[&val(10)]).unwrap();
        got.sort();
        assert_eq!(got, vec![b"pk1".to_vec(), b"pk2".to_vec()]);
        assert_eq!(idx.lookup_eq(&[&val(20)]).unwrap(), vec![b"pk3".to_vec()]);
        assert!(idx.lookup_eq(&[&val(99)]).unwrap().is_empty());
    }

    #[test]
    fn range_lookup_is_numeric() {
        let idx = SecondaryIndex::new("ix", vec![1], false);
        for n in [1i64, 5, 10, 100, 256, 1000] {
            idx.insert(&[&val(n)], format!("pk{}", n).as_bytes()).unwrap();
        }
        let got = idx.lookup_range(Some(&val(5)), Some(&val(256))).unwrap();
        assert!(got.contains(&b"pk5".to_vec()));
        assert!(got.contains(&b"pk100".to_vec()));
        assert!(got.contains(&b"pk256".to_vec()));
        assert!(!got.contains(&b"pk1".to_vec()));
        assert!(!got.contains(&b"pk1000".to_vec()));
    }

    #[test]
    fn unique_rejects_distinct_pk_same_value() {
        let idx = SecondaryIndex::new("ix", vec![1], true);
        idx.insert(&[&val(10)], b"pk1").unwrap();
        assert!(idx.insert(&[&val(10)], b"pk2").is_err());
        idx.insert(&[&val(10)], b"pk1").unwrap();
    }

    #[test]
    fn remove_then_lookup_empty() {
        let idx = SecondaryIndex::new("ix", vec![1], false);
        idx.insert(&[&val(10)], b"pk1").unwrap();
        idx.remove(&[&val(10)], b"pk1").unwrap();
        assert!(idx.lookup_eq(&[&val(10)]).unwrap().is_empty());
    }
}
