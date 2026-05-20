use crate::error::Result;
use super::btree::BPlusTree;

pub struct BTreeIterator {
    items: Vec<(Vec<u8>, Vec<u8>)>,
    position: usize,
}

impl BTreeIterator {
    pub fn new(tree: &BPlusTree, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<Self> {
        let items = match (start, end) {
            (Some(s), Some(e)) => tree.range_scan(s, e)?,
            _ => tree.scan_all()?,
        };

        Ok(BTreeIterator {
            items,
            position: 0,
        })
    }

    pub fn has_next(&self) -> bool {
        self.position < self.items.len()
    }
}

impl Iterator for BTreeIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.position < self.items.len() {
            let item = self.items[self.position].clone();
            self.position += 1;
            Some(item)
        } else {
            None
        }
    }
}
