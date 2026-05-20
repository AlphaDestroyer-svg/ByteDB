use std::sync::Arc;
use parking_lot::RwLock;

use crate::error::{CoreError, Result};

pub struct BPlusTree {
    root: Arc<RwLock<BTreeNode>>,
    order: usize,
    name: String,
}

#[derive(Debug, Clone)]
enum BTreeNode {
    Internal(InternalNode),
    Leaf(LeafNode),
}

#[derive(Debug, Clone)]
struct InternalNode {
    keys: Vec<Vec<u8>>,
    children: Vec<Arc<RwLock<BTreeNode>>>,
}

#[derive(Debug, Clone)]
struct LeafNode {
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
    next: Option<Arc<RwLock<BTreeNode>>>,
}

impl BPlusTree {
    pub fn new(name: impl Into<String>, order: usize) -> Self {
        let leaf = BTreeNode::Leaf(LeafNode {
            keys: Vec::new(),
            values: Vec::new(),
            next: None,
        });

        BPlusTree {
            root: Arc::new(RwLock::new(leaf)),
            order,
            name: name.into(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let root = self.root.read();
        match &*root {
            BTreeNode::Leaf(leaf) => {
                if leaf.keys.len() < self.order - 1 {
                    drop(root);
                    self.insert_into_leaf(key, value)?;
                } else {
                    drop(root);
                    self.insert_and_split(key, value)?;
                }
            }
            BTreeNode::Internal(_) => {
                drop(root);
                if let Some((split_key, new_child)) = self.insert_recursive(&self.root, key, value)? {
                    let mut root_w = self.root.write();
                    let old_root = std::mem::replace(&mut *root_w, BTreeNode::Leaf(LeafNode {
                        keys: Vec::new(),
                        values: Vec::new(),
                        next: None,
                    }));
                    let left = Arc::new(RwLock::new(old_root));
                    *root_w = BTreeNode::Internal(InternalNode {
                        keys: vec![split_key],
                        children: vec![left, new_child],
                    });
                }
            }
        }
        Ok(())
    }

    pub fn search(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let node = self.find_leaf(key);
        let node_read = node.read();
        match &*node_read {
            BTreeNode::Leaf(leaf) => {
                for (i, k) in leaf.keys.iter().enumerate() {
                    if k.as_slice() == key {
                        return Ok(Some(leaf.values[i].clone()));
                    }
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let node = self.find_leaf(key);
        let mut node_write = node.write();
        match &mut *node_write {
            BTreeNode::Leaf(leaf) => {
                if let Some(pos) = leaf.keys.iter().position(|k| k.as_slice() == key) {
                    leaf.keys.remove(pos);
                    leaf.values.remove(pos);
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            _ => Ok(false),
        }
    }

    pub fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results = Vec::new();
        let leaf_node = self.find_leaf(start);
        let mut current = Some(leaf_node);

        'outer: while let Some(node_arc) = current {
            let node = node_arc.read();
            match &*node {
                BTreeNode::Leaf(leaf) => {
                    for (i, k) in leaf.keys.iter().enumerate() {
                        if k.as_slice() >= start && k.as_slice() <= end {
                            results.push((k.clone(), leaf.values[i].clone()));
                        } else if k.as_slice() > end {
                            break 'outer;
                        }
                    }
                    current = leaf.next.clone();
                }
                _ => break,
            }
            drop(node);
        }

        Ok(results)
    }

    pub fn scan_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results = Vec::new();
        let leaf_node = self.find_leftmost_leaf();
        let mut current = Some(leaf_node);

        while let Some(node_arc) = current {
            let node = node_arc.read();
            match &*node {
                BTreeNode::Leaf(leaf) => {
                    for (i, k) in leaf.keys.iter().enumerate() {
                        results.push((k.clone(), leaf.values[i].clone()));
                    }
                    current = leaf.next.clone();
                }
                _ => break,
            }
            drop(node);
        }

        Ok(results)
    }

    pub fn for_each<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        let leaf_node = self.find_leftmost_leaf();
        let mut current = Some(leaf_node);

        while let Some(node_arc) = current {
            let node = node_arc.read();
            match &*node {
                BTreeNode::Leaf(leaf) => {
                    for (i, k) in leaf.keys.iter().enumerate() {
                        if !f(k, &leaf.values[i]) {
                            return Ok(());
                        }
                    }
                    current = leaf.next.clone();
                }
                _ => break,
            }
            drop(node);
        }

        Ok(())
    }

    pub fn for_each_batch<F>(&self, batch_size: usize, mut f: F) -> Result<()>
    where
        F: FnMut(&[&[u8]]) -> bool,
    {
        let leaf_node = self.find_leftmost_leaf();
        let mut current = Some(leaf_node);

        let mut owned_batch: Vec<Vec<u8>> = Vec::with_capacity(batch_size);

        while let Some(node_arc) = current {
            let node = node_arc.read();
            match &*node {
                BTreeNode::Leaf(leaf) => {
                    for v in &leaf.values {
                        owned_batch.push(v.clone());
                        if owned_batch.len() >= batch_size {
                            let refs: Vec<&[u8]> = owned_batch.iter().map(|b| b.as_slice()).collect();
                            if !f(&refs) {
                                return Ok(());
                            }
                            owned_batch.clear();
                        }
                    }
                    current = leaf.next.clone();
                }
                _ => break,
            }
            drop(node);
        }

        if !owned_batch.is_empty() {
            let refs: Vec<&[u8]> = owned_batch.iter().map(|b| b.as_slice()).collect();
            f(&refs);
        }

        Ok(())
    }

    pub fn for_each_leaf_batch<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[Vec<u8>]) -> bool,
    {
        let leaf_node = self.find_leftmost_leaf();
        let mut current = Some(leaf_node);

        while let Some(node_arc) = current {
            let node = node_arc.read();
            match &*node {
                BTreeNode::Leaf(leaf) => {
                    if !leaf.values.is_empty() && !f(&leaf.values) {
                        return Ok(());
                    }
                    current = leaf.next.clone();
                }
                _ => break,
            }
            drop(node);
        }

        Ok(())
    }

    pub fn collect_leaves(&self) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut leaves = Vec::new();
        let leaf_node = self.find_leftmost_leaf();
        let mut current = Some(leaf_node);

        while let Some(node_arc) = current {
            let node = node_arc.read();
            match &*node {
                BTreeNode::Leaf(leaf) => {
                    let entries: Vec<(Vec<u8>, Vec<u8>)> = leaf.keys.iter()
                        .zip(leaf.values.iter())
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    if !entries.is_empty() {
                        leaves.push(entries);
                    }
                    current = leaf.next.clone();
                }
                _ => break,
            }
            drop(node);
        }

        leaves
    }

    pub fn count(&self) -> usize {
        let mut count = 0;
        let leaf_node = self.find_leftmost_leaf();
        let mut current = Some(leaf_node);

        while let Some(node_arc) = current {
            let node = node_arc.read();
            match &*node {
                BTreeNode::Leaf(leaf) => {
                    count += leaf.keys.len();
                    current = leaf.next.clone();
                }
                _ => break,
            }
            drop(node);
        }

        count
    }

    fn find_leaf(&self, key: &[u8]) -> Arc<RwLock<BTreeNode>> {
        let mut current = Arc::clone(&self.root);
        loop {
            let node = current.read();
            match &*node {
                BTreeNode::Leaf(_) => {
                    drop(node);
                    return current;
                }
                BTreeNode::Internal(internal) => {
                    let mut idx = internal.keys.len();
                    for (i, k) in internal.keys.iter().enumerate() {
                        if key < k.as_slice() {
                            idx = i;
                            break;
                        }
                    }
                    let next = Arc::clone(&internal.children[idx]);
                    drop(node);
                    current = next;
                }
            }
        }
    }

    fn find_leftmost_leaf(&self) -> Arc<RwLock<BTreeNode>> {
        let mut current = Arc::clone(&self.root);
        loop {
            let node = current.read();
            match &*node {
                BTreeNode::Leaf(_) => {
                    drop(node);
                    return current;
                }
                BTreeNode::Internal(internal) => {
                    let next = Arc::clone(&internal.children[0]);
                    drop(node);
                    current = next;
                }
            }
        }
    }

    fn insert_into_leaf(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let leaf_node = self.find_leaf(&key);
        let mut node = leaf_node.write();
        match &mut *node {
            BTreeNode::Leaf(leaf) => {
                let pos = leaf.keys.iter().position(|k| k.as_slice() >= key.as_slice())
                    .unwrap_or(leaf.keys.len());

                if pos < leaf.keys.len() && leaf.keys[pos] == key {
                    leaf.values[pos] = value;
                } else {
                    leaf.keys.insert(pos, key);
                    leaf.values.insert(pos, value);
                }
                Ok(())
            }
            _ => Err(CoreError::Internal("Expected leaf node".into())),
        }
    }

    fn insert_and_split(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut root = self.root.write();
        match &mut *root {
            BTreeNode::Leaf(leaf) => {
                let pos = leaf.keys.iter().position(|k| k.as_slice() >= key.as_slice())
                    .unwrap_or(leaf.keys.len());

                if pos < leaf.keys.len() && leaf.keys[pos] == key {
                    leaf.values[pos] = value;
                    return Ok(());
                }

                leaf.keys.insert(pos, key);
                leaf.values.insert(pos, value);

                let mid = leaf.keys.len() / 2;
                let right_keys = leaf.keys.split_off(mid);
                let right_values = leaf.values.split_off(mid);
                let split_key = right_keys[0].clone();

                let right_leaf = Arc::new(RwLock::new(BTreeNode::Leaf(LeafNode {
                    keys: right_keys,
                    values: right_values,
                    next: leaf.next.take(),
                })));

                leaf.next = Some(Arc::clone(&right_leaf));

                let left_leaf = Arc::new(RwLock::new(BTreeNode::Leaf(LeafNode {
                    keys: std::mem::take(&mut leaf.keys),
                    values: std::mem::take(&mut leaf.values),
                    next: Some(Arc::clone(&right_leaf)),
                })));

                *root = BTreeNode::Internal(InternalNode {
                    keys: vec![split_key],
                    children: vec![left_leaf, right_leaf],
                });

                Ok(())
            }
            _ => Err(CoreError::Internal("Expected leaf for split".into())),
        }
    }

    fn insert_recursive(&self, node_arc: &Arc<RwLock<BTreeNode>>, key: Vec<u8>, value: Vec<u8>) -> Result<Option<(Vec<u8>, Arc<RwLock<BTreeNode>>)>> {
        let node = node_arc.read();
        match &*node {
            BTreeNode::Leaf(_) => {
                drop(node);
                let mut node_w = node_arc.write();
                match &mut *node_w {
                    BTreeNode::Leaf(leaf) => {
                        let pos = leaf.keys.iter().position(|k| k.as_slice() >= key.as_slice())
                            .unwrap_or(leaf.keys.len());

                        if pos < leaf.keys.len() && leaf.keys[pos] == key {
                            leaf.values[pos] = value;
                            return Ok(None);
                        }

                        leaf.keys.insert(pos, key);
                        leaf.values.insert(pos, value);

                        if leaf.keys.len() >= self.order {
                            let mid = leaf.keys.len() / 2;
                            let right_keys = leaf.keys.split_off(mid);
                            let right_values = leaf.values.split_off(mid);
                            let split_key = right_keys[0].clone();

                            let right_leaf = Arc::new(RwLock::new(BTreeNode::Leaf(LeafNode {
                                keys: right_keys,
                                values: right_values,
                                next: leaf.next.take(),
                            })));
                            leaf.next = Some(Arc::clone(&right_leaf));

                            return Ok(Some((split_key, right_leaf)));
                        }
                        Ok(None)
                    }
                    _ => unreachable!(),
                }
            }
            BTreeNode::Internal(internal) => {
                let mut idx = internal.keys.len();
                for (i, k) in internal.keys.iter().enumerate() {
                    if key.as_slice() < k.as_slice() {
                        idx = i;
                        break;
                    }
                }
                let child = Arc::clone(&internal.children[idx]);
                drop(node);

                if let Some((split_key, new_child)) = self.insert_recursive(&child, key, value)? {
                    let mut node_w = node_arc.write();
                    match &mut *node_w {
                        BTreeNode::Internal(internal) => {
                            internal.keys.insert(idx, split_key);
                            internal.children.insert(idx + 1, new_child);

                            if internal.keys.len() >= self.order {
                                let mid = internal.keys.len() / 2;
                                let split_key = internal.keys[mid].clone();

                                let right_keys = internal.keys.split_off(mid + 1);
                                internal.keys.pop();
                                let right_children = internal.children.split_off(mid + 1);

                                let right_node = Arc::new(RwLock::new(BTreeNode::Internal(InternalNode {
                                    keys: right_keys,
                                    children: right_children,
                                })));

                                return Ok(Some((split_key, right_node)));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(None)
            }
        }
    }
}
