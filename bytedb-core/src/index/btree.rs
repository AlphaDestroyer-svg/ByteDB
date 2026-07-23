use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use parking_lot::RwLock;

use crate::error::{CoreError, Result};

pub struct BPlusTree {
    root: RwLock<Arc<RwLock<BTreeNode>>>,
    order: usize,
    name: String,
    len: AtomicUsize,
}

#[derive(Debug)]
enum BTreeNode {
    Internal(InternalNode),
    Leaf(LeafNode),
}

#[derive(Debug)]
struct InternalNode {
    keys: Vec<Vec<u8>>,
    children: Vec<Arc<RwLock<BTreeNode>>>,
    high_key: Option<Vec<u8>>,
    right_link: Option<Arc<RwLock<BTreeNode>>>,
}

#[derive(Debug)]
struct LeafNode {
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
    high_key: Option<Vec<u8>>,
    right_link: Option<Arc<RwLock<BTreeNode>>>,
}

impl BTreeNode {
    fn high_key(&self) -> Option<&Vec<u8>> {
        match self {
            BTreeNode::Internal(n) => n.high_key.as_ref(),
            BTreeNode::Leaf(n) => n.high_key.as_ref(),
        }
    }

    fn right_link(&self) -> Option<&Arc<RwLock<BTreeNode>>> {
        match self {
            BTreeNode::Internal(n) => n.right_link.as_ref(),
            BTreeNode::Leaf(n) => n.right_link.as_ref(),
        }
    }

}

impl BPlusTree {
    pub fn new(name: impl Into<String>, order: usize) -> Self {
        let leaf = BTreeNode::Leaf(LeafNode {
            keys: Vec::new(),
            values: Vec::new(),
            high_key: None,
            right_link: None,
        });

        BPlusTree {
            root: RwLock::new(Arc::new(RwLock::new(leaf))),
            order,
            name: name.into(),
            len: AtomicUsize::new(0),
        }
    }

    pub fn approx_len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn root_arc(&self) -> Arc<RwLock<BTreeNode>> {
        Arc::clone(&*self.root.read())
    }

    fn move_right_read(&self, mut node_arc: Arc<RwLock<BTreeNode>>, key: &[u8]) -> Arc<RwLock<BTreeNode>> {
        loop {
            let node = node_arc.read();
            let should_move = match (node.high_key(), node.right_link()) {
                (Some(hk), Some(_)) => key > hk.as_slice(),
                _ => false,
            };
            if !should_move {
                drop(node);
                return node_arc;
            }
            let next = Arc::clone(node.right_link().unwrap());
            drop(node);
            node_arc = next;
        }
    }

    fn move_right_write(&self, mut node_arc: Arc<RwLock<BTreeNode>>, key: &[u8]) -> Arc<RwLock<BTreeNode>> {
        loop {
            let node = node_arc.read();
            let should_move = match (node.high_key(), node.right_link()) {
                (Some(hk), Some(_)) => key > hk.as_slice(),
                _ => false,
            };
            if !should_move {
                drop(node);
                return node_arc;
            }
            let next = Arc::clone(node.right_link().unwrap());
            drop(node);
            node_arc = next;
        }
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        loop {
            let root = self.root_arc();
            let mut path: Vec<Arc<RwLock<BTreeNode>>> = Vec::new();
            let mut current = root.clone();

            loop {
                current = self.move_right_write(current, &key);
                let node = current.read();
                match &*node {
                    BTreeNode::Leaf(_) => {
                        drop(node);
                        break;
                    }
                    BTreeNode::Internal(internal) => {
                        let idx = internal.keys.partition_point(|k| k.as_slice() <= key.as_slice());
                        let next = Arc::clone(&internal.children[idx]);
                        path.push(Arc::clone(&current));
                        drop(node);
                        current = next;
                    }
                }
            }

            let split = {
                let mut leaf_w = current.write();
                match &mut *leaf_w {
                    BTreeNode::Leaf(leaf) => {
                        if let Some(hk) = &leaf.high_key {
                            if key.as_slice() > hk.as_slice() && leaf.right_link.is_some() {
                                continue;
                            }
                        }
                        self.leaf_insert_locked(leaf, key.clone(), value.clone())
                    }
                    _ => return Err(CoreError::Internal("Expected leaf node".into())),
                }
            };

            let mut split = match split {
                Some(s) => s,
                None => return Ok(()),
            };

            for parent_arc in path.iter().rev() {
                let parent_arc = self.move_right_write(Arc::clone(parent_arc), &split.0);
                let mut parent_w = parent_arc.write();
                match &mut *parent_w {
                    BTreeNode::Internal(internal) => {
                        let new_split = self.internal_insert_locked(internal, split.0, split.1);
                        match new_split {
                            Some(s) => split = s,
                            None => return Ok(()),
                        }
                    }
                    _ => return Err(CoreError::Internal("Expected internal node".into())),
                }
            }

            let mut root_guard = self.root.write();
            if !Arc::ptr_eq(&*root_guard, &root) {
                let mut current = Arc::clone(&*root_guard);
                drop(root_guard);
                loop {
                    current = self.move_right_write(current, &split.0);
                    let node = current.read();
                    let is_root_child = match &*node {
                        BTreeNode::Internal(internal) => {
                            internal.children.iter().any(|c| Arc::ptr_eq(c, &split.1))
                        }
                        _ => false,
                    };
                    if is_root_child {
                        drop(node);
                        return Ok(());
                    }
                    match &*node {
                        BTreeNode::Internal(internal) => {
                            let idx = internal.keys.partition_point(|k| k.as_slice() <= split.0.as_slice());
                            let next = Arc::clone(&internal.children[idx]);
                            drop(node);
                            current = next;
                        }
                        BTreeNode::Leaf(_) => {
                            drop(node);
                            return Ok(());
                        }
                    }
                }
            }

            let old_root = Arc::clone(&*root_guard);
            let new_root = Arc::new(RwLock::new(BTreeNode::Internal(InternalNode {
                keys: vec![split.0],
                children: vec![old_root, split.1],
                high_key: None,
                right_link: None,
            })));
            *root_guard = new_root;
            return Ok(());
        }
    }

    fn leaf_insert_locked(
        &self,
        leaf: &mut LeafNode,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Option<(Vec<u8>, Arc<RwLock<BTreeNode>>)> {
        let pos = match leaf.keys.binary_search_by(|k| k.as_slice().cmp(key.as_slice())) {
            Ok(i) => {
                leaf.values[i] = value;
                return None;
            }
            Err(i) => i,
        };

        leaf.keys.insert(pos, key);
        leaf.values.insert(pos, value);
        self.len.fetch_add(1, Ordering::Relaxed);

        if leaf.keys.len() < self.order {
            return None;
        }

        crate::chaos::split_hook();

        let mid = leaf.keys.len() / 2;
        let right_keys = leaf.keys.split_off(mid);
        let right_values = leaf.values.split_off(mid);
        let split_key = right_keys[0].clone();
        let right_high_key = right_keys.last().cloned();

        let right_leaf = Arc::new(RwLock::new(BTreeNode::Leaf(LeafNode {
            keys: right_keys,
            values: right_values,
            high_key: right_high_key,
            right_link: leaf.right_link.take(),
        })));

        leaf.high_key = leaf.keys.last().cloned();
        leaf.right_link = Some(Arc::clone(&right_leaf));

        Some((split_key, right_leaf))
    }

    fn internal_insert_locked(
        &self,
        internal: &mut InternalNode,
        split_key: Vec<u8>,
        new_child: Arc<RwLock<BTreeNode>>,
    ) -> Option<(Vec<u8>, Arc<RwLock<BTreeNode>>)> {
        let idx = internal.keys.partition_point(|k| k.as_slice() <= split_key.as_slice());
        internal.keys.insert(idx, split_key);
        internal.children.insert(idx + 1, new_child);

        if internal.keys.len() < self.order {
            return None;
        }

        let mid = internal.keys.len() / 2;
        let promoted = internal.keys[mid].clone();

        let right_keys: Vec<Vec<u8>> = internal.keys.split_off(mid + 1);
        internal.keys.pop();
        let right_children: Vec<Arc<RwLock<BTreeNode>>> = internal.children.split_off(mid + 1);

        let parent_high_key = internal.high_key.take();

        let right_node = Arc::new(RwLock::new(BTreeNode::Internal(InternalNode {
            keys: right_keys,
            children: right_children,
            high_key: parent_high_key,
            right_link: internal.right_link.take(),
        })));

        internal.high_key = Some(promoted.clone());
        internal.right_link = Some(Arc::clone(&right_node));

        Some((promoted, right_node))
    }

    pub fn search(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        crate::chaos::index_traversal_hook();
        let leaf_arc = self.find_leaf_read(key);
        let node = leaf_arc.read();
        match &*node {
            BTreeNode::Leaf(leaf) => {
                match leaf.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                    Ok(i) => Ok(Some(leaf.values[i].clone())),
                    Err(_) => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let leaf_arc = self.find_leaf_write(key);
        let mut node_w = leaf_arc.write();
        match &mut *node_w {
            BTreeNode::Leaf(leaf) => {
                match leaf.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                    Ok(pos) => {
                        leaf.keys.remove(pos);
                        leaf.values.remove(pos);
                        self.len.fetch_sub(1, Ordering::Relaxed);
                        Ok(true)
                    }
                    Err(_) => Ok(false),
                }
            }
            _ => Ok(false),
        }
    }

    fn find_leaf_read(&self, key: &[u8]) -> Arc<RwLock<BTreeNode>> {
        let mut current = self.root_arc();
        loop {
            current = self.move_right_read(current, key);
            let node = current.read();
            match &*node {
                BTreeNode::Leaf(_) => {
                    drop(node);
                    return current;
                }
                BTreeNode::Internal(internal) => {
                    let idx = internal.keys.partition_point(|k| k.as_slice() <= key);
                    let next = Arc::clone(&internal.children[idx]);
                    drop(node);
                    current = next;
                }
            }
        }
    }

    fn find_leaf_write(&self, key: &[u8]) -> Arc<RwLock<BTreeNode>> {
        self.find_leaf_read(key)
    }

    fn find_leftmost_leaf(&self) -> Arc<RwLock<BTreeNode>> {
        let mut current = self.root_arc();
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

    pub fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results = Vec::new();
        let leaf_node = self.find_leaf_read(start);
        let mut current = Some(leaf_node);

        let mut first = true;
        'outer: while let Some(node_arc) = current {
            let node = node_arc.read();
            match &*node {
                BTreeNode::Leaf(leaf) => {
                    let start_idx = if first {
                        first = false;
                        match leaf.keys.binary_search_by(|k| k.as_slice().cmp(start)) {
                            Ok(i) => i,
                            Err(i) => i,
                        }
                    } else {
                        0
                    };
                    for i in start_idx..leaf.keys.len() {
                        let k = &leaf.keys[i];
                        if k.as_slice() > end {
                            break 'outer;
                        }
                        results.push((k.clone(), leaf.values[i].clone()));
                    }
                    current = leaf.right_link.clone();
                }
                _ => break,
            }
            drop(node);
        }

        Ok(results)
    }

    pub fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results = Vec::new();
        if prefix.is_empty() {
            return self.scan_all();
        }
        let leaf_node = self.find_leaf_read(prefix);
        let mut current = Some(leaf_node);

        let mut first = true;
        'outer: while let Some(node_arc) = current {
            let node = node_arc.read();
            match &*node {
                BTreeNode::Leaf(leaf) => {
                    let start_idx = if first {
                        first = false;
                        match leaf.keys.binary_search_by(|k| k.as_slice().cmp(prefix)) {
                            Ok(i) => i,
                            Err(i) => i,
                        }
                    } else {
                        0
                    };
                    for i in start_idx..leaf.keys.len() {
                        let k = &leaf.keys[i];
                        if !k.starts_with(prefix) {
                            if k.as_slice() > prefix {
                                break 'outer;
                            }
                            continue;
                        }
                        results.push((k.clone(), leaf.values[i].clone()));
                    }
                    current = leaf.right_link.clone();
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
                    current = leaf.right_link.clone();
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
                    current = leaf.right_link.clone();
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
                    current = leaf.right_link.clone();
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
                    current = leaf.right_link.clone();
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
                    current = leaf.right_link.clone();
                }
                _ => break,
            }
            drop(node);
        }

        leaves
    }

    pub fn collect_leaf_values(&self) -> Vec<Vec<Vec<u8>>> {
        let mut leaves = Vec::new();
        let leaf_node = self.find_leftmost_leaf();
        let mut current = Some(leaf_node);

        while let Some(node_arc) = current {
            let node = node_arc.read();
            match &*node {
                BTreeNode::Leaf(leaf) => {
                    if !leaf.values.is_empty() {
                        leaves.push(leaf.values.clone());
                    }
                    current = leaf.right_link.clone();
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
                    current = leaf.right_link.clone();
                }
                _ => break,
            }
            drop(node);
        }

        count
    }
}
