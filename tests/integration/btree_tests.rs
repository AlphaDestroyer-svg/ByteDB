#[cfg(test)]
mod tests {
    use bytedb_core::index::btree::BPlusTree;

    #[test]
    fn test_insert_and_search() {
        let tree = BPlusTree::new("test", 4);
        tree.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        tree.insert(b"key2".to_vec(), b"value2".to_vec()).unwrap();
        tree.insert(b"key3".to_vec(), b"value3".to_vec()).unwrap();

        assert_eq!(tree.search(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(tree.search(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(tree.search(b"key3").unwrap(), Some(b"value3".to_vec()));
        assert_eq!(tree.search(b"key4").unwrap(), None);
    }

    #[test]
    fn test_insert_overwrite() {
        let tree = BPlusTree::new("test", 4);
        tree.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        tree.insert(b"key1".to_vec(), b"updated".to_vec()).unwrap();

        assert_eq!(tree.search(b"key1").unwrap(), Some(b"updated".to_vec()));
    }

    #[test]
    fn test_delete() {
        let tree = BPlusTree::new("test", 4);
        tree.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        tree.insert(b"key2".to_vec(), b"value2".to_vec()).unwrap();

        assert!(tree.delete(b"key1").unwrap());
        assert_eq!(tree.search(b"key1").unwrap(), None);
        assert_eq!(tree.search(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert!(!tree.delete(b"key999").unwrap());
    }

    #[test]
    fn test_range_scan() {
        let tree = BPlusTree::new("test", 4);
        tree.insert(b"a".to_vec(), b"1".to_vec()).unwrap();
        tree.insert(b"b".to_vec(), b"2".to_vec()).unwrap();
        tree.insert(b"c".to_vec(), b"3".to_vec()).unwrap();
        tree.insert(b"d".to_vec(), b"4".to_vec()).unwrap();
        tree.insert(b"e".to_vec(), b"5".to_vec()).unwrap();

        let results = tree.range_scan(b"b", b"d").unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_many_inserts_with_splits() {
        let tree = BPlusTree::new("test", 4);
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{:04}", i);
            tree.insert(key.into_bytes(), val.into_bytes()).unwrap();
        }

        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{:04}", i);
            assert_eq!(tree.search(key.as_bytes()).unwrap(), Some(val.into_bytes()));
        }
    }

    #[test]
    fn test_scan_all() {
        let tree = BPlusTree::new("test", 4);
        tree.insert(b"c".to_vec(), b"3".to_vec()).unwrap();
        tree.insert(b"a".to_vec(), b"1".to_vec()).unwrap();
        tree.insert(b"b".to_vec(), b"2".to_vec()).unwrap();

        let all = tree.scan_all().unwrap();
        assert_eq!(all.len(), 3);
    }
}
