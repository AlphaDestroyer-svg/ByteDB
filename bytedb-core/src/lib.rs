pub mod storage;
pub mod buffer;
pub mod wal;
pub mod index;
pub mod tuple;
pub mod mvcc;
pub mod catalog;
pub mod snapshot;
pub mod dbstore;
pub mod workers;
pub mod error;

#[cfg(test)]
mod tests {
    use crate::index::btree::BPlusTree;
    use crate::wal::log_record::LogRecord;
    use crate::wal::log_manager::LogManager;
    use crate::wal::recovery::RecoveryManager;
    use crate::mvcc::transaction::{TransactionManager, IsolationLevel};
    use crate::catalog::database::Database;
    use crate::tuple::value::{Value, DataType};
    use crate::tuple::schema::{Schema, Column};
    use crate::tuple::tuple::Tuple;

    #[test]
    fn test_btree_insert_search() {
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
    fn test_btree_overwrite() {
        let tree = BPlusTree::new("test", 4);
        tree.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        tree.insert(b"key1".to_vec(), b"updated".to_vec()).unwrap();
        assert_eq!(tree.search(b"key1").unwrap(), Some(b"updated".to_vec()));
    }

    #[test]
    fn test_btree_delete() {
        let tree = BPlusTree::new("test", 4);
        tree.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        tree.insert(b"key2".to_vec(), b"value2".to_vec()).unwrap();

        assert!(tree.delete(b"key1").unwrap());
        assert_eq!(tree.search(b"key1").unwrap(), None);
        assert_eq!(tree.search(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert!(!tree.delete(b"key999").unwrap());
    }

    #[test]
    fn test_btree_range_scan() {
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
    fn test_btree_many_inserts() {
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
    fn test_wal_append_and_read() {
        let path = format!("./test_wal_{}.log", std::process::id());
        let log_manager = LogManager::new(&path).unwrap();

        log_manager.append(LogRecord::Begin { txn_id: 1 }).unwrap();
        log_manager.append(LogRecord::Insert {
            txn_id: 1, table_id: 1, page_id: 5, slot: 0, data: b"hello".to_vec(),
        }).unwrap();
        log_manager.append(LogRecord::Commit { txn_id: 1 }).unwrap();
        log_manager.flush().unwrap();

        let records = log_manager.read_all_records().unwrap();
        assert_eq!(records.len(), 3);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_wal_recovery() {
        let path = format!("./test_wal_recovery_{}.log", std::process::id());
        let log_manager = LogManager::new(&path).unwrap();

        log_manager.append(LogRecord::Begin { txn_id: 1 }).unwrap();
        log_manager.append(LogRecord::Insert {
            txn_id: 1, table_id: 1, page_id: 1, slot: 0, data: b"data1".to_vec(),
        }).unwrap();
        log_manager.append(LogRecord::Commit { txn_id: 1 }).unwrap();
        log_manager.append(LogRecord::Begin { txn_id: 2 }).unwrap();
        log_manager.append(LogRecord::Insert {
            txn_id: 2, table_id: 1, page_id: 2, slot: 0, data: b"data2".to_vec(),
        }).unwrap();
        log_manager.flush().unwrap();

        let result = RecoveryManager::recover(&log_manager).unwrap();
        assert!(result.committed_txns.contains(&1));
        assert!(result.aborted_txns.contains(&2));
        assert_eq!(result.redo_records.len(), 1);
        assert_eq!(result.undo_records.len(), 1);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_transaction_manager() {
        let tm = TransactionManager::new();
        let txn1 = tm.begin(IsolationLevel::ReadCommitted);
        let txn2 = tm.begin(IsolationLevel::Serializable);

        assert!(tm.is_active(txn1));
        assert!(tm.is_active(txn2));
        assert_eq!(tm.active_count(), 2);

        tm.commit(txn1).unwrap();
        assert!(!tm.is_active(txn1));
        assert!(tm.is_committed(txn1));

        tm.abort(txn2).unwrap();
        assert!(!tm.is_active(txn2));
        assert!(!tm.is_committed(txn2));
    }

    #[test]
    fn test_tuple_validation() {
        let schema = Schema::new("test", vec![
            Column::new("id", DataType::Int64).primary_key(),
            Column::new("name", DataType::Text).not_null(),
        ]);

        let valid = Tuple::new(vec![Value::Int64(1), Value::Text("Alice".into())]);
        assert!(valid.validate(&schema).is_ok());

        let null_pk = Tuple::new(vec![Value::Null, Value::Text("Bob".into())]);
        assert!(null_pk.validate(&schema).is_err());
    }

    #[test]
    fn test_database_catalog() {
        use crate::catalog::table::TableMeta;

        let db = Database::new("testdb");
        let schema = Schema::new("users", vec![
            Column::new("id", DataType::Int64).primary_key(),
        ]);
        let meta = TableMeta::new("users", schema, 1);

        db.create_table(meta).unwrap();
        assert!(db.table_exists("users"));
        assert!(!db.table_exists("orders"));

        let tables = db.list_tables();
        assert_eq!(tables.len(), 1);

        db.drop_table("users").unwrap();
        assert!(!db.table_exists("users"));
    }

    #[test]
    fn test_value_comparison() {
        assert_eq!(Value::Int64(1).compare(&Value::Int64(2)), Some(std::cmp::Ordering::Less));
        assert_eq!(Value::Text("a".into()).compare(&Value::Text("b".into())), Some(std::cmp::Ordering::Less));
        assert_eq!(Value::Null.compare(&Value::Int64(1)), Some(std::cmp::Ordering::Less));
    }
}
