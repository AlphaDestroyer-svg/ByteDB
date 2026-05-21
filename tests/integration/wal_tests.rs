#[cfg(test)]
mod tests {
    use bytedb_core::wal::log_record::LogRecord;
    use bytedb_core::wal::log_manager::LogManager;
    use std::fs;

    fn temp_log_path() -> String {
        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        format!("./test_wal_{}.log", id)
    }

    #[test]
    fn test_wal_append_and_read() {
        let path = temp_log_path();
        let log_manager = LogManager::new(&path).unwrap();

        let lsn1 = log_manager.append(LogRecord::Begin { txn_id: 1 }).unwrap();
        let lsn2 = log_manager.append(LogRecord::Insert {
            txn_id: 1,
            table_id: 1,
            page_id: 5,
            slot: 0,
            data: b"hello".to_vec(),
        }).unwrap();
        let lsn3 = log_manager.append(LogRecord::Commit { txn_id: 1 }).unwrap();

        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(lsn3, 3);

        log_manager.flush().unwrap();

        let records = log_manager.read_all_records().unwrap();
        assert_eq!(records.len(), 3);

        match &records[0].1 {
            LogRecord::Begin { txn_id } => assert_eq!(*txn_id, 1),
            _ => panic!("Expected Begin"),
        }
        match &records[2].1 {
            LogRecord::Commit { txn_id } => assert_eq!(*txn_id, 1),
            _ => panic!("Expected Commit"),
        }

        fs::remove_file(&path).ok();
    }

    #[test]
    fn test_wal_recovery_detection() {
        use bytedb_core::wal::recovery::RecoveryManager;

        let path = temp_log_path();
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

        fs::remove_file(&path).ok();
    }
}
