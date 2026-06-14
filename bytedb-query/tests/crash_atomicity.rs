use std::sync::Arc;

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::{IsolationLevel, TransactionManager};
use bytedb_query::executor::diskstore::DiskStore;
use bytedb_query::executor::engine::{ExecutionResult, QueryEngine};

fn count(e: &QueryEngine, sql: &str) -> usize {
    match e.execute_sql(sql, None).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.len(),
        other => panic!("{other:?}"),
    }
}

#[test]
fn uncommitted_txn_does_not_survive_crash() {
    let mut dir = std::env::temp_dir();
    dir.push(format!("bytedb_crash_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    {
        let ds = DiskStore::open(dir.clone(), "test").unwrap();
        let db = Arc::new(Database::new("test"));
        let txn = Arc::new(TransactionManager::new());
        let mut e = QueryEngine::new(db, txn.clone());
        e.attach_disk_store(ds);
        e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
        e.execute_sql("INSERT INTO t VALUES (1, 100)", None).unwrap();

        let t1 = txn.begin(IsolationLevel::ReadCommitted);
        e.execute_sql("INSERT INTO t VALUES (2, 200)", Some(t1)).unwrap();
        e.execute_sql("UPDATE t SET v = 999 WHERE id = 1", Some(t1)).unwrap();
    }

    {
        let ds = DiskStore::open(dir.clone(), "test").unwrap();
        let db = Arc::new(Database::new("test"));
        let txn = Arc::new(TransactionManager::new());
        let mut e = QueryEngine::new(db, txn);
        e.attach_disk_store(ds);

        assert_eq!(count(&e, "SELECT id FROM t"), 1, "uncommitted INSERT must not survive a crash");
        match e.execute_sql("SELECT v FROM t WHERE id = 1", None).unwrap() {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0].as_i64().unwrap(), 100, "uncommitted UPDATE must not survive a crash");
            }
            other => panic!("{other:?}"),
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn committed_txn_survives_crash() {
    let mut dir = std::env::temp_dir();
    dir.push(format!("bytedb_crash_commit_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    {
        let ds = DiskStore::open(dir.clone(), "test").unwrap();
        let db = Arc::new(Database::new("test"));
        let txn = Arc::new(TransactionManager::new());
        let mut e = QueryEngine::new(db, txn.clone());
        e.attach_disk_store(ds);
        e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();

        let t1 = txn.begin(IsolationLevel::ReadCommitted);
        e.execute_sql("INSERT INTO t VALUES (1, 100)", Some(t1)).unwrap();
        e.execute_sql("COMMIT", Some(t1)).unwrap();
    }

    {
        let ds = DiskStore::open(dir.clone(), "test").unwrap();
        let db = Arc::new(Database::new("test"));
        let txn = Arc::new(TransactionManager::new());
        let mut e = QueryEngine::new(db, txn);
        e.attach_disk_store(ds);
        assert_eq!(count(&e, "SELECT id FROM t"), 1, "committed row must survive a crash");
    }

    let _ = std::fs::remove_dir_all(&dir);
}
