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
fn wal_redo_replays_after_disk_load() {
    let mut dir = std::env::temp_dir();
    dir.push(format!("bytedb_wal_redo_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let _scope = {
        let ds = DiskStore::open(dir.clone(), "test").unwrap();
        let db = Arc::new(Database::new("test"));
        let txn = Arc::new(TransactionManager::new());
        let mut e = QueryEngine::new(db, txn.clone());
        e.attach_disk_store(ds);
        e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();

        let t1 = txn.begin(IsolationLevel::ReadCommitted);
        e.execute_sql("INSERT INTO t VALUES (1, 10)", Some(t1)).unwrap();
        e.execute_sql("INSERT INTO t VALUES (2, 20)", Some(t1)).unwrap();
        e.execute_sql("COMMIT", Some(t1)).unwrap();
    };

    {
        let ds = DiskStore::open(dir.clone(), "test").unwrap();
        let db = Arc::new(Database::new("test"));
        let txn = Arc::new(TransactionManager::new());
        let mut e = QueryEngine::new(db, txn);
        e.attach_disk_store(ds);

        let result = e.execute_sql("SELECT * FROM t ORDER BY id", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2, "both committed rows must survive");
                assert_eq!(rows[0][0].as_i64().unwrap(), 1);
                assert_eq!(rows[0][1].as_i64().unwrap(), 10);
                assert_eq!(rows[1][0].as_i64().unwrap(), 2);
                assert_eq!(rows[1][1].as_i64().unwrap(), 20);
            }
            other => panic!("{other:?}"),
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn truncate_then_reopen_returns_empty_table() {
    let mut dir = std::env::temp_dir();
    dir.push(format!("bytedb_truncate_test_{}", std::process::id()));
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
        e.execute_sql("INSERT INTO t VALUES (2, 200)", Some(t1)).unwrap();
        e.execute_sql("COMMIT", Some(t1)).unwrap();
        e.execute_sql("TRUNCATE TABLE t", None).unwrap();
        let t2 = txn.begin(IsolationLevel::ReadCommitted);
        e.execute_sql("INSERT INTO t VALUES (3, 300)", Some(t2)).unwrap();
        e.execute_sql("COMMIT", Some(t2)).unwrap();
    }

    {
        let ds = DiskStore::open(dir.clone(), "test").unwrap();
        let db = Arc::new(Database::new("test"));
        let txn = Arc::new(TransactionManager::new());
        let mut e = QueryEngine::new(db, txn);
        e.attach_disk_store(ds);
        let result = e.execute_sql("SELECT * FROM t ORDER BY id", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1, "only the post-truncate row must survive");
                assert_eq!(rows[0][0].as_i64().unwrap(), 3);
                assert_eq!(rows[0][1].as_i64().unwrap(), 300);
            }
            other => panic!("{other:?}"),
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn multiple_commits_then_crash_all_survive() {
    let mut dir = std::env::temp_dir();
    dir.push(format!("bytedb_multi_crash_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    {
        let ds = DiskStore::open(dir.clone(), "test").unwrap();
        let db = Arc::new(Database::new("test"));
        let txn = Arc::new(TransactionManager::new());
        let mut e = QueryEngine::new(db, txn.clone());
        e.attach_disk_store(ds);
        e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();

        for batch in 0..5 {
            let tid = txn.begin(IsolationLevel::ReadCommitted);
            for i in 0..10 {
                let id = batch * 10 + i;
                e.execute_sql(&format!("INSERT INTO t VALUES ({}, {})", id, id * 10), Some(tid)).unwrap();
            }
            e.execute_sql("COMMIT", Some(tid)).unwrap();
        }
        assert_eq!(count(&e, "SELECT * FROM t"), 50);
    }

    {
        let ds = DiskStore::open(dir.clone(), "test").unwrap();
        let db = Arc::new(Database::new("test"));
        let txn = Arc::new(TransactionManager::new());
        let mut e = QueryEngine::new(db, txn);
        e.attach_disk_store(ds);
        assert_eq!(count(&e, "SELECT * FROM t"), 50, "all 50 committed rows must survive crash");

        let result = e.execute_sql("SELECT MIN(v), MAX(v), SUM(v) FROM t", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0].as_i64().unwrap(), 0);
                assert_eq!(rows[0][1].as_i64().unwrap(), 490);
                assert_eq!(rows[0][2].as_i64().unwrap(), 12250);
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
