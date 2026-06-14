use std::sync::Arc;

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::{IsolationLevel, TransactionManager};
use bytedb_query::executor::diskstore::DiskStore;
use bytedb_query::executor::engine::{ExecutionResult, QueryEngine};

fn setup() -> (Arc<TransactionManager>, QueryEngine) {
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    let engine = QueryEngine::new(db, txn.clone());
    (txn, engine)
}

fn count(e: &QueryEngine, sql: &str, txn: Option<u64>) -> usize {
    match e.execute_sql(sql, txn).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.len(),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn rollback_of_insert_is_invisible_autocommit() {
    let (txn, e) = setup();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();

    let t1 = txn.begin(IsolationLevel::ReadCommitted);
    e.execute_sql("INSERT INTO t VALUES (1, 100)", Some(t1)).unwrap();
    e.execute_sql("ROLLBACK", Some(t1)).unwrap();

    let n = count(&e, "SELECT id FROM t", None);
    assert_eq!(n, 0, "rolled-back INSERT must not be visible (autocommit read)");
}

#[test]
fn rollback_of_insert_invisible_to_new_txn() {
    let (txn, e) = setup();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();

    let t1 = txn.begin(IsolationLevel::ReadCommitted);
    e.execute_sql("INSERT INTO t VALUES (1, 100)", Some(t1)).unwrap();
    e.execute_sql("ROLLBACK", Some(t1)).unwrap();

    let t2 = txn.begin(IsolationLevel::ReadCommitted);
    let n = count(&e, "SELECT id FROM t", Some(t2));
    assert_eq!(n, 0, "rolled-back INSERT must not be visible to a later txn");
}

#[test]
fn rollback_of_update_restores_value() {
    let (txn, e) = setup();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 100)", None).unwrap();

    let t1 = txn.begin(IsolationLevel::ReadCommitted);
    e.execute_sql("UPDATE t SET v = 999 WHERE id = 1", Some(t1)).unwrap();
    e.execute_sql("ROLLBACK", Some(t1)).unwrap();

    match e.execute_sql("SELECT v FROM t WHERE id = 1", None).unwrap() {
        ExecutionResult::Rows { rows, .. } => {
            let v = rows[0][0].as_i64().unwrap();
            assert_eq!(v, 100, "rolled-back UPDATE must restore the original value");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn rollback_of_delete_keeps_row() {
    let (txn, e) = setup();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 100)", None).unwrap();

    let t1 = txn.begin(IsolationLevel::ReadCommitted);
    e.execute_sql("DELETE FROM t WHERE id = 1", Some(t1)).unwrap();
    e.execute_sql("ROLLBACK", Some(t1)).unwrap();

    let n = count(&e, "SELECT id FROM t", None);
    assert_eq!(n, 1, "rolled-back DELETE must keep the row");
}

#[test]
fn rollback_restores_secondary_index() {
    let (txn, e) = setup();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 100)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2, 200)", None).unwrap();
    e.execute_sql("CREATE INDEX idx_v ON t (v)", None).unwrap();

    let t1 = txn.begin(IsolationLevel::ReadCommitted);
    e.execute_sql("UPDATE t SET v = 999 WHERE id = 1", Some(t1)).unwrap();
    e.execute_sql("INSERT INTO t VALUES (3, 300)", Some(t1)).unwrap();
    e.execute_sql("ROLLBACK", Some(t1)).unwrap();

    assert_eq!(count(&e, "SELECT id FROM t WHERE v = 100", None), 1, "index must point to restored value");
    assert_eq!(count(&e, "SELECT id FROM t WHERE v = 999", None), 0, "index must not retain rolled-back value");
    assert_eq!(count(&e, "SELECT id FROM t WHERE v = 300", None), 0, "index must not retain rolled-back insert");
}

#[test]
fn multi_statement_rollback() {
    let (txn, e) = setup();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();

    let t1 = txn.begin(IsolationLevel::ReadCommitted);
    e.execute_sql("INSERT INTO t VALUES (1, 10)", Some(t1)).unwrap();
    e.execute_sql("UPDATE t SET v = 20 WHERE id = 1", Some(t1)).unwrap();
    e.execute_sql("UPDATE t SET v = 30 WHERE id = 1", Some(t1)).unwrap();
    e.execute_sql("ROLLBACK", Some(t1)).unwrap();

    assert_eq!(count(&e, "SELECT id FROM t", None), 0, "all statements in the txn must be undone");
}

#[test]
fn commit_persists_changes() {
    let (txn, e) = setup();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();

    let t1 = txn.begin(IsolationLevel::ReadCommitted);
    e.execute_sql("INSERT INTO t VALUES (1, 100)", Some(t1)).unwrap();
    e.execute_sql("COMMIT", Some(t1)).unwrap();

    assert_eq!(count(&e, "SELECT id FROM t", None), 1, "committed row must be visible");
}

#[test]
fn rollback_survives_disk_reopen() {
    let mut dir = std::env::temp_dir();
    dir.push(format!("bytedb_rollback_test_{}", std::process::id()));
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
        e.execute_sql("ROLLBACK", Some(t1)).unwrap();
    }

    {
        let ds = DiskStore::open(dir.clone(), "test").unwrap();
        let db = Arc::new(Database::new("test"));
        let txn = Arc::new(TransactionManager::new());
        let mut e = QueryEngine::new(db, txn);
        e.attach_disk_store(ds);
        assert_eq!(count(&e, "SELECT id FROM t", None), 1, "rolled-back insert must not survive reopen");
        match e.execute_sql("SELECT v FROM t WHERE id = 1", None).unwrap() {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0].as_i64().unwrap(), 100, "rolled-back update must not survive reopen");
            }
            other => panic!("{other:?}"),
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}
