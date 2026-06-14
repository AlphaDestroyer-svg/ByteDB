use std::sync::Arc;

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::{IsolationLevel, TransactionManager};
use bytedb_query::executor::engine::{ExecutionResult, QueryEngine};

fn setup() -> (Arc<TransactionManager>, QueryEngine) {
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    let e = QueryEngine::new(db, txn.clone());
    (txn, e)
}

fn count(e: &QueryEngine, sql: &str, t: Option<u64>) -> usize {
    match e.execute_sql(sql, t).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.len(),
        o => panic!("{o:?}"),
    }
}

#[test]
fn no_dirty_read_across_transactions() {
    let (txn, e) = setup();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 100)", None).unwrap();

    let a = txn.begin(IsolationLevel::ReadCommitted);
    let b = txn.begin(IsolationLevel::ReadCommitted);

    e.execute_sql("INSERT INTO t VALUES (2, 200)", Some(a)).unwrap();
    e.execute_sql("UPDATE t SET v = 999 WHERE id = 1", Some(a)).unwrap();

    assert_eq!(count(&e, "SELECT id FROM t", Some(b)), 1, "B must not see A's uncommitted insert");
    match e.execute_sql("SELECT v FROM t WHERE id = 1", Some(b)).unwrap() {
        ExecutionResult::Rows { rows, .. } => {
            assert_eq!(rows[0][0].as_i64().unwrap(), 100, "B must not see A's uncommitted update");
        }
        o => panic!("{o:?}"),
    }
}

#[test]
fn read_your_own_writes() {
    let (txn, e) = setup();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();

    let a = txn.begin(IsolationLevel::ReadCommitted);
    e.execute_sql("INSERT INTO t VALUES (1, 100)", Some(a)).unwrap();
    e.execute_sql("UPDATE t SET v = 150 WHERE id = 1", Some(a)).unwrap();

    assert_eq!(count(&e, "SELECT id FROM t", Some(a)), 1, "A must see its own insert");
    match e.execute_sql("SELECT v FROM t WHERE id = 1", Some(a)).unwrap() {
        ExecutionResult::Rows { rows, .. } => {
            assert_eq!(rows[0][0].as_i64().unwrap(), 150, "A must see its own update");
        }
        o => panic!("{o:?}"),
    }
}

#[test]
fn snapshot_stable_after_concurrent_commit() {
    let (txn, e) = setup();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 100)", None).unwrap();

    let b = txn.begin(IsolationLevel::RepeatableRead);
    assert_eq!(count(&e, "SELECT id FROM t", Some(b)), 1);

    let a = txn.begin(IsolationLevel::ReadCommitted);
    e.execute_sql("INSERT INTO t VALUES (2, 200)", Some(a)).unwrap();
    e.execute_sql("COMMIT", Some(a)).unwrap();

    assert_eq!(count(&e, "SELECT id FROM t", Some(b)), 1, "B's snapshot must not see A's post-snapshot commit");

    assert_eq!(count(&e, "SELECT id FROM t", None), 2, "a fresh read sees the committed row");
}
