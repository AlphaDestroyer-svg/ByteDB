use std::sync::Arc;

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_query::executor::engine::{ExecutionResult, QueryEngine};

fn engine() -> QueryEngine {
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    QueryEngine::new(db, txn)
}

fn count(e: &QueryEngine, sql: &str) -> usize {
    match e.execute_sql(sql, None).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.len(),
        other => panic!("{other:?}"),
    }
}

#[test]
fn duplicate_primary_key_is_rejected() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 100)", None).unwrap();

    let r = e.execute_sql("INSERT INTO t VALUES (1, 200)", None);
    assert!(r.is_err(), "duplicate PRIMARY KEY must be rejected, got {r:?}");

    assert_eq!(count(&e, "SELECT id FROM t"), 1, "row count must stay 1");
    match e.execute_sql("SELECT v FROM t WHERE id = 1", None).unwrap() {
        ExecutionResult::Rows { rows, .. } => {
            assert_eq!(rows[0][0].as_i64().unwrap(), 100, "original value must be preserved");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn unique_column_is_rejected() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 'a@x.com')", None).unwrap();
    let r = e.execute_sql("INSERT INTO t VALUES (2, 'a@x.com')", None);
    assert!(r.is_err(), "duplicate UNIQUE value must be rejected, got {r:?}");
}
