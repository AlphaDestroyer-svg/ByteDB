use std::sync::Arc;
use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_query::executor::engine::{ExecutionResult, QueryEngine};

fn engine() -> QueryEngine {
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    QueryEngine::new(db, txn)
}

fn seed(e: &QueryEngine) {
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
    for i in 0..5 {
        e.execute_sql(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10), None).unwrap();
    }
}

#[test]
fn parse_cached_distinguishes_different_sql() {
    let e = engine();
    let s1 = format!("{:?}", e.parse_cached("SELECT 1").unwrap());
    let s2 = format!("{:?}", e.parse_cached("SELECT 2").unwrap());
    assert_ne!(s1, s2, "different SQL must not share a cached statement");

    let s1_again = format!("{:?}", e.parse_cached("SELECT 1").unwrap());
    assert_eq!(s1, s1_again, "same SQL must return an equivalent statement from cache");
}

#[test]
fn cache_serves_repeated_queries_correctly() {
    let e = engine();
    seed(&e);

    for _ in 0..4 {
        match e.execute_sql("SELECT id FROM t WHERE id = 3", None).unwrap() {
            ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            other => panic!("{other:?}"),
        }
    }

    match e.execute_sql("SELECT id FROM t", None).unwrap() {
        ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 5),
        other => panic!("{other:?}"),
    }

    match e.execute_sql("SELECT id FROM t WHERE id = 3", None).unwrap() {
        ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
        other => panic!("{other:?}"),
    }
}
