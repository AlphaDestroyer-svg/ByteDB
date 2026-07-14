use std::sync::Arc;
use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_query::error::QueryError;
use bytedb_query::executor::engine::{ExecutionResult, QueryEngine};
use bytedb_query::parser::parser::Parser;

fn engine() -> QueryEngine {
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    QueryEngine::new(db, txn)
}

fn run(e: &QueryEngine, sql: &str) -> bytedb_query::error::Result<ExecutionResult> {
    let mut p = Parser::new(sql).unwrap();
    let stmt = p.parse().unwrap();
    e.execute(stmt, None)
}

fn seed(e: &QueryEngine, n: i64) {
    run(e, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 0..n {
        run(e, &format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap();
    }
}

#[test]
fn bare_execute_is_unlimited_by_default() {
    let e = engine();
    seed(&e, 50);
    match run(&e, "SELECT * FROM t").unwrap() {
        ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 50),
        other => panic!("{other:?}"),
    }
}

#[test]
fn bare_execute_enforces_max_scan_rows() {
    let e = engine();
    seed(&e, 50);
    e.set_resource_limits(10, 0);
    let r = run(&e, "SELECT * FROM t");
    assert!(
        matches!(r, Err(QueryError::ResourceLimit(_))),
        "expected ResourceLimit on the server path, got {:?}",
        r
    );
}

#[test]
fn context_does_not_leak_between_queries() {
    let e = engine();
    seed(&e, 50);
    e.set_resource_limits(10, 0);
    assert!(run(&e, "SELECT * FROM t").is_err());

    e.set_resource_limits(1000, 0);
    match run(&e, "SELECT * FROM t").unwrap() {
        ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 50),
        other => panic!("{other:?}"),
    }

    e.set_resource_limits(0, 0);
    match run(&e, "SELECT * FROM t").unwrap() {
        ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 50),
        other => panic!("{other:?}"),
    }
}
