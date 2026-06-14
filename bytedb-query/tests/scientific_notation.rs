use std::sync::Arc;
use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_core::tuple::value::Value;
use bytedb_query::executor::engine::{ExecutionResult, QueryEngine};

fn engine() -> QueryEngine {
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    QueryEngine::new(db, txn)
}
fn one(e: &QueryEngine, sql: &str) -> Value {
    match e.execute_sql(sql, None).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows[0][0].clone(),
        other => panic!("{other:?}"),
    }
}

#[test]
fn scientific_notation_float_literals() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, f FLOAT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 9.9e18)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2, 1.5e-2)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (3, 2E10)", None).unwrap();
    assert_eq!(one(&e, "SELECT f FROM t WHERE id = 1"), Value::Float64(9.9e18));
    assert_eq!(one(&e, "SELECT f FROM t WHERE id = 2"), Value::Float64(1.5e-2));
    assert_eq!(one(&e, "SELECT f FROM t WHERE id = 3"), Value::Float64(2e10));
}
