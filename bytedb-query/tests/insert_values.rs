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
fn insert_negative_integer() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, n INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, -100)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2, -9223372036854775807)", None).unwrap();
    assert_eq!(one(&e, "SELECT n FROM t WHERE id = 1"), Value::Int64(-100));
    assert_eq!(one(&e, "SELECT n FROM t WHERE id = 2"), Value::Int64(-9223372036854775807));
}

#[test]
fn insert_arithmetic_expression() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, n INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 10 * 5 + 2)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2, -3 - 4)", None).unwrap();
    assert_eq!(one(&e, "SELECT n FROM t WHERE id = 1"), Value::Int64(52));
    assert_eq!(one(&e, "SELECT n FROM t WHERE id = 2"), Value::Int64(-7));
}

#[test]
fn insert_negative_float() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, f FLOAT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, -3.5)", None).unwrap();
    assert_eq!(one(&e, "SELECT f FROM t WHERE id = 1"), Value::Float64(-3.5));
}
