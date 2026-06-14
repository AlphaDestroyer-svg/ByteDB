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
        other => panic!("expected rows, got {other:?}"),
    }
}

fn approx(v: Value) -> f64 {
    match v {
        Value::Int64(n) => n as f64,
        Value::Float64(f) => f,
        other => panic!("unexpected numeric type {other:?}"),
    }
}

#[test]
fn multiply_overflow_does_not_wrap() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, a BIGINT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 9223372036854775807)", None).unwrap();
    let v = approx(one(&e, "SELECT a * 2 FROM t WHERE id = 1"));
    assert!(v > 1.8e19, "a*2 wrapped to {v}");
}

#[test]
fn add_overflow_does_not_wrap() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, a BIGINT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 9223372036854775807)", None).unwrap();
    let v = approx(one(&e, "SELECT a + a FROM t WHERE id = 1"));
    assert!(v > 1.8e19, "a+a wrapped to {v}");
}

#[test]
fn normal_arithmetic_stays_integer() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, a INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 21)", None).unwrap();
    assert_eq!(one(&e, "SELECT a * 2 FROM t WHERE id = 1"), Value::Int64(42));
    assert_eq!(one(&e, "SELECT a - 1 FROM t WHERE id = 1"), Value::Int64(20));
    assert_eq!(one(&e, "SELECT a / 2 FROM t WHERE id = 1"), Value::Int64(10));
}
