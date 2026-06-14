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

#[test]
fn abs_and_negate_work() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, n BIGINT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, -9223372036854775807)", None).unwrap();
    assert_eq!(one(&e, "SELECT ABS(n) FROM t WHERE id = 1"), Value::Int64(9223372036854775807));
    assert_eq!(one(&e, "SELECT ABS(-100) FROM t WHERE id = 1"), Value::Int64(100));
    assert_eq!(one(&e, "SELECT -n FROM t WHERE id = 1"), Value::Int64(9223372036854775807));
}

#[test]
fn repeat_huge_count_does_not_panic() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 'abcd')", None).unwrap();
    let v = one(&e, "SELECT REPEAT(s, 9223372036854775807) FROM t WHERE id = 1");
    assert!(matches!(v, Value::Null), "huge REPEAT should be NULL, got {v:?}");
    assert_eq!(one(&e, "SELECT REPEAT(s, 2) FROM t WHERE id = 1"), Value::Text("abcdabcd".into()));
}

#[test]
fn substring_out_of_range_is_safe() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 'hello')", None).unwrap();
    assert_eq!(one(&e, "SELECT SUBSTRING(s, 2, 3) FROM t WHERE id = 1"), Value::Text("ell".into()));
    assert_eq!(one(&e, "SELECT SUBSTRING(s, 100, 5) FROM t WHERE id = 1"), Value::Text("".into()));
    assert_eq!(one(&e, "SELECT SUBSTRING(s, 0, 2) FROM t WHERE id = 1"), Value::Text("he".into()));
}
