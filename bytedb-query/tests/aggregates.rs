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
fn sum_does_not_silently_overflow() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, n BIGINT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 9223372036854775807)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2, 1)", None).unwrap();

    let s = one(&e, "SELECT SUM(n) FROM t");
    let approx = match s {
        Value::Int64(v) => v as f64,
        Value::Float64(v) => v,
        other => panic!("unexpected SUM type {other:?}"),
    };
    assert!(approx > 9.2e18, "SUM wrapped to {approx} (should be ~9.22e18)");
}

#[test]
fn sum_within_range_stays_integer() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, n INT)", None).unwrap();
    for i in 1..=5 {
        e.execute_sql(&format!("INSERT INTO t VALUES ({i}, {})", i * 10), None).unwrap();
    }
    assert_eq!(one(&e, "SELECT SUM(n) FROM t"), Value::Int64(150));
}

#[test]
fn group_by_sum_overflow_safe() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, g INT, n BIGINT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 1, 9223372036854775807)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2, 1, 9223372036854775807)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (3, 2, 5)", None).unwrap();

    let r = e.execute_sql("SELECT g, SUM(n) FROM t GROUP BY g ORDER BY g", None).unwrap();
    if let ExecutionResult::Rows { rows, .. } = r {
        let g1 = match rows[0][1] {
            Value::Int64(v) => v as f64,
            Value::Float64(v) => v,
            _ => panic!(),
        };
        assert!(g1 > 1.8e19, "group SUM wrapped: {g1}");
        assert_eq!(rows[1][1], Value::Int64(5));
    } else {
        panic!("expected rows");
    }
}

#[test]
fn join_with_smaller_left_side() {
    let e = engine();
    e.execute_sql("CREATE TABLE big (id INT PRIMARY KEY, v INT)", None).unwrap();
    e.execute_sql("CREATE TABLE small (id INT PRIMARY KEY, label TEXT)", None).unwrap();
    for i in 1..=200 {
        e.execute_sql(&format!("INSERT INTO big VALUES ({i}, {})", i % 5), None).unwrap();
    }
    e.execute_sql("INSERT INTO small VALUES (1, 'a')", None).unwrap();
    e.execute_sql("INSERT INTO small VALUES (2, 'b')", None).unwrap();

    let r = e
        .execute_sql("SELECT big.id FROM small JOIN big ON small.id = big.id", None)
        .unwrap();
    if let ExecutionResult::Rows { rows, .. } = r {
        assert_eq!(rows.len(), 2, "inner join should match exactly 2 rows");
    } else {
        panic!("expected rows");
    }
}
