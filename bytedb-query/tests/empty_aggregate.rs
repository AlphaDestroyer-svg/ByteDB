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
fn row(e: &QueryEngine, sql: &str) -> Vec<Value> {
    match e.execute_sql(sql, None).unwrap() {
        ExecutionResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1, "ungrouped aggregate must return exactly one row for {sql}");
            rows.into_iter().next().unwrap()
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn count_star_on_empty_table_is_zero() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
    assert_eq!(row(&e, "SELECT COUNT(*) FROM t"), vec![Value::Int64(0)]);
}

#[test]
fn other_aggregates_on_empty_table_are_null() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
    assert_eq!(row(&e, "SELECT SUM(v) FROM t"), vec![Value::Null]);
    assert_eq!(row(&e, "SELECT AVG(v) FROM t"), vec![Value::Null]);
    assert_eq!(row(&e, "SELECT MIN(v) FROM t"), vec![Value::Null]);
    assert_eq!(row(&e, "SELECT MAX(v) FROM t"), vec![Value::Null]);
}

#[test]
fn multi_aggregate_on_empty_table() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
    assert_eq!(
        row(&e, "SELECT COUNT(*), SUM(v), MAX(v) FROM t"),
        vec![Value::Int64(0), Value::Null, Value::Null]
    );
}

#[test]
fn sum_of_all_nulls_is_null() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, NULL)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2, NULL)", None).unwrap();
    assert_eq!(row(&e, "SELECT SUM(v) FROM t"), vec![Value::Null]);
    assert_eq!(row(&e, "SELECT COUNT(*) FROM t"), vec![Value::Int64(2)]);
}
