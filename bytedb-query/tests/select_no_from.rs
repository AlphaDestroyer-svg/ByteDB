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
fn rows(e: &QueryEngine, sql: &str) -> Vec<Vec<Value>> {
    match e.execute_sql(sql, None).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows,
        other => panic!("{other:?}"),
    }
}

#[test]
fn select_literal() {
    let e = engine();
    assert_eq!(rows(&e, "SELECT 50"), vec![vec![Value::Int64(50)]]);
}

#[test]
fn select_arithmetic() {
    let e = engine();
    assert_eq!(rows(&e, "SELECT 1 + 1"), vec![vec![Value::Int64(2)]]);
    assert_eq!(rows(&e, "SELECT 2 * 3 + 4"), vec![vec![Value::Int64(10)]]);
}

#[test]
fn select_multiple_columns_with_aliases() {
    let e = engine();
    match e.execute_sql("SELECT 1 AS a, 'hi' AS b", None).unwrap() {
        ExecutionResult::Rows { columns, rows } => {
            assert_eq!(columns, vec!["a".to_string(), "b".to_string()]);
            assert_eq!(rows, vec![vec![Value::Int64(1), Value::Text("hi".into())]]);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn select_function_no_from() {
    let e = engine();
    assert_eq!(rows(&e, "SELECT UPPER('abc')"), vec![vec![Value::Text("ABC".into())]]);
    assert_eq!(rows(&e, "SELECT ABS(-7)"), vec![vec![Value::Int64(7)]]);
}

#[test]
fn select_star_no_from_errors() {
    let e = engine();
    assert!(e.execute_sql("SELECT *", None).is_err());
}

#[test]
fn scalar_subquery_no_from() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2)", None).unwrap();
    assert_eq!(rows(&e, "SELECT (SELECT COUNT(*) FROM t)"), vec![vec![Value::Int64(2)]]);
}

#[test]
fn from_less_subquery_in_where() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY)", None).unwrap();
    for i in 1..=5 { e.execute_sql(&format!("INSERT INTO t VALUES ({i})"), None).unwrap(); }
    let got: Vec<i64> = rows(&e, "SELECT id FROM t WHERE id > (SELECT 2) ORDER BY id")
        .into_iter().map(|r| match r[0] { Value::Int64(n) => n, _ => panic!() }).collect();
    assert_eq!(got, vec![3, 4, 5]);
}
