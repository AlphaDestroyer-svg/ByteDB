use std::sync::Arc;
use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_query::executor::engine::{ExecutionResult, QueryEngine};

fn engine() -> QueryEngine {
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    QueryEngine::new(db, txn)
}
fn ids(e: &QueryEngine, sql: &str) -> Vec<i64> {
    match e.execute_sql(sql, None).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.into_iter().filter_map(|r| r[0].as_i64()).collect(),
        other => panic!("{other:?}"),
    }
}
fn seed(e: &QueryEngine) {
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 10)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2, 20)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (3, NULL)", None).unwrap();
}

#[test]
fn not_of_null_comparison_excludes_row() {
    let e = engine();
    seed(&e);
    assert_eq!(ids(&e, "SELECT id FROM t WHERE NOT (v > 15) ORDER BY id"), vec![1]);
}

#[test]
fn not_equal_excludes_null() {
    let e = engine();
    seed(&e);
    assert_eq!(ids(&e, "SELECT id FROM t WHERE v <> 10 ORDER BY id"), vec![2]);
}

#[test]
fn not_in_with_null_excludes_everything() {
    let e = engine();
    seed(&e);
    assert_eq!(ids(&e, "SELECT id FROM t WHERE v NOT IN (10, NULL) ORDER BY id"), Vec::<i64>::new());
    assert_eq!(ids(&e, "SELECT id FROM t WHERE v NOT IN (10) ORDER BY id"), vec![2]);
}

#[test]
fn between_excludes_null() {
    let e = engine();
    seed(&e);
    assert_eq!(ids(&e, "SELECT id FROM t WHERE v BETWEEN 5 AND 15 ORDER BY id"), vec![1]);
}

#[test]
fn column_check_enforced_on_insert_and_update() {
    let e = engine();
    e.execute_sql("CREATE TABLE c (id INT PRIMARY KEY, age INT CHECK (age >= 0))", None).unwrap();
    assert!(e.execute_sql("INSERT INTO c VALUES (1, 5)", None).is_ok());
    assert!(e.execute_sql("INSERT INTO c VALUES (2, -1)", None).is_err());
    assert!(e.execute_sql("INSERT INTO c VALUES (3, NULL)", None).is_ok());
    assert!(e.execute_sql("UPDATE c SET age = -5 WHERE id = 1", None).is_err());
}
