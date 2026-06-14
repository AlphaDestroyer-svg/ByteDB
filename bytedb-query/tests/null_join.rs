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
fn seed(e: &QueryEngine) {
    e.execute_sql("CREATE TABLE a (id INT PRIMARY KEY, k INT)", None).unwrap();
    e.execute_sql("CREATE TABLE b (id INT PRIMARY KEY, k INT)", None).unwrap();
    e.execute_sql("INSERT INTO a VALUES (1, NULL)", None).unwrap();
    e.execute_sql("INSERT INTO a VALUES (2, 5)", None).unwrap();
    e.execute_sql("INSERT INTO b VALUES (1, NULL)", None).unwrap();
    e.execute_sql("INSERT INTO b VALUES (2, 5)", None).unwrap();
}

#[test]
fn null_keys_do_not_match_in_inner_join() {
    let e = engine();
    seed(&e);
    let r = rows(&e, "SELECT a.id FROM a JOIN b ON a.k = b.k ORDER BY a.id");
    assert_eq!(r, vec![vec![Value::Int64(2)]]);
}

#[test]
fn left_join_keeps_null_keyed_row_unmatched() {
    let e = engine();
    seed(&e);
    let r = rows(&e, "SELECT a.id, b.id FROM a LEFT JOIN b ON a.k = b.k ORDER BY a.id");
    assert_eq!(
        r,
        vec![
            vec![Value::Int64(1), Value::Null],
            vec![Value::Int64(2), Value::Int64(2)],
        ]
    );
}
