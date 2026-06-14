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
        ExecutionResult::Rows { rows, .. } => rows.into_iter().next().unwrap(),
        other => panic!("{other:?}"),
    }
}

#[test]
fn string_literals_coerced_to_numeric_and_bool_columns() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, n INT, f FLOAT, b BOOL)", None).unwrap();
    e.execute_sql("INSERT INTO t (id, n, f, b) VALUES (1, '42', '3.5', 'true')", None).unwrap();
    assert_eq!(
        row(&e, "SELECT n, f, b FROM t WHERE id = 1"),
        vec![Value::Int64(42), Value::Float64(3.5), Value::Bool(true)]
    );
    // The coerced int participates in integer comparison and aggregation.
    assert_eq!(row(&e, "SELECT id FROM t WHERE n = 42"), vec![Value::Int64(1)]);
    assert_eq!(row(&e, "SELECT SUM(n) FROM t"), vec![Value::Int64(42)]);
}
