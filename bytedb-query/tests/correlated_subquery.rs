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
    e.execute_sql("CREATE TABLE emp (id INT PRIMARY KEY, dept INT, sal INT)", None).unwrap();
    for (i, d, s) in [(1, 10, 100), (2, 10, 200), (3, 20, 150)] {
        e.execute_sql(&format!("INSERT INTO emp VALUES ({i},{d},{s})"), None).unwrap();
    }
    e.execute_sql("CREATE TABLE dept (id INT PRIMARY KEY, dname TEXT)", None).unwrap();
    for (i, n) in [(10, "eng"), (20, "sales"), (30, "hr")] {
        e.execute_sql(&format!("INSERT INTO dept VALUES ({i},'{n}')"), None).unwrap();
    }
}

#[test]
fn correlated_exists_in_where() {
    let e = engine();
    seed(&e);
    let r = rows(&e, "SELECT dname FROM dept WHERE EXISTS (SELECT 1 FROM emp WHERE emp.dept = dept.id) ORDER BY id");
    assert_eq!(r, vec![vec![Value::Text("eng".into())], vec![Value::Text("sales".into())]]);
}

#[test]
fn correlated_not_exists() {
    let e = engine();
    seed(&e);
    let r = rows(&e, "SELECT dname FROM dept WHERE NOT EXISTS (SELECT 1 FROM emp WHERE emp.dept = dept.id) ORDER BY id");
    assert_eq!(r, vec![vec![Value::Text("hr".into())]]);
}

#[test]
fn correlated_scalar_subquery_in_projection() {
    let e = engine();
    seed(&e);
    let r = rows(&e, "SELECT (SELECT COUNT(*) FROM emp WHERE emp.dept = dept.id) FROM dept ORDER BY id");
    assert_eq!(r, vec![vec![Value::Int64(2)], vec![Value::Int64(1)], vec![Value::Int64(0)]]);
}
