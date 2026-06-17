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
fn one(e: &QueryEngine, sql: &str) -> Value {
    let r = rows(e, sql);
    assert_eq!(r.len(), 1, "expected one row for {sql}");
    r[0][0].clone()
}

fn seed(e: &QueryEngine) {
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)", None).unwrap();
    for (i, g, v) in [(1, 1, 10), (2, 1, 20), (3, 2, 5), (4, 2, 15), (5, 2, 25)] {
        e.execute_sql(&format!("INSERT INTO t VALUES ({i},{g},{v})"), None).unwrap();
    }
}

#[test]
fn scalar_arithmetic_over_aggregate_no_group() {
    let e = engine();
    seed(&e);
    assert_eq!(one(&e, "SELECT MAX(v) * 10 FROM t"), Value::Int64(250));
    assert_eq!(one(&e, "SELECT SUM(v) + 1 FROM t"), Value::Int64(76));
    assert_eq!(one(&e, "SELECT COUNT(*) * 2 FROM t"), Value::Int64(10));
    assert_eq!(one(&e, "SELECT MAX(v) - MIN(v) FROM t"), Value::Int64(20));
}

#[test]
fn arithmetic_over_aggregate_with_group() {
    let e = engine();
    seed(&e);
    let r = rows(&e, "SELECT g, SUM(v) * 2 FROM t GROUP BY g ORDER BY g");
    assert_eq!(r, vec![
        vec![Value::Int64(1), Value::Int64(60)],
        vec![Value::Int64(2), Value::Int64(90)],
    ]);
}

#[test]
fn multiple_aggregates_in_one_expression() {
    let e = engine();
    seed(&e);
    let r = rows(&e, "SELECT g, MAX(v) - MIN(v) FROM t GROUP BY g ORDER BY g");
    assert_eq!(r, vec![
        vec![Value::Int64(1), Value::Int64(10)],
        vec![Value::Int64(2), Value::Int64(20)],
    ]);
}

#[test]
fn aggregate_and_its_expression_together() {
    let e = engine();
    seed(&e);
    // SUM(v) appears twice: bare and in an expression; computed once, used twice
    let r = rows(&e, "SELECT SUM(v), SUM(v) + 100 FROM t");
    assert_eq!(r, vec![vec![Value::Int64(75), Value::Int64(175)]]);
}

#[test]
fn aggregate_in_scalar_subquery_expression() {
    let e = engine();
    seed(&e);
    // nested-aggregate expression inside a constant scalar subquery
    let r = rows(&e, "SELECT id FROM t WHERE v > (SELECT MAX(v) / 2 FROM t) ORDER BY id");
    // MAX(v)/2 = 25/2 = 12 ; v in {10,20,5,15,25} > 12 -> ids 2,4,5
    assert_eq!(r, vec![vec![Value::Int64(2)], vec![Value::Int64(4)], vec![Value::Int64(5)]]);
}

#[test]
fn having_aggregate_not_in_select() {
    let e = engine();
    seed(&e);
    // g1: sum=30,count=2,max-min=10 ; g2: sum=45,count=3,max-min=20
    let r = rows(&e, "SELECT g FROM t GROUP BY g HAVING SUM(v) > 30 ORDER BY g");
    assert_eq!(r, vec![vec![Value::Int64(2)]]);

    let r = rows(&e, "SELECT g FROM t GROUP BY g HAVING COUNT(*) > 2 ORDER BY g");
    assert_eq!(r, vec![vec![Value::Int64(2)]]);

    let r = rows(&e, "SELECT g FROM t GROUP BY g HAVING MAX(v) - MIN(v) > 15 ORDER BY g");
    assert_eq!(r, vec![vec![Value::Int64(2)]]);
}

#[test]
fn having_aggregate_also_in_select() {
    let e = engine();
    seed(&e);
    let r = rows(&e, "SELECT g, SUM(v) FROM t GROUP BY g HAVING SUM(v) > 30 ORDER BY g");
    assert_eq!(r, vec![vec![Value::Int64(2), Value::Int64(45)]]);
}

#[test]
fn plain_aggregates_still_work() {
    let e = engine();
    seed(&e);
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), Value::Int64(5));
    assert_eq!(one(&e, "SELECT SUM(v) FROM t"), Value::Int64(75));
    let r = rows(&e, "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY g");
    assert_eq!(r, vec![
        vec![Value::Int64(1), Value::Int64(2)],
        vec![Value::Int64(2), Value::Int64(3)],
    ]);
}
