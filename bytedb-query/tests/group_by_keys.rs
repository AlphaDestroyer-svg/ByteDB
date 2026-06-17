use std::sync::Arc;
use std::collections::HashMap;
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

fn count_for(result: &[Vec<Value>], a: &str, b: &str) -> Option<i64> {
    for r in result {
        let ka = match &r[0] { Value::Text(s) => s.as_str(), _ => continue };
        let kb = match &r[1] { Value::Text(s) => s.as_str(), _ => continue };
        if ka == a && kb == b {
            if let Value::Int64(c) = r[2] { return Some(c); }
        }
    }
    None
}

#[test]
fn multi_column_group_keys_do_not_collide() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 'a', 'b')", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2, 'ab', '')", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (3, 'a', 'b')", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (4, '', 'ab')", None).unwrap();

    let result = rows(&e, "SELECT a, b, COUNT(*) FROM t GROUP BY a, b");
    assert_eq!(result.len(), 3, "three distinct group tuples expected");
    assert_eq!(count_for(&result, "a", "b"), Some(2));
    assert_eq!(count_for(&result, "ab", ""), Some(1));
    assert_eq!(count_for(&result, "", "ab"), Some(1));
}

#[test]
fn group_by_with_embedded_separator_bytes() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 'x', 'y')", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2, 'x', 'y')", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (3, 'xy', 'z')", None).unwrap();

    let result = rows(&e, "SELECT a, b, COUNT(*) FROM t GROUP BY a, b");
    assert_eq!(result.len(), 2);
    assert_eq!(count_for(&result, "x", "y"), Some(2));
    assert_eq!(count_for(&result, "xy", "z"), Some(1));
}

#[test]
fn group_by_null_distinct_from_empty_string() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, a TEXT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, '')", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2, NULL)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (3, '')", None).unwrap();

    let result = rows(&e, "SELECT a, COUNT(*) FROM t GROUP BY a");
    let mut counts: HashMap<bool, i64> = HashMap::new();
    for r in &result {
        let is_null = matches!(r[0], Value::Null);
        if let Value::Int64(c) = r[1] {
            counts.insert(is_null, c);
        }
    }
    assert_eq!(result.len(), 2, "NULL and empty string must be distinct groups");
    assert_eq!(counts.get(&false), Some(&2));
    assert_eq!(counts.get(&true), Some(&1));
}

#[test]
fn group_by_aggregates_on_different_columns() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, g INT, a INT, b INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 1, 100, 5)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2, 1, 200, 9)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (3, 2, 7, 3)", None).unwrap();

    let result = rows(&e, "SELECT g, SUM(a), MAX(b) FROM t GROUP BY g");
    assert_eq!(result.len(), 2);
    for r in &result {
        match r[0] {
            Value::Int64(1) => {
                assert_eq!(r[1], Value::Int64(300));
                assert_eq!(r[2], Value::Int64(9));
            }
            Value::Int64(2) => {
                assert_eq!(r[1], Value::Int64(7));
                assert_eq!(r[2], Value::Int64(3));
            }
            _ => panic!("unexpected group {:?}", r[0]),
        }
    }
}

#[test]
fn group_by_int_and_text_multi_aggregate() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, g INT, label TEXT, v INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 10, 'p', 5)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2, 10, 'p', 7)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (3, 20, 'q', 3)", None).unwrap();

    let result = rows(&e, "SELECT g, label, COUNT(*), SUM(v), MIN(v), MAX(v) FROM t GROUP BY g, label");
    assert_eq!(result.len(), 2);
    for r in &result {
        match r[0] {
            Value::Int64(10) => {
                assert_eq!(r[2], Value::Int64(2));
                assert_eq!(r[3], Value::Int64(12));
                assert_eq!(r[4], Value::Int64(5));
                assert_eq!(r[5], Value::Int64(7));
            }
            Value::Int64(20) => {
                assert_eq!(r[2], Value::Int64(1));
                assert_eq!(r[3], Value::Int64(3));
                assert_eq!(r[4], Value::Int64(3));
                assert_eq!(r[5], Value::Int64(3));
            }
            _ => panic!("unexpected group {:?}", r[0]),
        }
    }
}
