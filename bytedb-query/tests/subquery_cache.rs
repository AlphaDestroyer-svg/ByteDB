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
fn ints(e: &QueryEngine, sql: &str) -> Vec<i64> {
    rows(e, sql).into_iter().map(|r| match r[0] { Value::Int64(n) => n, _ => panic!("not int: {:?}", r[0]) }).collect()
}

fn seed(e: &QueryEngine) {
    e.execute_sql("CREATE TABLE big (id INT PRIMARY KEY, v INT)", None).unwrap();
    for i in 1..=10 {
        e.execute_sql(&format!("INSERT INTO big VALUES ({i}, {})", i * 10), None).unwrap();
    }
    e.execute_sql("CREATE TABLE small (id INT PRIMARY KEY, ref INT)", None).unwrap();
    e.execute_sql("INSERT INTO small VALUES (1, 2)", None).unwrap();
    e.execute_sql("INSERT INTO small VALUES (2, 4)", None).unwrap();
    e.execute_sql("INSERT INTO small VALUES (3, 6)", None).unwrap();
}

#[test]
fn constant_in_subquery_correct() {
    let e = engine();
    seed(&e);
    let got = ints(&e, "SELECT id FROM big WHERE id IN (SELECT ref FROM small) ORDER BY id");
    assert_eq!(got, vec![2, 4, 6]);
}

#[test]
fn constant_scalar_subquery_correct() {
    let e = engine();
    seed(&e);
    // (SELECT MAX(ref) FROM small) = 6, constant for every outer row
    let got = ints(&e, "SELECT id FROM big WHERE id > (SELECT MAX(ref) FROM small) ORDER BY id");
    assert_eq!(got, vec![7, 8, 9, 10]);
}

#[test]
fn constant_exists_true_returns_all() {
    let e = engine();
    seed(&e);
    let got = ints(&e, "SELECT id FROM big WHERE EXISTS (SELECT 1 FROM small) ORDER BY id");
    assert_eq!(got, (1..=10).collect::<Vec<_>>());
}

#[test]
fn constant_exists_false_returns_none() {
    let e = engine();
    seed(&e);
    e.execute_sql("CREATE TABLE empty (id INT PRIMARY KEY)", None).unwrap();
    let got = ints(&e, "SELECT id FROM big WHERE EXISTS (SELECT 1 FROM empty) ORDER BY id");
    assert!(got.is_empty());
}

// The critical correctness test: the cache must be cleared between statements.
#[test]
fn cache_invalidated_between_statements() {
    let e = engine();
    seed(&e);
    let before = ints(&e, "SELECT id FROM big WHERE id IN (SELECT ref FROM small) ORDER BY id");
    assert_eq!(before, vec![2, 4, 6]);

    e.execute_sql("INSERT INTO small VALUES (4, 8)", None).unwrap();
    let after = ints(&e, "SELECT id FROM big WHERE id IN (SELECT ref FROM small) ORDER BY id");
    assert_eq!(after, vec![2, 4, 6, 8], "new small row must be visible (cache not stale)");

    e.execute_sql("DELETE FROM small WHERE ref = 2", None).unwrap();
    let after_del = ints(&e, "SELECT id FROM big WHERE id IN (SELECT ref FROM small) ORDER BY id");
    assert_eq!(after_del, vec![4, 6, 8], "deleted small row must drop out");
}

// A correlated subquery must NOT be cached: each outer row gets its own result.
#[test]
fn correlated_not_cached_still_correct() {
    let e = engine();
    seed(&e);
    let got = ints(&e, "SELECT id FROM big WHERE EXISTS (SELECT 1 FROM small WHERE small.ref = big.id) ORDER BY id");
    assert_eq!(got, vec![2, 4, 6]);
}

// Constant subquery nested inside a correlated EXISTS: outer correlated (per-row),
// inner constant. Must produce correct results.
#[test]
fn nested_constant_in_correlated() {
    let e = engine();
    seed(&e);
    let got = ints(
        &e,
        "SELECT id FROM big WHERE EXISTS (SELECT 1 FROM small WHERE small.ref = big.id AND small.ref IN (SELECT ref FROM small)) ORDER BY id",
    );
    assert_eq!(got, vec![2, 4, 6]);
}

// Same constant subquery shape but data changes via UPDATE between statements.
#[test]
fn cache_reflects_update_between_statements() {
    let e = engine();
    seed(&e);
    let before = ints(&e, "SELECT id FROM big WHERE id IN (SELECT ref FROM small) ORDER BY id");
    assert_eq!(before, vec![2, 4, 6]);
    e.execute_sql("UPDATE small SET ref = 8 WHERE id = 1", None).unwrap();
    let after = ints(&e, "SELECT id FROM big WHERE id IN (SELECT ref FROM small) ORDER BY id");
    assert_eq!(after, vec![4, 6, 8]);
}

#[test]
fn two_distinct_constant_subqueries_one_statement() {
    let e = engine();
    seed(&e);
    let got = ints(
        &e,
        "SELECT id FROM big WHERE id IN (SELECT ref FROM small) OR id IN (SELECT id FROM small) ORDER BY id",
    );
    // small.ref = {2,4,6}, small.id = {1,2,3} -> union {1,2,3,4,6}
    assert_eq!(got, vec![1, 2, 3, 4, 6]);
}
