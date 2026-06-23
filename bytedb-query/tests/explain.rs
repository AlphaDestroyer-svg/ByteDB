use std::sync::Arc;
use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_core::tuple::value::Value;
use bytedb_query::executor::engine::{ExecutionResult, QueryEngine};

fn engine() -> QueryEngine {
    QueryEngine::new(Arc::new(Database::new("test")), Arc::new(TransactionManager::new()))
}

fn plan_text(e: &QueryEngine, sql: &str) -> String {
    match e.execute_sql(sql, None).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.iter()
            .map(|r| if let Value::Text(s) = &r[0] { s.clone() } else { String::new() })
            .collect::<Vec<_>>()
            .join("\n"),
        other => panic!("{other:?}"),
    }
}

fn seed(e: &QueryEngine) {
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)", None).unwrap();
    for i in 1..=20 {
        e.execute_sql(&format!("INSERT INTO t VALUES ({i}, {}, {})", i % 3, i * 10), None).unwrap();
    }
}

#[test]
fn explain_shows_estimated_rows_and_cost() {
    let e = engine();
    seed(&e);
    let p = plan_text(&e, "EXPLAIN SELECT * FROM t WHERE v > 50");
    assert!(p.contains("estimated_rows="), "must show estimate: {p}");
    assert!(p.contains("estimated_cost="), "must show cost: {p}");
    assert!(p.contains("Seq Scan on t"), "must show scan node: {p}");
    assert!(p.contains("filter:"), "must show filter predicate: {p}");
}

#[test]
fn explain_analyze_shows_actual_rows_and_time() {
    let e = engine();
    seed(&e);
    let p = plan_text(&e, "EXPLAIN ANALYZE SELECT * FROM t WHERE v > 100");
    assert!(p.contains("Actual"), "ANALYZE must show actual stats: {p}");
    assert!(p.contains("rows="), "must show actual rows: {p}");
    assert!(p.contains("time="), "must show timing: {p}");
}

#[test]
fn explain_aggregate_shows_group_by() {
    let e = engine();
    seed(&e);
    let p = plan_text(&e, "EXPLAIN SELECT g, COUNT(*) FROM t GROUP BY g");
    assert!(p.contains("Aggregate"), "must show aggregate node: {p}");
    assert!(p.contains("group by"), "must show group keys: {p}");
}

#[test]
fn explain_sort_shows_order() {
    let e = engine();
    seed(&e);
    let p = plan_text(&e, "EXPLAIN SELECT * FROM t ORDER BY v DESC");
    assert!(p.contains("Sort"), "must show sort node: {p}");
    assert!(p.contains("DESC"), "must show sort direction: {p}");
}

#[test]
fn explain_does_not_execute_dml() {
    let e = engine();
    seed(&e);
    let before = match e.execute_sql("SELECT * FROM t", None).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.len(),
        other => panic!("{other:?}"),
    };
    // EXPLAIN (without ANALYZE) must not run the query
    let _ = plan_text(&e, "EXPLAIN DELETE FROM t WHERE id = 1");
    let after = match e.execute_sql("SELECT * FROM t", None).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.len(),
        other => panic!("{other:?}"),
    };
    assert_eq!(before, after, "EXPLAIN without ANALYZE must not mutate data");
}
