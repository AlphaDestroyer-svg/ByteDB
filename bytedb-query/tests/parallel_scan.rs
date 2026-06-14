use std::sync::Arc;

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_core::tuple::value::Value;
use bytedb_query::executor::context::{QueryContext, ResourceLimits};
use bytedb_query::executor::engine::{ExecutionResult, QueryEngine};

fn engine() -> QueryEngine {
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    QueryEngine::new(db, txn)
}

fn seed_large(e: &QueryEngine, n: i64) {
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT, g INT)", None).unwrap();
    for i in 0..n {
        e.execute_sql(&format!("INSERT INTO t VALUES ({i}, {}, {})", i % 1000, i % 7), None).unwrap();
    }
}

fn ids(res: ExecutionResult) -> Vec<i64> {
    match res {
        ExecutionResult::Rows { rows, .. } => {
            let mut v: Vec<i64> = rows.into_iter()
                .filter_map(|r| r.first().and_then(|x| x.as_i64()))
                .collect();
            v.sort_unstable();
            v
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn parallel_scan_matches_expected() {
    let e = engine();
    let n = 20000;
    seed_large(&e, n);

    let got = ids(e.execute_sql("SELECT id FROM t WHERE v = 500", None).unwrap());
    let expected: Vec<i64> = (0..n).filter(|i| i % 1000 == 500).collect();
    assert_eq!(got, expected);
}

#[test]
fn parallel_scan_complex_predicate() {
    let e = engine();
    let n = 20000;
    seed_large(&e, n);

    let got = ids(e.execute_sql("SELECT id FROM t WHERE v > 100 AND g = 3", None).unwrap());
    let expected: Vec<i64> = (0..n).filter(|i| i % 1000 > 100 && i % 7 == 3).collect();
    assert_eq!(got, expected);
}

#[test]
fn parallel_scan_row_count_matches_sequential_semantics() {
    let e = engine();
    let n = 20000;
    seed_large(&e, n);

    let count = match e.execute_sql("SELECT id FROM t WHERE g = 0", None).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows.len(),
        _ => panic!(),
    };
    let expected = (0..n).filter(|i| i % 7 == 0).count();
    assert_eq!(count, expected);
}

#[test]
fn parallel_scan_respects_row_limit() {
    let e = engine();
    seed_large(&e, 20000);

    let ctx = QueryContext::with_limits(ResourceLimits::UNLIMITED.with_scan_rows(5000));
    let r = e.execute_sql_with_ctx("SELECT id FROM t WHERE v = 1", None, ctx);
    assert!(r.is_err(), "row limit must trip on the parallel scan path");
}

#[test]
fn parallel_scan_respects_cancel() {
    let e = engine();
    seed_large(&e, 20000);

    let ctx = QueryContext::new();
    ctx.cancel();
    let r = e.execute_sql_with_ctx("SELECT id FROM t WHERE v = 1", None, ctx);
    assert!(r.is_err(), "cancelled query must not return rows");
}

#[test]
fn aggregate_over_large_table_is_correct() {
    let e = engine();
    let n = 20000i64;
    seed_large(&e, n);

    match e.execute_sql("SELECT g, COUNT(*) FROM t WHERE v < 500 GROUP BY g", None).unwrap() {
        ExecutionResult::Rows { rows, columns } => {
            let gi = columns.iter().position(|c| c == "g").unwrap();
            let ci = columns.len() - 1;
            let mut total = 0i64;
            for r in &rows {
                let _ = &r[gi];
                total += r[ci].as_i64().unwrap();
            }
            let expected = (0..n).filter(|i| i % 1000 < 500).count() as i64;
            assert_eq!(total, expected);
        }
        other => panic!("expected rows, got {other:?}"),
    }
    let _ = Value::Null;
}
