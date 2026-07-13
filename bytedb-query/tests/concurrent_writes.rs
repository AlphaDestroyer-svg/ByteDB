use std::sync::Arc;
use std::thread;

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_query::executor::engine::{ExecutionResult, QueryEngine};

fn engine() -> Arc<QueryEngine> {
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    Arc::new(QueryEngine::new(db, txn))
}

fn count(e: &QueryEngine, sql: &str) -> i64 {
    match e.execute_sql(sql, None).unwrap() {
        ExecutionResult::Rows { rows, .. } => match rows[0][0] {
            bytedb_core::tuple::value::Value::Int64(n) => n,
            _ => panic!("not int"),
        },
        other => panic!("{other:?}"),
    }
}

#[test]
fn concurrent_duplicate_pk_inserts_only_one_wins() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, worker INT)", None).unwrap();

    let threads = 16;
    let mut handles = Vec::new();
    for w in 0..threads {
        let e = Arc::clone(&e);
        handles.push(thread::spawn(move || {
            e.execute_sql(&format!("INSERT INTO t VALUES (1, {w})"), None).is_ok()
        }));
    }
    let successes: usize = handles.into_iter().map(|h| h.join().unwrap()).filter(|&ok| ok).count();

    assert_eq!(successes, 1, "exactly one INSERT of PK=1 must succeed, got {successes}");
    assert_eq!(count(&e, "SELECT COUNT(*) FROM t"), 1, "table must hold exactly one row");
}

#[test]
fn concurrent_duplicate_unique_inserts_only_one_wins() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)", None).unwrap();

    let threads = 16;
    let mut handles = Vec::new();
    for id in 0..threads {
        let e = Arc::clone(&e);
        handles.push(thread::spawn(move || {
            // distinct PK, same UNIQUE value -> only one may commit
            e.execute_sql(&format!("INSERT INTO t VALUES ({id}, 'same@x.com')"), None).is_ok()
        }));
    }
    let successes: usize = handles.into_iter().map(|h| h.join().unwrap()).filter(|&ok| ok).count();

    assert_eq!(successes, 1, "exactly one INSERT of the unique email must succeed, got {successes}");
    assert_eq!(count(&e, "SELECT COUNT(*) FROM t"), 1, "table must hold exactly one row");
}

#[test]
fn concurrent_distinct_inserts_all_succeed() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();

    let threads = 32;
    let mut handles = Vec::new();
    for id in 0..threads {
        let e = Arc::clone(&e);
        handles.push(thread::spawn(move || {
            e.execute_sql(&format!("INSERT INTO t VALUES ({id}, {})", id * 10), None).is_ok()
        }));
    }
    let successes: usize = handles.into_iter().map(|h| h.join().unwrap()).filter(|&ok| ok).count();

    assert_eq!(successes, threads as usize, "all distinct-key inserts must succeed");
    assert_eq!(count(&e, "SELECT COUNT(*) FROM t"), threads, "all rows must be present");
}

#[test]
fn concurrent_inserts_and_reads_stay_consistent() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
    for id in 0..50 {
        e.execute_sql(&format!("INSERT INTO t VALUES ({id}, {id})"), None).unwrap();
    }

    let mut handles = Vec::new();
    // writers add new distinct rows
    for id in 50..90 {
        let e = Arc::clone(&e);
        handles.push(thread::spawn(move || {
            let _ = e.execute_sql(&format!("INSERT INTO t VALUES ({id}, {id})"), None);
        }));
    }
    // concurrent readers must never crash or see a torn state
    for _ in 0..8 {
        let e = Arc::clone(&e);
        handles.push(thread::spawn(move || {
            for _ in 0..20 {
                let _ = e.execute_sql("SELECT COUNT(*) FROM t", None);
                let _ = e.execute_sql("SELECT SUM(v) FROM t", None);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    assert_eq!(count(&e, "SELECT COUNT(*) FROM t"), 90, "50 seed + 40 concurrent inserts");
}
