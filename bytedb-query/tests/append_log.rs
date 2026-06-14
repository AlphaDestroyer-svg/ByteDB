use std::path::PathBuf;
use std::sync::Arc;

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_core::tuple::value::Value;
use bytedb_query::executor::diskstore::DiskStore;
use bytedb_query::executor::engine::{ExecutionResult, QueryEngine};

fn tmp(tag: &str) -> PathBuf {
    let mut d = std::env::temp_dir();
    d.push(format!("bytedb_applog_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn open(dir: &PathBuf) -> QueryEngine {
    let ds = DiskStore::open(dir.clone(), "test").unwrap();
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    let mut e = QueryEngine::new(db, txn);
    e.attach_disk_store(ds);
    e
}

fn ids_vals(e: &QueryEngine, sql: &str) -> Vec<(i64, i64)> {
    match e.execute_sql(sql, None).unwrap() {
        ExecutionResult::Rows { rows, .. } => {
            let mut v: Vec<(i64, i64)> = rows.into_iter()
                .filter_map(|r| Some((r.first()?.as_i64()?, r.get(1)?.as_i64()?)))
                .collect();
            v.sort_unstable();
            v
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn insert_update_delete_survive_reopen() {
    let dir = tmp("reopen");
    {
        let e = open(&dir);
        e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
        for i in 0..50 {
            e.execute_sql(&format!("INSERT INTO t VALUES ({i}, {})", i * 10), None).unwrap();
        }
        e.execute_sql("UPDATE t SET v = 999 WHERE id = 7", None).unwrap();
        e.execute_sql("DELETE FROM t WHERE id = 3", None).unwrap();
    }
    {
        let e = open(&dir);
        let got = ids_vals(&e, "SELECT id, v FROM t");
        assert_eq!(got.len(), 49);
        assert!(!got.iter().any(|(id, _)| *id == 3));
        assert!(got.contains(&(7, 999)));
        assert!(got.contains(&(0, 0)));
        assert!(got.contains(&(49, 490)));
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn insert_does_not_rewrite_whole_table_file() {
    let dir = tmp("noamp");
    let e = open(&dir);
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
    for i in 0..200 {
        e.execute_sql(&format!("INSERT INTO t VALUES ({i}, {i})"), None).unwrap();
    }
    let tbl = dir.join("databases").join("test").join("tables").join("t.tbl");
    let log = dir.join("databases").join("test").join("tables").join("t.log");
    let tbl_len = std::fs::metadata(&tbl).map(|m| m.len()).unwrap_or(0);
    let log_len = std::fs::metadata(&log).map(|m| m.len()).unwrap_or(0);
    assert!(tbl_len < 200, "table file should stay tiny (no full rewrites), got {tbl_len}");
    assert!(log_len > 200, "log should hold the deltas, got {log_len}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn delete_on_one_table_does_not_touch_another() {
    let dir = tmp("isolation");
    let e = open(&dir);
    e.execute_sql("CREATE TABLE a (id INT PRIMARY KEY, v INT)", None).unwrap();
    e.execute_sql("CREATE TABLE b (id INT PRIMARY KEY, v INT)", None).unwrap();
    e.execute_sql("INSERT INTO a VALUES (1, 1)", None).unwrap();
    e.execute_sql("INSERT INTO b VALUES (1, 1)", None).unwrap();

    let b_log = dir.join("databases").join("test").join("tables").join("b.log");
    let before = std::fs::metadata(&b_log).map(|m| m.len()).unwrap_or(0);

    e.execute_sql("DELETE FROM a WHERE id = 1", None).unwrap();

    let after = std::fs::metadata(&b_log).map(|m| m.len()).unwrap_or(0);
    assert_eq!(before, after, "deleting from table a must not write table b's log");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn fold_correctness_with_overwrites_across_reopen() {
    let dir = tmp("overwrite");
    {
        let e = open(&dir);
        e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
        e.execute_sql("INSERT INTO t VALUES (1, 1)", None).unwrap();
        e.execute_sql("UPDATE t SET v = 2 WHERE id = 1", None).unwrap();
        e.execute_sql("UPDATE t SET v = 3 WHERE id = 1", None).unwrap();
        e.execute_sql("INSERT INTO t VALUES (2, 20)", None).unwrap();
        e.execute_sql("DELETE FROM t WHERE id = 2", None).unwrap();
        e.execute_sql("INSERT INTO t VALUES (2, 99)", None).unwrap();
    }
    {
        let e = open(&dir);
        let got = ids_vals(&e, "SELECT id, v FROM t");
        assert_eq!(got, vec![(1, 3), (2, 99)]);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn secondary_index_consistent_after_reopen_with_log() {
    let dir = tmp("secidx");
    {
        let e = open(&dir);
        e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, c INT)", None).unwrap();
        e.execute_sql("CREATE INDEX ix ON t (c)", None).unwrap();
        for i in 0..30 {
            e.execute_sql(&format!("INSERT INTO t VALUES ({i}, {})", i % 3), None).unwrap();
        }
        e.execute_sql("DELETE FROM t WHERE id = 0", None).unwrap();
    }
    {
        let e = open(&dir);
        let got = ids_vals(&e, "SELECT id, c FROM t WHERE c = 0");
        let expected: Vec<(i64, i64)> = (1..30).filter(|i| i % 3 == 0).map(|i| (i, 0)).collect();
        assert_eq!(got, expected);
    }
    let _ = std::fs::remove_dir_all(&dir);
    let _ = Value::Null;
}
