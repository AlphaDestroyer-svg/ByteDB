use std::sync::Arc;

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_query::executor::diskstore::DiskStore;
use bytedb_query::executor::engine::{ExecutionResult, QueryEngine};

fn engine() -> QueryEngine {
    QueryEngine::new(Arc::new(Database::new("t")), Arc::new(TransactionManager::new()))
}

fn rows(e: &QueryEngine, sql: &str) -> Vec<Vec<bytedb_core::tuple::value::Value>> {
    match e.execute_sql(sql, None).unwrap() {
        ExecutionResult::Rows { rows, .. } => rows,
        o => panic!("{o:?}"),
    }
}

fn ints(e: &QueryEngine, sql: &str, col: usize) -> Vec<i64> {
    let mut v: Vec<i64> = rows(e, sql).into_iter().filter_map(|r| r.get(col).and_then(|x| x.as_i64())).collect();
    v.sort_unstable();
    v
}

#[test]
fn no_pk_table_holds_many_rows() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (a INT, b INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 10)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2, 20)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (3, 30)", None).unwrap();
    assert_eq!(ints(&e, "SELECT a FROM t", 0), vec![1, 2, 3]);
}

#[test]
fn no_pk_duplicate_rows_allowed() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (a INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (5)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (5)", None).unwrap();
    assert_eq!(rows(&e, "SELECT a FROM t").len(), 2);
}

#[test]
fn no_pk_update_and_delete() {
    let e = engine();
    e.execute_sql("CREATE TABLE t (a INT, b INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 10)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2, 20)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (3, 30)", None).unwrap();

    e.execute_sql("UPDATE t SET b = 99 WHERE a = 2", None).unwrap();
    assert_eq!(ints(&e, "SELECT b FROM t WHERE a = 2", 0), vec![99]);
    assert_eq!(ints(&e, "SELECT b FROM t", 0), vec![10, 30, 99]);

    e.execute_sql("DELETE FROM t WHERE a = 1", None).unwrap();
    assert_eq!(ints(&e, "SELECT a FROM t", 0), vec![2, 3]);
}

#[test]
fn no_pk_survives_reopen_and_continues_rowid() {
    let mut dir = std::env::temp_dir();
    dir.push(format!("bytedb_nopk_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    {
        let ds = DiskStore::open(dir.clone(), "t").unwrap();
        let mut e = QueryEngine::new(Arc::new(Database::new("t")), Arc::new(TransactionManager::new()));
        e.attach_disk_store(ds);
        e.execute_sql("CREATE TABLE t (a INT)", None).unwrap();
        e.execute_sql("INSERT INTO t VALUES (1)", None).unwrap();
        e.execute_sql("INSERT INTO t VALUES (2)", None).unwrap();
    }
    {
        let ds = DiskStore::open(dir.clone(), "t").unwrap();
        let mut e = QueryEngine::new(Arc::new(Database::new("t")), Arc::new(TransactionManager::new()));
        e.attach_disk_store(ds);
        assert_eq!(ints(&e, "SELECT a FROM t", 0), vec![1, 2]);
        e.execute_sql("INSERT INTO t VALUES (3)", None).unwrap();
        assert_eq!(ints(&e, "SELECT a FROM t", 0), vec![1, 2, 3]);
    }
    let _ = std::fs::remove_dir_all(&dir);
}
