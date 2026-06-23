use std::sync::Arc;
use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_query::executor::diskstore::DiskStore;
use bytedb_query::executor::engine::QueryEngine;

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let mut d = std::env::temp_dir();
    d.push(format!("bytedb_check_persist_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn open(dir: &std::path::Path) -> QueryEngine {
    let ds = DiskStore::open(dir.to_path_buf(), "test").unwrap();
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    let mut e = QueryEngine::new(db, txn);
    e.attach_disk_store(ds);
    e
}

#[test]
fn column_check_survives_restart() {
    let dir = temp_dir("col");
    {
        let e = open(&dir);
        e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, age INT CHECK (age >= 0))", None).unwrap();
        assert!(e.execute_sql("INSERT INTO t VALUES (1, 25)", None).is_ok());
        assert!(e.execute_sql("INSERT INTO t VALUES (2, -5)", None).is_err());
    }
    {
        let e = open(&dir);
        // CHECK must still be enforced after reopening from disk
        assert!(e.execute_sql("INSERT INTO t VALUES (3, 30)", None).is_ok());
        assert!(e.execute_sql("INSERT INTO t VALUES (4, -1)", None).is_err(),
            "CHECK (age >= 0) must survive restart and reject negative");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn table_check_survives_restart() {
    let dir = temp_dir("table");
    {
        let e = open(&dir);
        e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, lo INT, hi INT, CHECK (lo <= hi))", None).unwrap();
        assert!(e.execute_sql("INSERT INTO t VALUES (1, 5, 10)", None).is_ok());
        assert!(e.execute_sql("INSERT INTO t VALUES (2, 10, 5)", None).is_err());
    }
    {
        let e = open(&dir);
        assert!(e.execute_sql("INSERT INTO t VALUES (3, 1, 2)", None).is_ok());
        assert!(e.execute_sql("INSERT INTO t VALUES (4, 9, 1)", None).is_err(),
            "table CHECK (lo <= hi) must survive restart");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn complex_check_survives_restart() {
    let dir = temp_dir("complex");
    {
        let e = open(&dir);
        e.execute_sql(
            "CREATE TABLE t (id INT PRIMARY KEY, status TEXT CHECK (status IN ('active', 'inactive', 'pending')))",
            None,
        ).unwrap();
        assert!(e.execute_sql("INSERT INTO t VALUES (1, 'active')", None).is_ok());
        assert!(e.execute_sql("INSERT INTO t VALUES (2, 'bogus')", None).is_err());
    }
    {
        let e = open(&dir);
        assert!(e.execute_sql("INSERT INTO t VALUES (3, 'pending')", None).is_ok());
        assert!(e.execute_sql("INSERT INTO t VALUES (4, 'unknown')", None).is_err(),
            "CHECK (status IN (...)) must survive restart");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
