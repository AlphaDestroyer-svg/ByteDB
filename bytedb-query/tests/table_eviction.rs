use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_core::tuple::value::Value;
use bytedb_query::executor::diskstore::DiskStore;
use bytedb_query::executor::engine::{ExecutionResult, QueryEngine};

fn temp_dir(tag: &str) -> PathBuf {
    let mut d = std::env::temp_dir();
    d.push(format!("bytedb_evict_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn open(dir: &Path) -> QueryEngine {
    let ds = DiskStore::open(dir.to_path_buf(), "test").unwrap();
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    let mut e = QueryEngine::new(db, txn);
    e.attach_disk_store(ds);
    e
}

fn single_int(res: ExecutionResult) -> Option<i64> {
    match res {
        ExecutionResult::Rows { rows, .. } => {
            let row = rows.into_iter().next()?;
            match row.into_iter().next()? {
                Value::Int64(v) => Some(v),
                _ => None,
            }
        }
        _ => None,
    }
}

#[test]
fn cold_tables_evicted_and_reload_preserves_data() {
    let dir = temp_dir("reload");
    let e = open(&dir);

    for i in 0..5 {
        e.execute_sql(&format!("CREATE TABLE t{} (id INT PRIMARY KEY, v INT)", i), None).unwrap();
        e.execute_sql(&format!("INSERT INTO t{} VALUES ({}, {})", i, i, i * 100), None).unwrap();
    }
    assert_eq!(e.tables().read().len(), 5);

    let evicted = e.evict_cold_tables(2);
    assert!(evicted >= 3, "expected to evict at least 3 cold tables, got {}", evicted);
    assert!(e.tables().read().len() <= 2, "resident set must be bounded by budget");

    // The coldest tables were evicted; querying one must transparently reload it with the same data.
    for i in 0..5 {
        let got = single_int(e.execute_sql(&format!("SELECT v FROM t{}", i), None).unwrap());
        assert_eq!(got, Some(i as i64 * 100), "table t{} lost data across eviction/reload", i);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn table_with_uncommitted_writes_is_not_evicted() {
    let dir = temp_dir("uncommitted");
    let e = open(&dir);

    for i in 0..4 {
        e.execute_sql(&format!("CREATE TABLE u{} (id INT PRIMARY KEY, v INT)", i), None).unwrap();
        e.execute_sql(&format!("INSERT INTO u{} VALUES ({}, {})", i, i, i), None).unwrap();
    }

    // Open a transaction and write to u0 (uncommitted, not yet in the on-disk log).
    let begin = e.execute_sql("BEGIN", None).unwrap();
    let tid: u64 = match begin {
        ExecutionResult::Ok(m) => m.split_whitespace().nth(1).unwrap().parse().unwrap(),
        _ => panic!("BEGIN did not return a transaction id"),
    };
    e.execute_sql("INSERT INTO u0 VALUES (99, 99)", Some(tid)).unwrap();

    // Make the other tables hotter so u0 is the coldest eviction candidate.
    for i in 1..4 {
        let _ = e.execute_sql(&format!("SELECT v FROM u{}", i), None).unwrap();
    }

    // Even as the coldest table, u0 must survive: evicting it would drop uncommitted state.
    let _ = e.evict_cold_tables(1);
    assert!(
        e.tables().read().contains_key("u0"),
        "a table with uncommitted MVCC versions must never be evicted"
    );

    // The uncommitted row is still visible inside the transaction (no data loss).
    let got = single_int(e.execute_sql("SELECT v FROM u0 WHERE id = 99", Some(tid)).unwrap());
    assert_eq!(got, Some(99), "uncommitted row must remain visible to its transaction");

    e.execute_sql("COMMIT", Some(tid)).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
