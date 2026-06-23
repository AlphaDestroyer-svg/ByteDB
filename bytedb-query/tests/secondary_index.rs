use std::sync::Arc;

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_core::tuple::value::Value;
use bytedb_query::executor::diskstore::DiskStore;
use bytedb_query::executor::engine::{ExecutionResult, QueryEngine};

fn engine() -> QueryEngine {
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    QueryEngine::new(db, txn)
}

fn run(e: &QueryEngine, sql: &str) -> ExecutionResult {
    e.execute_sql(sql, None).unwrap_or_else(|err| panic!("sql failed: {sql}: {err}"))
}

fn rows(e: &QueryEngine, sql: &str) -> Vec<Vec<Value>> {
    match run(e, sql) {
        ExecutionResult::Rows { rows, .. } => rows,
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

fn ints(e: &QueryEngine, sql: &str, col: usize) -> Vec<i64> {
    let mut v: Vec<i64> = rows(e, sql)
        .into_iter()
        .filter_map(|r| r.get(col).and_then(|x| x.as_i64()))
        .collect();
    v.sort_unstable();
    v
}

fn seed(e: &QueryEngine) {
    run(e, "CREATE TABLE users (id INT PRIMARY KEY, age INT, city TEXT)");
    for (id, age, city) in [
        (1, 30, "NYC"),
        (2, 25, "LA"),
        (3, 30, "NYC"),
        (4, 40, "SF"),
        (5, 25, "LA"),
        (6, 100, "NYC"),
    ] {
        run(e, &format!("INSERT INTO users VALUES ({id}, {age}, '{city}')"));
    }
}

#[test]
fn equality_lookup_matches_seq_scan() {
    let e = engine();
    seed(&e);
    run(&e, "CREATE INDEX idx_age ON users (age)");

    let plan = rows(&e, "EXPLAIN SELECT id FROM users WHERE age = 30");
    let txt: String = plan.iter().flatten().map(|v| v.to_string()).collect::<Vec<_>>().join("\n");
    assert!(txt.contains("Index Scan"), "expected Index Scan in plan, got:\n{txt}");

    assert_eq!(ints(&e, "SELECT id FROM users WHERE age = 30", 0), vec![1, 3]);
    assert_eq!(ints(&e, "SELECT id FROM users WHERE age = 25", 0), vec![2, 5]);
    assert_eq!(ints(&e, "SELECT id FROM users WHERE age = 99", 0), Vec::<i64>::new());
}

#[test]
fn range_lookup_is_correct() {
    let e = engine();
    seed(&e);
    run(&e, "CREATE INDEX idx_age ON users (age)");

    assert_eq!(ints(&e, "SELECT id FROM users WHERE age > 30", 0), vec![4, 6]);
    assert_eq!(ints(&e, "SELECT id FROM users WHERE age >= 30", 0), vec![1, 3, 4, 6]);
    assert_eq!(ints(&e, "SELECT id FROM users WHERE age < 30", 0), vec![2, 5]);
    assert_eq!(ints(&e, "SELECT id FROM users WHERE age <= 25", 0), vec![2, 5]);
}

#[test]
fn text_index_equality() {
    let e = engine();
    seed(&e);
    run(&e, "CREATE INDEX idx_city ON users (city)");

    let plan = rows(&e, "EXPLAIN SELECT id FROM users WHERE city = 'NYC'");
    let txt: String = plan.iter().flatten().map(|v| v.to_string()).collect::<Vec<_>>().join("\n");
    assert!(txt.contains("Index Scan"), "expected Index Scan, got:\n{txt}");

    assert_eq!(ints(&e, "SELECT id FROM users WHERE city = 'NYC'", 0), vec![1, 3, 6]);
    assert_eq!(ints(&e, "SELECT id FROM users WHERE city = 'LA'", 0), vec![2, 5]);
}

#[test]
fn update_keeps_index_consistent() {
    let e = engine();
    seed(&e);
    run(&e, "CREATE INDEX idx_age ON users (age)");

    run(&e, "UPDATE users SET age = 25 WHERE id = 1");
    // id 1 left the age=30 bucket and joined age=25.
    assert_eq!(ints(&e, "SELECT id FROM users WHERE age = 30", 0), vec![3]);
    assert_eq!(ints(&e, "SELECT id FROM users WHERE age = 25", 0), vec![1, 2, 5]);
}

#[test]
fn delete_removes_from_index() {
    let e = engine();
    seed(&e);
    run(&e, "CREATE INDEX idx_age ON users (age)");

    run(&e, "DELETE FROM users WHERE id = 3");
    assert_eq!(ints(&e, "SELECT id FROM users WHERE age = 30", 0), vec![1]);
}

#[test]
fn unique_index_rejects_duplicates() {
    let e = engine();
    run(&e, "CREATE TABLE u (id INT PRIMARY KEY, email TEXT)");
    run(&e, "INSERT INTO u VALUES (1, 'a@x.com')");
    run(&e, "CREATE UNIQUE INDEX idx_email ON u (email)");

    // Duplicate value must be rejected.
    assert!(e.execute_sql("INSERT INTO u VALUES (2, 'a@x.com')", None).is_err());
    // A distinct value is fine.
    assert!(e.execute_sql("INSERT INTO u VALUES (3, 'b@x.com')", None).is_ok());
}

#[test]
fn unique_index_build_rejects_existing_duplicates() {
    let e = engine();
    run(&e, "CREATE TABLE u (id INT PRIMARY KEY, email TEXT)");
    run(&e, "INSERT INTO u VALUES (1, 'a@x.com')");
    run(&e, "INSERT INTO u VALUES (2, 'a@x.com')");
    assert!(e.execute_sql("CREATE UNIQUE INDEX idx_email ON u (email)", None).is_err());
}

#[test]
fn drop_index_falls_back_to_seq_scan() {
    let e = engine();
    seed(&e);
    run(&e, "CREATE INDEX idx_age ON users (age)");
    run(&e, "DROP INDEX idx_age");

    let plan = rows(&e, "EXPLAIN SELECT id FROM users WHERE age = 30");
    let txt: String = plan.iter().flatten().map(|v| v.to_string()).collect::<Vec<_>>().join("\n");
    assert!(!txt.contains("Index Scan"), "index should be gone, got:\n{txt}");
    // Results still correct via seq scan.
    assert_eq!(ints(&e, "SELECT id FROM users WHERE age = 30", 0), vec![1, 3]);
}

#[test]
fn index_survives_reopen_from_disk() {
    let mut dir = std::env::temp_dir();
    dir.push(format!("bytedb_idx_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    {
        let ds = DiskStore::open(dir.clone(), "test").unwrap();
        let db = Arc::new(Database::new("test"));
        let txn = Arc::new(TransactionManager::new());
        let mut e = QueryEngine::new(db, txn);
        e.attach_disk_store(ds);
        run(&e, "CREATE TABLE users (id INT PRIMARY KEY, age INT)");
        run(&e, "INSERT INTO users VALUES (1, 30)");
        run(&e, "INSERT INTO users VALUES (2, 25)");
        run(&e, "INSERT INTO users VALUES (3, 30)");
        run(&e, "CREATE INDEX idx_age ON users (age)");
    }

    {
        let ds = DiskStore::open(dir.clone(), "test").unwrap();
        let db = Arc::new(Database::new("test"));
        let txn = Arc::new(TransactionManager::new());
        let mut e = QueryEngine::new(db, txn);
        e.attach_disk_store(ds);

        let plan = rows(&e, "EXPLAIN SELECT id FROM users WHERE age = 30");
        let txt: String = plan.iter().flatten().map(|v| v.to_string()).collect::<Vec<_>>().join("\n");
        assert!(txt.contains("Index Scan"), "index should persist, got:\n{txt}");
        assert_eq!(ints(&e, "SELECT id FROM users WHERE age = 30", 0), vec![1, 3]);
    }

    let _ = std::fs::remove_dir_all(&dir);
}
