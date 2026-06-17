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

fn count(e: &QueryEngine, sql: &str) -> i64 {
    match e.execute_sql(sql, None).unwrap() {
        ExecutionResult::Rows { rows, .. } => match rows[0][0] {
            Value::Int64(n) => n,
            _ => panic!("not int"),
        },
        other => panic!("{other:?}"),
    }
}

fn setup(with_index: bool, on_delete: &str) -> QueryEngine {
    let e = engine();
    e.execute_sql("CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)", None).unwrap();
    e.execute_sql(
        &format!("CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id) ON DELETE {})", on_delete),
        None,
    ).unwrap();
    if with_index {
        e.execute_sql("CREATE INDEX ix_child_parent ON child(parent_id)", None).unwrap();
    }
    for i in 1..=3 {
        e.execute_sql(&format!("INSERT INTO parent VALUES ({}, 'p{}')", i, i), None).unwrap();
    }
    e.execute_sql("INSERT INTO child VALUES (10, 1)", None).unwrap();
    e.execute_sql("INSERT INTO child VALUES (11, 1)", None).unwrap();
    e.execute_sql("INSERT INTO child VALUES (12, 2)", None).unwrap();
    e.execute_sql("INSERT INTO child VALUES (13, 3)", None).unwrap();
    e
}

fn cascade_deletes_children(with_index: bool) {
    let e = setup(with_index, "CASCADE");
    e.execute_sql("DELETE FROM parent WHERE id = 1", None).unwrap();
    assert_eq!(count(&e, "SELECT COUNT(*) FROM child WHERE parent_id = 1"), 0);
    assert_eq!(count(&e, "SELECT COUNT(*) FROM child"), 2);
    assert_eq!(count(&e, "SELECT COUNT(*) FROM parent"), 2);
}

#[test]
fn cascade_with_index() { cascade_deletes_children(true); }
#[test]
fn cascade_without_index() { cascade_deletes_children(false); }

fn restrict_blocks_delete(with_index: bool) {
    let e = setup(with_index, "RESTRICT");
    assert!(e.execute_sql("DELETE FROM parent WHERE id = 1", None).is_err());
    assert_eq!(count(&e, "SELECT COUNT(*) FROM parent"), 3);
    assert_eq!(count(&e, "SELECT COUNT(*) FROM child"), 4);
    e.execute_sql("DELETE FROM child WHERE parent_id = 3", None).unwrap();
    assert!(e.execute_sql("DELETE FROM parent WHERE id = 3", None).is_ok());
}

#[test]
fn restrict_with_index() { restrict_blocks_delete(true); }
#[test]
fn restrict_without_index() { restrict_blocks_delete(false); }

fn setnull_nulls_children(with_index: bool) {
    let e = setup(with_index, "SET NULL");
    e.execute_sql("DELETE FROM parent WHERE id = 1", None).unwrap();
    assert_eq!(count(&e, "SELECT COUNT(*) FROM child WHERE parent_id IS NULL"), 2);
    assert_eq!(count(&e, "SELECT COUNT(*) FROM child"), 4);
    assert_eq!(count(&e, "SELECT COUNT(*) FROM child WHERE parent_id = 2"), 1);
}

#[test]
fn setnull_with_index() { setnull_nulls_children(true); }
#[test]
fn setnull_without_index() { setnull_nulls_children(false); }

#[test]
fn cascade_multi_parent_delete_with_index() {
    let e = setup(true, "CASCADE");
    e.execute_sql("DELETE FROM parent WHERE id < 3", None).unwrap();
    assert_eq!(count(&e, "SELECT COUNT(*) FROM child"), 1);
    assert_eq!(count(&e, "SELECT COUNT(*) FROM child WHERE parent_id = 3"), 1);
    assert_eq!(count(&e, "SELECT COUNT(*) FROM parent"), 1);
}

#[test]
fn setnull_after_index_lookup_keeps_index_consistent() {
    let e = setup(true, "SET NULL");
    e.execute_sql("DELETE FROM parent WHERE id = 1", None).unwrap();
    assert_eq!(count(&e, "SELECT COUNT(*) FROM child WHERE parent_id = 1"), 0);
    assert_eq!(count(&e, "SELECT COUNT(*) FROM child WHERE parent_id = 2"), 1);
    e.execute_sql("DELETE FROM parent WHERE id = 2", None).unwrap();
    assert_eq!(count(&e, "SELECT COUNT(*) FROM child WHERE parent_id = 2"), 0);
    assert_eq!(count(&e, "SELECT COUNT(*) FROM child WHERE parent_id IS NULL"), 3);
}

#[test]
fn cascade_text_fk_with_index() {
    let e = engine();
    e.execute_sql("CREATE TABLE parent (id INT PRIMARY KEY, code TEXT UNIQUE)", None).unwrap();
    e.execute_sql(
        "CREATE TABLE child (id INT PRIMARY KEY, pcode TEXT REFERENCES parent(code) ON DELETE CASCADE)",
        None,
    ).unwrap();
    e.execute_sql("CREATE INDEX ix_child_pcode ON child(pcode)", None).unwrap();
    e.execute_sql("INSERT INTO parent VALUES (1, 'AA')", None).unwrap();
    e.execute_sql("INSERT INTO parent VALUES (2, 'BB')", None).unwrap();
    e.execute_sql("INSERT INTO child VALUES (10, 'AA')", None).unwrap();
    e.execute_sql("INSERT INTO child VALUES (11, 'AA')", None).unwrap();
    e.execute_sql("INSERT INTO child VALUES (12, 'BB')", None).unwrap();

    e.execute_sql("DELETE FROM parent WHERE code = 'AA'", None).unwrap();
    assert_eq!(count(&e, "SELECT COUNT(*) FROM child"), 1);
    assert_eq!(count(&e, "SELECT COUNT(*) FROM child WHERE pcode = 'BB'"), 1);
}
