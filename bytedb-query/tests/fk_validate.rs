use std::sync::Arc;
use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_query::executor::engine::QueryEngine;

fn engine() -> QueryEngine {
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    QueryEngine::new(db, txn)
}

fn setup_unique_parent(with_index: bool) -> QueryEngine {
    let e = engine();
    e.execute_sql("CREATE TABLE parent (id INT PRIMARY KEY, code TEXT UNIQUE)", None).unwrap();
    e.execute_sql(
        "CREATE TABLE child (id INT PRIMARY KEY, pcode TEXT REFERENCES parent(code))",
        None,
    ).unwrap();
    if with_index {
        e.execute_sql("CREATE INDEX ix_parent_code ON parent(code)", None).unwrap();
    }
    e.execute_sql("INSERT INTO parent VALUES (1, 'AA')", None).unwrap();
    e.execute_sql("INSERT INTO parent VALUES (2, 'BB')", None).unwrap();
    e
}

fn insert_validates_against_non_pk(with_index: bool) {
    let e = setup_unique_parent(with_index);
    assert!(e.execute_sql("INSERT INTO child VALUES (10, 'AA')", None).is_ok());
    assert!(e.execute_sql("INSERT INTO child VALUES (11, 'BB')", None).is_ok());
    assert!(e.execute_sql("INSERT INTO child VALUES (12, 'ZZ')", None).is_err());
    assert!(e.execute_sql("INSERT INTO child VALUES (13, NULL)", None).is_ok());
}

#[test]
fn insert_non_pk_fk_with_parent_index() { insert_validates_against_non_pk(true); }
#[test]
fn insert_non_pk_fk_without_parent_index() { insert_validates_against_non_pk(false); }

fn update_validates_against_non_pk(with_index: bool) {
    let e = setup_unique_parent(with_index);
    e.execute_sql("INSERT INTO child VALUES (10, 'AA')", None).unwrap();
    assert!(e.execute_sql("UPDATE child SET pcode = 'BB' WHERE id = 10", None).is_ok());
    assert!(e.execute_sql("UPDATE child SET pcode = 'ZZ' WHERE id = 10", None).is_err());
    assert!(e.execute_sql("UPDATE child SET pcode = NULL WHERE id = 10", None).is_ok());
}

#[test]
fn update_non_pk_fk_with_parent_index() { update_validates_against_non_pk(true); }
#[test]
fn update_non_pk_fk_without_parent_index() { update_validates_against_non_pk(false); }

#[test]
fn insert_pk_fk_still_validates() {
    let e = engine();
    e.execute_sql("CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)", None).unwrap();
    e.execute_sql(
        "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id))",
        None,
    ).unwrap();
    e.execute_sql("INSERT INTO parent VALUES (1, 'a')", None).unwrap();
    assert!(e.execute_sql("INSERT INTO child VALUES (10, 1)", None).is_ok());
    assert!(e.execute_sql("INSERT INTO child VALUES (11, 99)", None).is_err());
}

#[test]
fn update_pk_fk_still_validates() {
    let e = engine();
    e.execute_sql("CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)", None).unwrap();
    e.execute_sql(
        "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id))",
        None,
    ).unwrap();
    e.execute_sql("INSERT INTO parent VALUES (1, 'a')", None).unwrap();
    e.execute_sql("INSERT INTO parent VALUES (2, 'b')", None).unwrap();
    e.execute_sql("INSERT INTO child VALUES (10, 1)", None).unwrap();
    assert!(e.execute_sql("UPDATE child SET parent_id = 2 WHERE id = 10", None).is_ok());
    assert!(e.execute_sql("UPDATE child SET parent_id = 99 WHERE id = 10", None).is_err());
}

#[test]
fn insert_int_fk_to_unique_with_index() {
    let e = engine();
    e.execute_sql("CREATE TABLE parent (id INT PRIMARY KEY, num INT UNIQUE)", None).unwrap();
    e.execute_sql(
        "CREATE TABLE child (id INT PRIMARY KEY, pnum INT REFERENCES parent(num))",
        None,
    ).unwrap();
    e.execute_sql("CREATE INDEX ix_pnum ON parent(num)", None).unwrap();
    e.execute_sql("INSERT INTO parent VALUES (1, 500)", None).unwrap();
    e.execute_sql("INSERT INTO parent VALUES (2, 600)", None).unwrap();
    assert!(e.execute_sql("INSERT INTO child VALUES (10, 500)", None).is_ok());
    assert!(e.execute_sql("INSERT INTO child VALUES (11, 600)", None).is_ok());
    assert!(e.execute_sql("INSERT INTO child VALUES (12, 700)", None).is_err());
}
