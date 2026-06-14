use std::sync::Arc;

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_query::executor::engine::QueryEngine;

fn engine() -> QueryEngine {
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    QueryEngine::new(db, txn)
}

#[test]
fn fk_to_primary_key_accepts_valid_rejects_invalid() {
    let e = engine();
    e.execute_sql("CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)", None).unwrap();
    e.execute_sql(
        "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id))",
        None,
    )
    .unwrap();
    e.execute_sql("INSERT INTO parent VALUES (1, 'a')", None).unwrap();
    e.execute_sql("INSERT INTO parent VALUES (2, 'b')", None).unwrap();

    assert!(e.execute_sql("INSERT INTO child VALUES (10, 1)", None).is_ok());
    assert!(e.execute_sql("INSERT INTO child VALUES (11, 2)", None).is_ok());
    assert!(e.execute_sql("INSERT INTO child VALUES (12, 999)", None).is_err());
    assert!(e.execute_sql("INSERT INTO child VALUES (13, NULL)", None).is_ok());
}

#[test]
fn fk_to_non_pk_column_still_validates() {
    let e = engine();
    e.execute_sql("CREATE TABLE parent (id INT PRIMARY KEY, code TEXT UNIQUE)", None).unwrap();
    e.execute_sql(
        "CREATE TABLE child (id INT PRIMARY KEY, pcode TEXT REFERENCES parent(code))",
        None,
    )
    .unwrap();
    e.execute_sql("INSERT INTO parent VALUES (1, 'X')", None).unwrap();

    assert!(e.execute_sql("INSERT INTO child VALUES (10, 'X')", None).is_ok());
    assert!(e.execute_sql("INSERT INTO child VALUES (11, 'Y')", None).is_err());
}

#[test]
fn unique_update_to_existing_value_rejected() {
    let e = engine();
    e.execute_sql("CREATE TABLE u (id INT PRIMARY KEY, email TEXT UNIQUE)", None).unwrap();
    e.execute_sql("INSERT INTO u VALUES (1, 'a@x.com')", None).unwrap();
    e.execute_sql("INSERT INTO u VALUES (2, 'b@x.com')", None).unwrap();
    assert!(e.execute_sql("UPDATE u SET email = 'a@x.com' WHERE id = 2", None).is_err());
    assert!(e.execute_sql("UPDATE u SET email = 'c@x.com' WHERE id = 2", None).is_ok());
}
