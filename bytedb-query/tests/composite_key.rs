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

#[test]
fn composite_primary_key() {
    let e = engine();
    e.execute_sql("CREATE TABLE mc (a INT, b INT, v INT, PRIMARY KEY (a, b))", None).unwrap();
    e.execute_sql("INSERT INTO mc VALUES (1, 1, 10)", None).unwrap();
    e.execute_sql("INSERT INTO mc VALUES (1, 2, 20)", None).unwrap();
    // Same (a,b) is a duplicate; differing b is not.
    assert!(e.execute_sql("INSERT INTO mc VALUES (1, 1, 99)", None).is_err());
    e.execute_sql("INSERT INTO mc VALUES (2, 1, 30)", None).unwrap();

    let r = e.execute_sql("SELECT v FROM mc WHERE a = 1 AND b = 2", None).unwrap();
    if let ExecutionResult::Rows { rows, .. } = r {
        assert_eq!(rows, vec![vec![Value::Int64(20)]]);
    } else {
        panic!();
    }
}

#[test]
fn table_level_unique() {
    let e = engine();
    e.execute_sql("CREATE TABLE uu (id INT PRIMARY KEY, x INT, UNIQUE (x))", None).unwrap();
    e.execute_sql("INSERT INTO uu VALUES (1, 5)", None).unwrap();
    assert!(e.execute_sql("INSERT INTO uu VALUES (2, 5)", None).is_err());
    assert!(e.execute_sql("INSERT INTO uu VALUES (3, 6)", None).is_ok());
}
