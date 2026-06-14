use std::sync::Arc;

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::{IsolationLevel, TransactionManager};
use bytedb_query::executor::engine::QueryEngine;

fn setup() -> (Arc<TransactionManager>, QueryEngine) {
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    let engine = QueryEngine::new(db, txn.clone());
    (txn, engine)
}

fn seed(e: &QueryEngine) {
    e.execute_sql("CREATE TABLE acct (id INT PRIMARY KEY, bal INT)", None).unwrap();
    e.execute_sql("INSERT INTO acct VALUES (1, 100)", None).unwrap();
    e.execute_sql("INSERT INTO acct VALUES (2, 100)", None).unwrap();
}

#[test]
fn probe_pivot_commits_first() {
    let (txn, e) = setup();
    seed(&e);

    let t1 = txn.begin(IsolationLevel::Serializable);
    let t2 = txn.begin(IsolationLevel::Serializable);

    e.execute_sql("SELECT bal FROM acct WHERE id = 2", Some(t1)).unwrap();
    e.execute_sql("UPDATE acct SET bal = 0 WHERE id = 1", Some(t1)).unwrap();
    let c1 = txn.commit(t1);

    e.execute_sql("SELECT bal FROM acct WHERE id = 1", Some(t2)).unwrap();
    e.execute_sql("UPDATE acct SET bal = 0 WHERE id = 2", Some(t2)).unwrap();
    let c2 = txn.commit(t2);

    let aborts = [c1.is_err(), c2.is_err()].iter().filter(|x| **x).count();
    assert_eq!(aborts, 1, "exactly one must abort (c1={c1:?} c2={c2:?})");
}

#[test]
fn probe_three_txn_cycle_pivot_reads() {
    let (txn, e) = setup();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (1, 0)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (2, 0)", None).unwrap();
    e.execute_sql("INSERT INTO t VALUES (3, 0)", None).unwrap();

    let tout = txn.begin(IsolationLevel::Serializable);
    let tpiv = txn.begin(IsolationLevel::Serializable);
    let tin = txn.begin(IsolationLevel::Serializable);

    e.execute_sql("SELECT v FROM t WHERE id = 1", Some(tout)).unwrap();
    e.execute_sql("UPDATE t SET v = 1 WHERE id = 2", Some(tout)).unwrap();
    let cout = txn.commit(tout);

    e.execute_sql("SELECT v FROM t WHERE id = 2", Some(tpiv)).unwrap();
    e.execute_sql("UPDATE t SET v = 1 WHERE id = 3", Some(tpiv)).unwrap();

    e.execute_sql("SELECT v FROM t WHERE id = 3", Some(tin)).unwrap();
    e.execute_sql("UPDATE t SET v = 1 WHERE id = 1", Some(tin)).unwrap();

    let cpiv = txn.commit(tpiv);
    let cin = txn.commit(tin);

    let aborts = [cout.is_err(), cpiv.is_err(), cin.is_err()].iter().filter(|x| **x).count();
    assert!(aborts >= 1, "at least one must abort to break cycle (cout={cout:?} cpiv={cpiv:?} cin={cin:?})");
}

#[test]
fn probe_topn_unstrumented() {
    let (txn, e) = setup();
    seed(&e);

    let t1 = txn.begin(IsolationLevel::Serializable);
    let t2 = txn.begin(IsolationLevel::Serializable);

    e.execute_sql("SELECT id FROM acct ORDER BY bal LIMIT 5", Some(t1)).unwrap();
    e.execute_sql("SELECT id FROM acct ORDER BY bal LIMIT 5", Some(t2)).unwrap();

    e.execute_sql("UPDATE acct SET bal = 0 WHERE id = 1", Some(t1)).unwrap();
    e.execute_sql("UPDATE acct SET bal = 0 WHERE id = 2", Some(t2)).unwrap();

    let c1 = txn.commit(t1);
    let c2 = txn.commit(t2);
    let aborts = [c1.is_err(), c2.is_err()].iter().filter(|x| **x).count();
    println!("TOPN aborts={aborts} c1={c1:?} c2={c2:?}");
    assert_eq!(aborts, 1, "top-n read path must record predicate scan");
}
