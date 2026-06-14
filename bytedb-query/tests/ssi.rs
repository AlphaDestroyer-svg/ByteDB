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

fn seed_doctors(e: &QueryEngine) {
    e.execute_sql("CREATE TABLE doctors (id INT PRIMARY KEY, on_call INT)", None).unwrap();
    e.execute_sql("INSERT INTO doctors VALUES (1, 1)", None).unwrap();
    e.execute_sql("INSERT INTO doctors VALUES (2, 1)", None).unwrap();
}

#[test]
fn write_skew_aborts_one_under_serializable() {
    let (txn, e) = setup();
    seed_doctors(&e);

    let t1 = txn.begin(IsolationLevel::Serializable);
    let t2 = txn.begin(IsolationLevel::Serializable);

    e.execute_sql("SELECT id FROM doctors WHERE on_call = 1", Some(t1)).unwrap();
    e.execute_sql("SELECT id FROM doctors WHERE on_call = 1", Some(t2)).unwrap();

    e.execute_sql("UPDATE doctors SET on_call = 0 WHERE id = 1", Some(t1)).unwrap();
    e.execute_sql("UPDATE doctors SET on_call = 0 WHERE id = 2", Some(t2)).unwrap();

    let c1 = txn.commit(t1);
    let c2 = txn.commit(t2);

    let aborts = [c1.is_err(), c2.is_err()].iter().filter(|x| **x).count();
    assert_eq!(aborts, 1, "exactly one serializable txn must abort (c1={c1:?} c2={c2:?})");
}

#[test]
fn write_skew_allowed_under_read_committed() {
    let (txn, e) = setup();
    seed_doctors(&e);

    let t1 = txn.begin(IsolationLevel::ReadCommitted);
    let t2 = txn.begin(IsolationLevel::ReadCommitted);

    e.execute_sql("SELECT id FROM doctors WHERE on_call = 1", Some(t1)).unwrap();
    e.execute_sql("SELECT id FROM doctors WHERE on_call = 1", Some(t2)).unwrap();

    e.execute_sql("UPDATE doctors SET on_call = 0 WHERE id = 1", Some(t1)).unwrap();
    e.execute_sql("UPDATE doctors SET on_call = 0 WHERE id = 2", Some(t2)).unwrap();

    assert!(txn.commit(t1).is_ok());
    assert!(txn.commit(t2).is_ok());
}

#[test]
fn disjoint_point_writes_both_commit() {
    let (txn, e) = setup();
    seed_doctors(&e);

    let t1 = txn.begin(IsolationLevel::Serializable);
    let t2 = txn.begin(IsolationLevel::Serializable);

    // Point read+write only on their own key (no table scan).
    e.execute_sql("UPDATE doctors SET on_call = 0 WHERE id = 1", Some(t1)).unwrap();
    e.execute_sql("UPDATE doctors SET on_call = 0 WHERE id = 2", Some(t2)).unwrap();

    assert!(txn.commit(t1).is_ok());
    assert!(txn.commit(t2).is_ok());
}

/// Predicate / phantom write-skew: each counts black rows then inserts one.
#[test]
fn phantom_write_skew_aborts_one() {
    let (txn, e) = setup();
    e.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, color TEXT)", None).unwrap();
    e.execute_sql("INSERT INTO items VALUES (1, 'white')", None).unwrap();

    let t1 = txn.begin(IsolationLevel::Serializable);
    let t2 = txn.begin(IsolationLevel::Serializable);

    e.execute_sql("SELECT id FROM items WHERE color = 'black'", Some(t1)).unwrap();
    e.execute_sql("SELECT id FROM items WHERE color = 'black'", Some(t2)).unwrap();

    e.execute_sql("INSERT INTO items VALUES (10, 'black')", Some(t1)).unwrap();
    e.execute_sql("INSERT INTO items VALUES (11, 'black')", Some(t2)).unwrap();

    let c1 = txn.commit(t1);
    let c2 = txn.commit(t2);
    let aborts = [c1.is_err(), c2.is_err()].iter().filter(|x| **x).count();
    assert_eq!(aborts, 1, "exactly one must abort (c1={c1:?} c2={c2:?})");
}

/// A read-only serializable transaction never aborts itself.
#[test]
fn read_only_serializable_never_aborts() {
    let (txn, e) = setup();
    seed_doctors(&e);

    let t_ro = txn.begin(IsolationLevel::Serializable);
    let t_w = txn.begin(IsolationLevel::Serializable);

    e.execute_sql("SELECT id FROM doctors WHERE on_call = 1", Some(t_ro)).unwrap();
    e.execute_sql("UPDATE doctors SET on_call = 0 WHERE id = 1", Some(t_w)).unwrap();
    assert!(txn.commit(t_w).is_ok());

    // Reader observes the overwritten key but, having no writes, cannot be a pivot.
    e.execute_sql("SELECT id FROM doctors WHERE on_call = 1", Some(t_ro)).unwrap();
    assert!(txn.commit(t_ro).is_ok());
}

#[test]
fn ssi_state_drains_after_txns_finish() {
    let (txn, e) = setup();
    seed_doctors(&e);

    for i in 0..50i64 {
        let t = txn.begin(IsolationLevel::Serializable);
        e.execute_sql("SELECT id FROM doctors WHERE on_call = 1", Some(t)).unwrap();
        e.execute_sql(&format!("UPDATE doctors SET on_call = {} WHERE id = 1", i % 2), Some(t)).unwrap();
        let _ = txn.commit(t);
    }
    assert_eq!(txn.ssi_active_count(), 0, "active SSI states leaked");
    assert_eq!(txn.ssi_recent_count(), 0, "recent SSI footprints leaked");
}

#[test]
fn ssi_recent_trimmed_under_readonly_workload() {
    let (txn, e) = setup();
    seed_doctors(&e);

    let w = txn.begin(IsolationLevel::Serializable);
    e.execute_sql("UPDATE doctors SET on_call = 0 WHERE id = 1", Some(w)).unwrap();
    txn.commit(w).unwrap();

    for _ in 0..10 {
        let r = txn.begin(IsolationLevel::Serializable);
        e.execute_sql("SELECT id FROM doctors", Some(r)).unwrap();
        txn.commit(r).unwrap();
    }
    assert_eq!(txn.ssi_recent_count(), 0, "ssi_recent not trimmed by read-only commits");
}

#[test]
fn concurrent_serializable_no_deadlock() {
    let (txn, e) = setup();
    e.execute_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)", None).unwrap();
    for i in 0..10 {
        e.execute_sql(&format!("INSERT INTO t VALUES ({i}, 0)"), None).unwrap();
    }
    let e = Arc::new(e);

    let mut handles = Vec::new();
    for thread in 0..8u64 {
        let e = Arc::clone(&e);
        let txn = Arc::clone(&txn);
        handles.push(std::thread::spawn(move || {
            for iter in 0..25u64 {
                let key = (thread.wrapping_mul(7).wrapping_add(iter)) % 10;
                let tid = txn.begin(IsolationLevel::Serializable);
                let _ = e.execute_sql(&format!("SELECT v FROM t WHERE id = {key}"), Some(tid));
                let _ = e.execute_sql(&format!("UPDATE t SET v = v + 1 WHERE id = {key}"), Some(tid));
                if txn.commit(tid).is_err() {
                    let _ = txn.abort(tid);
                }
            }
        }));
    }
    for h in handles {
        h.join().expect("worker thread panicked");
    }
}
