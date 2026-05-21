use std::sync::Arc;
use std::collections::HashSet;
use std::io::{BufRead, BufReader};

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_core::wal::log_manager::LogManager;
use bytedb_core::wal::recovery::RecoveryManager;
use bytedb_query::executor::diskstore::DiskStore;
use bytedb_query::executor::engine::QueryEngine;
use bytedb_query::executor::engine::ExecutionResult;
use bytedb_query::parser::parser::Parser;
use bytedb_core::tuple::value::Value;

fn main() {
    let dir = std::env::var("DATA_DIR").expect("DATA_DIR required");
    let dir_path = std::path::PathBuf::from(&dir);
    let log_path = std::env::var("COMMIT_LOG").expect("COMMIT_LOG required");

    let mut committed: HashSet<u64> = HashSet::new();
    if let Ok(f) = std::fs::File::open(&log_path) {
        for line in BufReader::new(f).lines().flatten() {
            if let Some(rest) = line.strip_prefix("COMMITTED ") {
                if let Ok(id) = rest.trim().parse::<u64>() {
                    committed.insert(id);
                }
            }
        }
    }
    println!("committed-by-stdout-log: {} ids", committed.len());

    let wal_path = dir_path.join("bytedb.wal");
    let log_manager = Arc::new(LogManager::new(&wal_path).expect("open WAL"));
    let recovery = RecoveryManager::recover(&log_manager).expect("recovery");
    println!(
        "recovery: committed_txns={} aborted_txns={} redo={} undo={}",
        recovery.committed_txns.len(),
        recovery.aborted_txns.len(),
        recovery.redo_records.len(),
        recovery.undo_records.len()
    );

    let db = Arc::new(Database::new("k9"));
    let txn_mgr = Arc::new(TransactionManager::new());
    let mut engine_owned = QueryEngine::with_wal(Arc::clone(&db), Arc::clone(&txn_mgr), Arc::clone(&log_manager));
    let store = DiskStore::open(dir_path.clone(), "k9").expect("open disk store");
    engine_owned.attach_disk_store(store);
    let engine = Arc::new(engine_owned);

    let mut p = Parser::new("SELECT id, value FROM log").unwrap();
    let stmt = p.parse().unwrap();
    let result = engine.execute(stmt, None).expect("select");

    let mut present: HashSet<u64> = HashSet::new();
    let mut bad_value = 0u64;
    if let ExecutionResult::Rows { rows, .. } = result {
        for row in rows {
            if let (Some(Value::Int64(id)), Some(Value::Int64(val))) = (row.get(0), row.get(1)) {
                let id_u = *id as u64;
                let expected = id_u * 7;
                if (*val as u64) != expected {
                    bad_value += 1;
                }
                present.insert(id_u);
            }
        }
    }
    println!("rows-present: {}", present.len());
    println!("rows-with-bad-value: {}", bad_value);

    let missing_committed: Vec<u64> = committed.iter().filter(|id| !present.contains(id)).copied().collect();
    let extra_uncommitted: Vec<u64> = present.iter().filter(|id| !committed.contains(id)).copied().collect();

    println!("missing-but-stdout-said-committed: {}", missing_committed.len());
    println!("present-but-not-in-stdout-log: {}", extra_uncommitted.len());

    let durability_loss = !missing_committed.is_empty();
    let corruption = bad_value > 0;

    if durability_loss {
        eprintln!("FAIL durability: {} commits lost", missing_committed.len());
        if missing_committed.len() <= 10 {
            eprintln!("  ids: {:?}", missing_committed);
        }
    }
    if corruption {
        eprintln!("FAIL corruption: {} rows have wrong value", bad_value);
    }

    if durability_loss || corruption {
        std::process::exit(1);
    }
    println!("OK: no durability loss, no corruption");
    println!("  (note: rows present but not in stdout log = {} are crash-window writes that fsynced before stdout flushed - acceptable)", extra_uncommitted.len());
}
