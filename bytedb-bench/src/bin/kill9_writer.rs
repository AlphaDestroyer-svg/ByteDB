use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::thread;

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::{IsolationLevel, TransactionManager};
use bytedb_core::wal::log_manager::LogManager;
use bytedb_core::wal::recovery::RecoveryManager;
use bytedb_query::executor::diskstore::DiskStore;
use bytedb_query::executor::engine::QueryEngine;
use bytedb_query::parser::parser::Parser;

fn run_sql(engine: &QueryEngine, sql: &str, txn_id: Option<u64>) -> Result<(), String> {
    let mut p = Parser::new(sql).map_err(|e| format!("parse {sql}: {e:?}"))?;
    let stmt = p.parse().map_err(|e| format!("parse {sql}: {e:?}"))?;
    engine.execute(stmt, txn_id).map_err(|e| format!("exec {sql}: {e:?}"))?;
    Ok(())
}

fn main() {
    bytedb_core::chaos::configure_from_env();
    let dir = std::env::var("DATA_DIR").expect("DATA_DIR required");
    let dir_path = std::path::PathBuf::from(&dir);
    std::fs::create_dir_all(&dir_path).unwrap();

    let wal_path = dir_path.join("bytedb.wal");
    let log_manager = Arc::new(LogManager::new(&wal_path).expect("open WAL"));
    let _ = RecoveryManager::recover(&log_manager);

    let db = Arc::new(Database::new("k9"));
    let txn_mgr = Arc::new(TransactionManager::new());
    let mut engine_owned = QueryEngine::with_wal(Arc::clone(&db), Arc::clone(&txn_mgr), Arc::clone(&log_manager));
    let store = DiskStore::open(dir_path.clone(), "k9").expect("open disk store");
    engine_owned.attach_disk_store(store);
    let engine = Arc::new(engine_owned);

    let _ = run_sql(&engine, "CREATE TABLE log (id INT PRIMARY KEY, value INT)", None);

    let next_id = Arc::new(AtomicU64::new(1));
    let threads: usize = std::env::var("THREADS").ok().and_then(|s| s.parse().ok()).unwrap_or(4);

    let mut handles = Vec::new();
    for _ in 0..threads {
        let engine = Arc::clone(&engine);
        let txn_mgr = Arc::clone(&txn_mgr);
        let next_id = Arc::clone(&next_id);
        let h = thread::spawn(move || {
            loop {
                let id = next_id.fetch_add(1, Ordering::Relaxed);
                let val = id * 7;
                let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
                use bytedb_core::wal::log_record::LogRecord;
                if let Some(wal) = engine.wal_handle() {
                    let _ = wal.append(LogRecord::Begin { txn_id: txn });
                }
                if run_sql(&engine, &format!("INSERT INTO log VALUES ({id}, {val})"), Some(txn)).is_err() {
                    if let Some(wal) = engine.wal_handle() {
                        let _ = wal.append(LogRecord::Abort { txn_id: txn });
                        let _ = wal.flush();
                    }
                    let _ = txn_mgr.abort(txn);
                    continue;
                }
                if let Some(wal) = engine.wal_handle() {
                    let _ = wal.append(LogRecord::Commit { txn_id: txn });
                    let _ = wal.flush();
                }
                if txn_mgr.commit(txn).is_ok() {
                    println!("COMMITTED {id}");
                }
            }
        });
        handles.push(h);
    }

    thread::sleep(Duration::from_secs(3600));
    for h in handles { let _ = h.join(); }
}
