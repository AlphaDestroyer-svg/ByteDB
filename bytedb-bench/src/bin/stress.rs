//! Realistic concurrent stress test for ByteDB.
//!
//! Mirrors the workload of a billing system:
//!   - read user balance
//!   - update user balance
//!   - insert a balance_transactions row
//! all inside a single transaction, hammered by N threads.
//!
//! Reports throughput and latency percentiles, and counts conflicts/errors
//! instead of swallowing them.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::thread;

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::{IsolationLevel, TransactionManager};
use bytedb_query::executor::engine::QueryEngine;
use bytedb_query::parser::parser::Parser;

fn run_sql(engine: &QueryEngine, sql: &str, txn_id: Option<u64>) -> Result<(), String> {
    let mut p = Parser::new(sql).map_err(|e| format!("parse {sql}: {e:?}"))?;
    let stmt = p.parse().map_err(|e| format!("parse {sql}: {e:?}"))?;
    engine.execute(stmt, txn_id).map_err(|e| format!("exec {sql}: {e:?}"))?;
    Ok(())
}

fn percentile(sorted_us: &[u64], p: f64) -> u64 {
    if sorted_us.is_empty() { return 0; }
    let idx = ((sorted_us.len() as f64 - 1.0) * p).round() as usize;
    sorted_us[idx]
}

fn main() {
    let num_users: usize = std::env::var("USERS").ok().and_then(|s| s.parse().ok()).unwrap_or(10_000);
    let num_threads: usize = std::env::var("THREADS").ok().and_then(|s| s.parse().ok()).unwrap_or(32);
    let test_secs: u64 = std::env::var("SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(30);
    let txn_timeout = Duration::from_secs(5);

    println!("=== ByteDB realistic stress test ===");
    println!("users:        {num_users}");
    println!("threads:      {num_threads}");
    println!("duration:     {test_secs}s");
    println!("isolation:    RepeatableRead");
    println!();

    // --- setup ---
    let db = Arc::new(Database::new("stress"));
    let txn_mgr = Arc::new(TransactionManager::new());
    txn_mgr.set_default_txn_timeout(Some(txn_timeout));
    let engine = Arc::new(QueryEngine::new(db, Arc::clone(&txn_mgr)));

    println!("[setup] creating tables...");
    run_sql(&engine, "CREATE TABLE users (id INT PRIMARY KEY, balance INT, name TEXT)", None).unwrap();
    run_sql(&engine, "CREATE TABLE balance_tx (id INT PRIMARY KEY, user_id INT, amount INT, kind TEXT)", None).unwrap();

    println!("[setup] seeding {num_users} users...");
    let seed_start = Instant::now();
    for i in 0..num_users {
        let sql = format!("INSERT INTO users VALUES ({i}, 100000, 'user_{i}')");
        run_sql(&engine, &sql, None).unwrap();
    }
    println!("[setup] seeded in {:.2}s", seed_start.elapsed().as_secs_f64());
    println!();

    // --- workload ---
    let stop = Arc::new(AtomicBool::new(false));
    let tx_id_counter = Arc::new(AtomicU64::new(1));

    let start = Instant::now();
    let mut handles = Vec::with_capacity(num_threads);

    for t in 0..num_threads {
        let engine = Arc::clone(&engine);
        let txn_mgr = Arc::clone(&txn_mgr);
        let stop = Arc::clone(&stop);
        let tx_id_counter = Arc::clone(&tx_id_counter);

        let h = thread::spawn(move || -> ThreadStats {
            let mut latencies_us: Vec<u64> = Vec::with_capacity(100_000);
            let mut commits: u64 = 0;
            let mut conflicts: u64 = 0;
            let mut errors: u64 = 0;
            let mut panics: u64 = 0;

            // deterministic-but-spread per-thread starting offset
            let mut rng_state: u64 = (t as u64).wrapping_mul(0x9E3779B97F4A7C15) ^ 0xDEADBEEFCAFEBABE;

            while !stop.load(Ordering::Relaxed) {
                rng_state ^= rng_state << 13;
                rng_state ^= rng_state >> 7;
                rng_state ^= rng_state << 17;
                let user_id = (rng_state as usize) % num_users;
                let amount = ((rng_state >> 32) as i64 % 100).abs() + 1;
                let tx_pk = tx_id_counter.fetch_add(1, Ordering::Relaxed);

                let op_start = Instant::now();

                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let txn = txn_mgr.begin(IsolationLevel::RepeatableRead);

                    // 1. read
                    if let Err(e) = run_sql(
                        &engine,
                        &format!("SELECT balance FROM users WHERE id = {user_id}"),
                        Some(txn),
                    ) {
                        let _ = txn_mgr.abort(txn);
                        return Err(e);
                    }

                    // 2. update balance
                    if let Err(e) = run_sql(
                        &engine,
                        &format!("UPDATE users SET balance = balance + {amount} WHERE id = {user_id}"),
                        Some(txn),
                    ) {
                        let _ = txn_mgr.abort(txn);
                        return Err(e);
                    }

                    // 3. insert balance_tx row
                    if let Err(e) = run_sql(
                        &engine,
                        &format!("INSERT INTO balance_tx VALUES ({tx_pk}, {user_id}, {amount}, 'topup')"),
                        Some(txn),
                    ) {
                        let _ = txn_mgr.abort(txn);
                        return Err(e);
                    }

                    txn_mgr.commit(txn).map_err(|e| format!("commit: {e:?}"))?;
                    Ok(())
                }));

                let elapsed_us = op_start.elapsed().as_micros() as u64;
                latencies_us.push(elapsed_us);

                match result {
                    Ok(Ok(())) => commits += 1,
                    Ok(Err(e)) => {
                        if e.contains("conflict") || e.contains("Conflict") || e.contains("abort") {
                            conflicts += 1;
                        } else {
                            errors += 1;
                            if errors <= 5 {
                                eprintln!("[t{t}] error: {e}");
                            }
                        }
                    }
                    Err(_) => panics += 1,
                }
            }

            ThreadStats { latencies_us, commits, conflicts, errors, panics }
        });
        handles.push(h);
    }

    // run for test_secs, then signal stop
    for elapsed in 1..=test_secs {
        thread::sleep(Duration::from_secs(1));
        let active = txn_mgr.active_count();
        if elapsed % 5 == 0 {
            println!("[t+{elapsed:>2}s] active txns: {active}");
        }
    }
    stop.store(true, Ordering::Relaxed);

    let mut all_latencies: Vec<u64> = Vec::new();
    let mut total_commits: u64 = 0;
    let mut total_conflicts: u64 = 0;
    let mut total_errors: u64 = 0;
    let mut total_panics: u64 = 0;

    for h in handles {
        match h.join() {
            Ok(s) => {
                all_latencies.extend(s.latencies_us);
                total_commits += s.commits;
                total_conflicts += s.conflicts;
                total_errors += s.errors;
                total_panics += s.panics;
            }
            Err(_) => total_panics += 1,
        }
    }

    let wall = start.elapsed();
    all_latencies.sort_unstable();

    let total_ops = total_commits + total_conflicts + total_errors + total_panics;
    let throughput = total_commits as f64 / wall.as_secs_f64();

    println!();
    println!("=== results ===");
    println!("wall time:    {:.2}s", wall.as_secs_f64());
    println!("total ops:    {total_ops}");
    println!("  commits:    {total_commits}");
    println!("  conflicts:  {total_conflicts}");
    println!("  errors:     {total_errors}");
    println!("  panics:     {total_panics}");
    println!("throughput:   {:.0} commits/s", throughput);
    println!();
    if !all_latencies.is_empty() {
        println!("latency (per txn, microseconds):");
        println!("  p50:        {} us", percentile(&all_latencies, 0.50));
        println!("  p95:        {} us", percentile(&all_latencies, 0.95));
        println!("  p99:        {} us", percentile(&all_latencies, 0.99));
        println!("  p999:       {} us", percentile(&all_latencies, 0.999));
        println!("  max:        {} us", *all_latencies.last().unwrap());
    }

    // sanity: peek at final state
    println!();
    println!("[verify] sampling rows...");
    let mut p = Parser::new("SELECT * FROM balance_tx LIMIT 3").unwrap();
    let stmt = p.parse().unwrap();
    match engine.execute(stmt, None) {
        Ok(r) => println!("[verify] balance_tx scan ok: {:?}", r),
        Err(e) => println!("[verify] balance_tx scan FAILED: {e:?}"),
    }
}

struct ThreadStats {
    latencies_us: Vec<u64>,
    commits: u64,
    conflicts: u64,
    errors: u64,
    panics: u64,
}
