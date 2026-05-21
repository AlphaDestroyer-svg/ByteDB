//! Mixed-workload realistic stress test for ByteDB.
//!
//! Workload mix (configurable via env MIX_PCT_READ, default 70):
//!   - read txns:  short SELECT, range scan, secondary-column lookup
//!   - write txns: read-modify-write balance + insert into balance_tx
//!   - long txns (every Nth thread): hold a snapshot for 100-500ms,
//!                                   do many reads + a few writes
//!
//! Contention amplifier: small dataset (default 200 users) so 32 threads
//! collide on the same rows. This is where MVCC correctness gets stress-tested.

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

#[derive(Default, Clone)]
struct OpStats {
    latencies_us: Vec<u64>,
    commits: u64,
    conflicts: u64,
    errors: u64,
    panics: u64,
}

#[derive(Default)]
struct ThreadStats {
    short_reads: OpStats,
    range_scans: OpStats,
    sec_lookups: OpStats,
    rmw_writes: OpStats,
    long_txns: OpStats,
}

fn main() {
    let num_users: usize = std::env::var("USERS").ok().and_then(|s| s.parse().ok()).unwrap_or(200);
    let num_threads: usize = std::env::var("THREADS").ok().and_then(|s| s.parse().ok()).unwrap_or(32);
    let test_secs: u64 = std::env::var("SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(20);
    let pct_read: u32 = std::env::var("MIX_PCT_READ").ok().and_then(|s| s.parse().ok()).unwrap_or(70);
    let long_every: usize = std::env::var("LONG_EVERY").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
    let hold_ms: u64 = std::env::var("HOLD_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let txn_timeout = Duration::from_secs(10);

    println!("=== ByteDB mixed-workload stress test ===");
    println!("users:        {num_users}");
    println!("threads:      {num_threads}");
    println!("duration:     {test_secs}s");
    println!("read mix:     {pct_read}%");
    println!("long-tx every {long_every}-th iteration");
    println!("rmw hold:     {hold_ms}ms (force overlap)");
    println!("isolation:    RepeatableRead");
    println!();

    let db = Arc::new(Database::new("stress_mixed"));
    let txn_mgr = Arc::new(TransactionManager::new());
    txn_mgr.set_default_txn_timeout(Some(txn_timeout));
    let engine = Arc::new(QueryEngine::new(db, Arc::clone(&txn_mgr)));

    println!("[setup] creating tables...");
    run_sql(&engine, "CREATE TABLE users (id INT PRIMARY KEY, balance INT, name TEXT, region TEXT)", None).unwrap();
    run_sql(&engine, "CREATE TABLE balance_tx (id INT PRIMARY KEY, user_id INT, amount INT, kind TEXT)", None).unwrap();

    println!("[setup] seeding {num_users} users...");
    let regions = ["us-east", "us-west", "eu", "asia"];
    let seed_start = Instant::now();
    for i in 0..num_users {
        let r = regions[i % regions.len()];
        let sql = format!("INSERT INTO users VALUES ({i}, 100000, 'user_{i}', '{r}')");
        run_sql(&engine, &sql, None).unwrap();
    }
    println!("[setup] seeded in {:.2}s", seed_start.elapsed().as_secs_f64());
    println!();

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
            let mut stats = ThreadStats::default();
            let mut iter: u64 = 0;
            let mut rng_state: u64 = (t as u64).wrapping_mul(0x9E3779B97F4A7C15) ^ 0xDEADBEEFCAFEBABE;

            while !stop.load(Ordering::Relaxed) {
                iter += 1;
                rng_state ^= rng_state << 13;
                rng_state ^= rng_state >> 7;
                rng_state ^= rng_state << 17;
                let user_id = (rng_state as usize) % num_users;
                let amount = ((rng_state >> 32) as i64 % 100).abs() + 1;
                let r = (rng_state >> 16) as u32 % 100;
                let region = ["us-east", "us-west", "eu", "asia"][(rng_state >> 24) as usize % 4];

                let is_long = (iter as usize) % long_every == 0;
                let is_read = !is_long && r < pct_read;

                let op_start = Instant::now();
                let result: std::thread::Result<Result<&'static str, String>> = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let txn = txn_mgr.begin(IsolationLevel::RepeatableRead);

                    if is_long {
                        for _ in 0..5 {
                            let uid = (rng_state as usize) % num_users;
                            run_sql(&engine, &format!("SELECT balance FROM users WHERE id = {uid}"), Some(txn))
                                .map_err(|e| { let _ = txn_mgr.abort(txn); e })?;
                            rng_state ^= rng_state << 5;
                        }
                        run_sql(&engine, &format!("SELECT id, balance FROM users WHERE region = '{region}'"), Some(txn))
                            .map_err(|e| { let _ = txn_mgr.abort(txn); e })?;
                        thread::sleep(Duration::from_millis(50));
                        let tx_pk = tx_id_counter.fetch_add(1, Ordering::Relaxed);
                        run_sql(
                            &engine,
                            &format!("UPDATE users SET balance = balance + {amount} WHERE id = {user_id}"),
                            Some(txn),
                        ).map_err(|e| { let _ = txn_mgr.abort(txn); e })?;
                        run_sql(
                            &engine,
                            &format!("INSERT INTO balance_tx VALUES ({tx_pk}, {user_id}, {amount}, 'long')"),
                            Some(txn),
                        ).map_err(|e| { let _ = txn_mgr.abort(txn); e })?;
                        txn_mgr.commit(txn).map_err(|e| format!("commit: {e:?}"))?;
                        Ok("long")
                    } else if is_read {
                        let pick = (rng_state >> 8) as u32 % 3;
                        match pick {
                            0 => {
                                run_sql(&engine, &format!("SELECT balance FROM users WHERE id = {user_id}"), Some(txn))
                                    .map_err(|e| { let _ = txn_mgr.abort(txn); e })?;
                                txn_mgr.commit(txn).map_err(|e| format!("commit: {e:?}"))?;
                                Ok("short_read")
                            }
                            1 => {
                                let lo = user_id.saturating_sub(10);
                                let hi = (user_id + 10).min(num_users - 1);
                                run_sql(&engine, &format!("SELECT id, balance FROM users WHERE id >= {lo} AND id <= {hi}"), Some(txn))
                                    .map_err(|e| { let _ = txn_mgr.abort(txn); e })?;
                                txn_mgr.commit(txn).map_err(|e| format!("commit: {e:?}"))?;
                                Ok("range")
                            }
                            _ => {
                                run_sql(&engine, &format!("SELECT id, balance FROM users WHERE region = '{region}'"), Some(txn))
                                    .map_err(|e| { let _ = txn_mgr.abort(txn); e })?;
                                txn_mgr.commit(txn).map_err(|e| format!("commit: {e:?}"))?;
                                Ok("sec_lookup")
                            }
                        }
                    } else {
                        let tx_pk = tx_id_counter.fetch_add(1, Ordering::Relaxed);
                        run_sql(&engine, &format!("SELECT balance FROM users WHERE id = {user_id}"), Some(txn))
                            .map_err(|e| { let _ = txn_mgr.abort(txn); e })?;
                        if hold_ms > 0 {
                            thread::sleep(Duration::from_millis(hold_ms));
                        }
                        run_sql(
                            &engine,
                            &format!("UPDATE users SET balance = balance + {amount} WHERE id = {user_id}"),
                            Some(txn),
                        ).map_err(|e| { let _ = txn_mgr.abort(txn); e })?;
                        run_sql(
                            &engine,
                            &format!("INSERT INTO balance_tx VALUES ({tx_pk}, {user_id}, {amount}, 'topup')"),
                            Some(txn),
                        ).map_err(|e| { let _ = txn_mgr.abort(txn); e })?;
                        txn_mgr.commit(txn).map_err(|e| format!("commit: {e:?}"))?;
                        Ok("rmw")
                    }
                }));

                let elapsed_us = op_start.elapsed().as_micros() as u64;

                let bucket: &mut OpStats = match (&result, is_long) {
                    (Ok(Ok("long")), _) => &mut stats.long_txns,
                    (Ok(Ok("short_read")), _) => &mut stats.short_reads,
                    (Ok(Ok("range")), _) => &mut stats.range_scans,
                    (Ok(Ok("sec_lookup")), _) => &mut stats.sec_lookups,
                    (Ok(Ok("rmw")), _) => &mut stats.rmw_writes,
                    (_, true) => &mut stats.long_txns,
                    _ => &mut stats.rmw_writes,
                };
                bucket.latencies_us.push(elapsed_us);
                match &result {
                    Ok(Ok(_)) => bucket.commits += 1,
                    Ok(Err(e)) => {
                        if e.contains("conflict") || e.contains("Conflict") || e.contains("abort") {
                            bucket.conflicts += 1;
                        } else {
                            bucket.errors += 1;
                            if bucket.errors <= 3 {
                                eprintln!("[t{t}] error: {e}");
                            }
                        }
                    }
                    Err(_) => bucket.panics += 1,
                }
            }
            stats
        });
        handles.push(h);
    }

    for elapsed in 1..=test_secs {
        thread::sleep(Duration::from_secs(1));
        if elapsed % 5 == 0 {
            let active = txn_mgr.active_count();
            println!("[t+{elapsed:>2}s] active txns: {active}");
        }
    }
    stop.store(true, Ordering::Relaxed);

    let mut agg = ThreadStats::default();
    for h in handles {
        match h.join() {
            Ok(s) => {
                agg.short_reads.merge(&s.short_reads);
                agg.range_scans.merge(&s.range_scans);
                agg.sec_lookups.merge(&s.sec_lookups);
                agg.rmw_writes.merge(&s.rmw_writes);
                agg.long_txns.merge(&s.long_txns);
            }
            Err(_) => agg.rmw_writes.panics += 1,
        }
    }

    let wall = start.elapsed();
    println!();
    println!("=== results (wall {:.2}s) ===", wall.as_secs_f64());
    print_op("short_read    ", &agg.short_reads, wall);
    print_op("range_scan    ", &agg.range_scans, wall);
    print_op("sec_lookup    ", &agg.sec_lookups, wall);
    print_op("rmw_write     ", &agg.rmw_writes, wall);
    print_op("long_txn      ", &agg.long_txns, wall);

    let total_commits = agg.short_reads.commits + agg.range_scans.commits + agg.sec_lookups.commits
        + agg.rmw_writes.commits + agg.long_txns.commits;
    let total_conflicts = agg.short_reads.conflicts + agg.range_scans.conflicts + agg.sec_lookups.conflicts
        + agg.rmw_writes.conflicts + agg.long_txns.conflicts;
    let total_errors = agg.short_reads.errors + agg.range_scans.errors + agg.sec_lookups.errors
        + agg.rmw_writes.errors + agg.long_txns.errors;
    let total_panics = agg.short_reads.panics + agg.range_scans.panics + agg.sec_lookups.panics
        + agg.rmw_writes.panics + agg.long_txns.panics;

    println!();
    println!("=== totals ===");
    println!("total commits:   {total_commits}");
    println!("total conflicts: {total_conflicts}");
    println!("total errors:    {total_errors}");
    println!("total panics:    {total_panics}");
    println!("throughput:      {:.0} commits/s", total_commits as f64 / wall.as_secs_f64());
}

impl OpStats {
    fn merge(&mut self, other: &OpStats) {
        self.latencies_us.extend_from_slice(&other.latencies_us);
        self.commits += other.commits;
        self.conflicts += other.conflicts;
        self.errors += other.errors;
        self.panics += other.panics;
    }
}

fn print_op(label: &str, s: &OpStats, wall: Duration) {
    let mut lats = s.latencies_us.clone();
    lats.sort_unstable();
    let tps = s.commits as f64 / wall.as_secs_f64();
    let p50 = percentile(&lats, 0.50);
    let p95 = percentile(&lats, 0.95);
    let p99 = percentile(&lats, 0.99);
    println!(
        "{label} commits={:>8} confl={:>5} err={:>3} panic={:>3} tps={:>8.0} p50={:>7}us p95={:>7}us p99={:>7}us",
        s.commits, s.conflicts, s.errors, s.panics, tps, p50, p95, p99
    );
}
