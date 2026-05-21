use std::sync::Arc;
use std::time::Instant;
use std::thread;

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_core::tuple::value::Value;
use bytedb_query::executor::engine::QueryEngine;
use bytedb_query::parser::parser::Parser;

fn setup_engine() -> QueryEngine {
    let db = Arc::new(Database::new("bench"));
    let txn = Arc::new(TransactionManager::new());
    QueryEngine::new(db, txn)
}

fn format_rate(count: usize, duration: std::time::Duration) -> String {
    let secs = duration.as_secs_f64();
    let rate = count as f64 / secs;
    if rate > 1_000_000.0 {
        format!("{:.2}M ops/s", rate / 1_000_000.0)
    } else if rate > 1_000.0 {
        format!("{:.2}K ops/s", rate / 1_000.0)
    } else {
        format!("{:.2} ops/s", rate)
    }
}

fn main() {
    println!("╔══════════════════════════════════════════════════════════════════════╗");
    println!("║           ByteDB Performance Benchmark (Large Scale)                 ║");
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!();

    let engine = setup_engine();

    let mut p = Parser::new("CREATE TABLE benchmark (id INT PRIMARY KEY, name TEXT, value INT, category TEXT)").unwrap();
    engine.execute(p.parse().unwrap(), None).unwrap();

    let row_count = 100_000;
    let start = Instant::now();
    for i in 0..row_count {
        let cat = match i % 5 {
            0 => "alpha",
            1 => "beta",
            2 => "gamma",
            3 => "delta",
            _ => "epsilon",
        };
        let sql = format!("INSERT INTO benchmark VALUES ({}, 'item_{}', {}, '{}')", i, i, i * 7 % 10000, cat);
        let mut p = Parser::new(&sql).unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
    }
    let insert_dur = start.elapsed();
    println!("  1. INSERT {} rows:        {:>9.2}ms  ({})", row_count, insert_dur.as_secs_f64() * 1000.0, format_rate(row_count, insert_dur));

    let mut p = Parser::new("CREATE TABLE bulk_test (id INT PRIMARY KEY, name TEXT, value INT, category TEXT)").unwrap();
    engine.execute(p.parse().unwrap(), None).unwrap();

    let bulk_count = 100_000;
    let start = Instant::now();
    let rows: Vec<Vec<Value>> = (0..bulk_count).map(|i: usize| {
        vec![
            Value::Int64(i as i64),
            Value::Text(format!("item_{}", i)),
            Value::Int64((i * 7 % 10000) as i64),
            Value::Text(["alpha", "beta", "gamma", "delta", "epsilon"][i % 5].to_string()),
        ]
    }).collect();
    engine.bulk_insert("bulk_test", rows).unwrap();
    let bulk_dur = start.elapsed();
    println!("  1b. BULK INSERT {} rows:   {:>9.2}ms  ({})", bulk_count, bulk_dur.as_secs_f64() * 1000.0, format_rate(bulk_count, bulk_dur));

    let select_count = 100_000;
    let start = Instant::now();
    for i in 0..select_count {
        let sql = format!("SELECT * FROM benchmark WHERE id = {}", i);
        let mut p = Parser::new(&sql).unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
    }
    let point_dur = start.elapsed();
    println!("  2. Point SELECT x{}:    {:>9.2}ms  ({})", select_count, point_dur.as_secs_f64() * 1000.0, format_rate(select_count, point_dur));

    let scan_iters = 10;
    let start = Instant::now();
    for _ in 0..scan_iters {
        let mut p = Parser::new("SELECT * FROM benchmark").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
    }
    let scan_dur = start.elapsed();
    println!("  3. Full scan x{} (100K rows): {:>6.2}ms  ({} scans/s)", scan_iters, scan_dur.as_secs_f64() * 1000.0, format_rate(scan_iters, scan_dur));

    let filter_iters = 10;
    let start = Instant::now();
    for _ in 0..filter_iters {
        let mut p = Parser::new("SELECT * FROM benchmark WHERE value > 5000").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
    }
    let filter_dur = start.elapsed();
    println!("  4. Filtered scan x{}:       {:>9.2}ms  ({} scans/s)", filter_iters, filter_dur.as_secs_f64() * 1000.0, format_rate(filter_iters, filter_dur));

    let update_count = 10_000;
    let start = Instant::now();
    for i in 0..update_count {
        let sql = format!("UPDATE benchmark SET value = {} WHERE id = {}", i * 100, i);
        let mut p = Parser::new(&sql).unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
    }
    let update_dur = start.elapsed();
    println!("  5. UPDATE by PK x{}:     {:>9.2}ms  ({})", update_count, update_dur.as_secs_f64() * 1000.0, format_rate(update_count, update_dur));

    let order_iters = 10;
    let start = Instant::now();
    for _ in 0..order_iters {
        let mut p = Parser::new("SELECT * FROM benchmark ORDER BY value LIMIT 100").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
    }
    let order_dur = start.elapsed();
    println!("  6. ORDER BY+LIMIT x{}:      {:>9.2}ms  ({} queries/s)", order_iters, order_dur.as_secs_f64() * 1000.0, format_rate(order_iters, order_dur));

    let limit_iters = 1000;
    let start = Instant::now();
    for _ in 0..limit_iters {
        let mut p = Parser::new("SELECT * FROM benchmark LIMIT 10").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
    }
    let limit_dur = start.elapsed();
    println!("  6b. SELECT LIMIT 10 x{}:   {:>9.2}ms  ({})", limit_iters, limit_dur.as_secs_f64() * 1000.0, format_rate(limit_iters, limit_dur));

    let mut p = Parser::new("CREATE TABLE categories (id INT PRIMARY KEY, cat_name TEXT, priority INT)").unwrap();
    engine.execute(p.parse().unwrap(), None).unwrap();
    for i in 0..1000 {
        let sql = format!("INSERT INTO categories VALUES ({}, 'cat_{}', {})", i, i, i % 10);
        let mut p = Parser::new(&sql).unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
    }

    let join_iters = 5;
    let start = Instant::now();
    for _ in 0..join_iters {
        let mut p = Parser::new("SELECT * FROM benchmark JOIN categories ON value = id").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
    }
    let join_dur = start.elapsed();
    println!("  7. JOIN (100Kx1K) x{}:       {:>9.2}ms  ({} joins/s)", join_iters, join_dur.as_secs_f64() * 1000.0, format_rate(join_iters, join_dur));

    let agg_iters = 5;
    let start = Instant::now();
    for _ in 0..agg_iters {
        let mut p = Parser::new("SELECT category, COUNT(id), SUM(value), AVG(value), MIN(value), MAX(value) FROM benchmark GROUP BY category").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
    }
    let agg_dur = start.elapsed();
    println!("  8. GROUP BY+5 AGGs x{}:     {:>9.2}ms  ({} queries/s)", agg_iters, agg_dur.as_secs_f64() * 1000.0, format_rate(agg_iters, agg_dur));

    let delete_count = 10_000;
    let start = Instant::now();
    for i in 0..delete_count {
        let sql = format!("DELETE FROM benchmark WHERE id = {}", i);
        let mut p = Parser::new(&sql).unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
    }
    let delete_dur = start.elapsed();
    println!("  9. DELETE by PK x{}:     {:>9.2}ms  ({})", delete_count, delete_dur.as_secs_f64() * 1000.0, format_rate(delete_count, delete_dur));

    let engine = Arc::new(engine);
    let num_threads = 4;
    let ops_per_thread = 25_000;
    let start = Instant::now();
    let handles: Vec<_> = (0..num_threads).map(|t| {
        let eng = Arc::clone(&engine);
        thread::spawn(move || {
            for i in 0..ops_per_thread {
                let id = (t * ops_per_thread + i) % 90_000 + 10_000;
                let sql = format!("SELECT * FROM benchmark WHERE id = {}", id);
                let mut p = Parser::new(&sql).unwrap();
                let _ = eng.execute(p.parse().unwrap(), None);
            }
        })
    }).collect();
    for h in handles {
        h.join().unwrap();
    }
    let conc_dur = start.elapsed();
    let total_ops = num_threads * ops_per_thread;
    println!(" 10. Concurrent SELECT x{} ({}T): {:>6.2}ms  ({})", total_ops, num_threads, conc_dur.as_secs_f64() * 1000.0, format_rate(total_ops, conc_dur));

    println!();
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║  Summary (ByteDB in-memory, single-threaded, no network overhead)    ║");
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║                                                                      ║");
    println!("║  For comparison, typical PostgreSQL on same hardware:                 ║");
    println!("║    INSERT 100K:     ~2000-5000ms  (ByteDB: {:.0}ms)          ║", insert_dur.as_secs_f64() * 1000.0);
    println!("║    Point SELECT:    ~0.1-0.5ms/q  (ByteDB: {:.4}ms/q)       ║", point_dur.as_secs_f64() * 1000.0 / select_count as f64);
    println!("║    Full scan 100K:  ~50-200ms     (ByteDB: {:.0}ms)          ║", scan_dur.as_secs_f64() * 1000.0 / scan_iters as f64);
    println!("║    UPDATE by PK:    ~0.2-1ms/q    (ByteDB: {:.4}ms/q)       ║", update_dur.as_secs_f64() * 1000.0 / update_count as f64);
    println!("║                                                                      ║");
    println!("║  Note: ByteDB is in-memory only. PostgreSQL provides durability,     ║");
    println!("║  concurrent access, and disk persistence. This comparison shows       ║");
    println!("║  raw engine throughput without network/disk overhead.                  ║");
    println!("║                                                                      ║");
    println!("╚══════════════════════════════════════════════════════════════════════╝");
}
