use std::fmt::Write as _;

use bytedb_core::wal::log_manager::LogManager;
use bytedb_query::executor::engine::QueryEngine;

#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub queries_total: u64,
    pub qps: f64,
    pub lat_mean_us: u64,
    pub lat_p50_us: u64,
    pub lat_p95_us: u64,
    pub lat_p99_us: u64,
    pub lat_max_us: u64,
    pub wal_fsync_total: u64,
    pub wal_commits_total: u64,
    pub wal_relaxed_acks: u64,
    pub wal_current_lsn: u64,
    pub wal_flushed_lsn: u64,
    pub conns_active: u64,
    pub conns_max: u64,
}

impl MetricsSnapshot {
    pub fn gather(
        engine: &QueryEngine,
        wal: &LogManager,
        conns_active: u64,
        conns_max: u64,
    ) -> Self {
        let hist = engine.query_latency();
        let lat = hist.snapshot();
        MetricsSnapshot {
            queries_total: lat.total_count,
            qps: hist.qps(),
            lat_mean_us: lat.mean_micros,
            lat_p50_us: lat.p50_micros,
            lat_p95_us: lat.p95_micros,
            lat_p99_us: lat.p99_micros,
            lat_max_us: lat.max_micros,
            wal_fsync_total: wal.fsync_count(),
            wal_commits_total: wal.commits_served(),
            wal_relaxed_acks: wal.relaxed_acks(),
            wal_current_lsn: wal.current_lsn(),
            wal_flushed_lsn: wal.flushed_lsn(),
            conns_active,
            conns_max,
        }
    }

    pub fn to_prometheus(&self) -> String {
        let mut o = String::with_capacity(1536);

        counter(&mut o, "bytedb_queries_total", "Total queries executed", self.queries_total);
        gauge_f(&mut o, "bytedb_query_qps", "Queries per second since last window reset", self.qps);

        o.push_str("# HELP bytedb_query_latency_micros Query latency in microseconds\n");
        o.push_str("# TYPE bytedb_query_latency_micros summary\n");
        let _ = writeln!(o, "bytedb_query_latency_micros{{quantile=\"0.5\"}} {}", self.lat_p50_us);
        let _ = writeln!(o, "bytedb_query_latency_micros{{quantile=\"0.95\"}} {}", self.lat_p95_us);
        let _ = writeln!(o, "bytedb_query_latency_micros{{quantile=\"0.99\"}} {}", self.lat_p99_us);
        gauge_u(&mut o, "bytedb_query_latency_micros_mean", "Mean query latency (microseconds)", self.lat_mean_us);
        gauge_u(&mut o, "bytedb_query_latency_micros_max", "Max query latency in the sample window", self.lat_max_us);

        counter(&mut o, "bytedb_wal_fsync_total", "WAL fsync operations", self.wal_fsync_total);
        counter(&mut o, "bytedb_wal_commits_total", "Commits served by the WAL", self.wal_commits_total);
        counter(&mut o, "bytedb_wal_relaxed_acks_total", "Commits acked without fsync (relaxed mode)", self.wal_relaxed_acks);
        gauge_u(&mut o, "bytedb_wal_current_lsn", "Highest assigned log sequence number", self.wal_current_lsn);
        gauge_u(&mut o, "bytedb_wal_flushed_lsn", "Highest durably flushed log sequence number", self.wal_flushed_lsn);

        gauge_u(&mut o, "bytedb_connections_active", "Active client connections", self.conns_active);
        gauge_u(&mut o, "bytedb_connections_max", "Maximum client connections", self.conns_max);

        o
    }
}

fn counter(o: &mut String, name: &str, help: &str, val: u64) {
    let _ = writeln!(o, "# HELP {name} {help}");
    let _ = writeln!(o, "# TYPE {name} counter");
    let _ = writeln!(o, "{name} {val}");
}

fn gauge_u(o: &mut String, name: &str, help: &str, val: u64) {
    let _ = writeln!(o, "# HELP {name} {help}");
    let _ = writeln!(o, "# TYPE {name} gauge");
    let _ = writeln!(o, "{name} {val}");
}

fn gauge_f(o: &mut String, name: &str, help: &str, val: f64) {
    let _ = writeln!(o, "# HELP {name} {help}");
    let _ = writeln!(o, "# TYPE {name} gauge");
    let _ = writeln!(o, "{name} {val:.3}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_expected_series() {
        let snap = MetricsSnapshot {
            queries_total: 42,
            qps: 3.5,
            lat_mean_us: 120,
            lat_p50_us: 90,
            lat_p95_us: 300,
            lat_p99_us: 800,
            lat_max_us: 1500,
            wal_fsync_total: 10,
            wal_commits_total: 40,
            wal_relaxed_acks: 0,
            wal_current_lsn: 100,
            wal_flushed_lsn: 98,
            conns_active: 3,
            conns_max: 128,
        };
        let text = snap.to_prometheus();
        assert!(text.contains("bytedb_queries_total 42"));
        assert!(text.contains("bytedb_query_latency_micros{quantile=\"0.99\"} 800"));
        assert!(text.contains("bytedb_wal_flushed_lsn 98"));
        assert!(text.contains("bytedb_connections_active 3"));
        assert!(text.contains("# TYPE bytedb_queries_total counter"));
    }
}
