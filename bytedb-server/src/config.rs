use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Clone, Parser)]
#[command(name = "bytedb-server", about = "ByteDB Database Server")]
pub struct Config {
    #[arg(short, long, default_value = "127.0.0.1")]
    pub host: String,

    #[arg(short, long, default_value_t = 7654)]
    pub port: u16,

    #[arg(short, long, default_value = "./bytedb_data")]
    pub data_dir: PathBuf,

    #[arg(long, default_value_t = 32768)]
    pub buffer_pool_size: usize,

    #[arg(long, default_value_t = 128)]
    pub max_connections: usize,

    #[arg(long, default_value_t = 300)]
    pub connection_timeout_secs: u64,

    #[arg(long, default_value_t = 0)]
    pub statement_timeout_ms: u64,

    #[arg(long, default_value_t = 0)]
    pub max_scan_rows: u64,

    #[arg(long, default_value_t = 0)]
    pub max_query_memory_mb: u64,

    #[arg(long, default_value = "strict")]
    pub durability: String,

    #[arg(long, default_value_t = 0)]
    pub group_commit_delay_us: u64,

    #[arg(long, default_value_t = 0)]
    pub metrics_port: u16,

    #[arg(long)]
    pub admin_password: Option<String>,

    #[arg(long)]
    pub tls_cert: Option<PathBuf>,

    #[arg(long)]
    pub tls_key: Option<PathBuf>,

    #[arg(long, default_value_t = 1800)]
    pub snapshot_interval_secs: u64,

    #[arg(long, default_value_t = 0)]
    pub snapshot_write_threshold: u64,

    #[arg(long, default_value = "binary")]
    pub snapshot_format: String,

    #[arg(long, default_value_t = false)]
    pub no_snapshot: bool,

    #[arg(long, default_value_t = false)]
    pub no_shutdown_snapshot: bool,
}

impl Config {
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn wal_path(&self) -> PathBuf {
        self.data_dir.join("bytedb.wal")
    }

    pub fn snapshot_dir(&self) -> PathBuf {
        self.data_dir.join("snapshots")
    }
}
