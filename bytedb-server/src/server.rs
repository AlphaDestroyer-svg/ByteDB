use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{info, error, warn};

use crate::config::Config;
use crate::connection::handle_connection;
use crate::error::Result;
use crate::auth::credentials::{Credentials, SessionManager};
use bytedb_core::catalog::database::Database;
use bytedb_core::catalog::table::TableMeta;
use bytedb_core::index::btree::BPlusTree;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_core::mvcc::version_store::VersionStore;
use bytedb_core::snapshot::format::{FullSnapshot, SnapshotFormat, TableSnapshot};
use bytedb_core::snapshot::manager::SnapshotManager;
use bytedb_core::wal::log_manager::{LogManager, DurabilityMode};
use bytedb_core::wal::recovery::RecoveryManager;
use bytedb_core::workers::{wal_flusher, vacuum, WorkerHandle};
use bytedb_query::executor::engine::{QueryEngine, TableData};
use bytedb_query::kv::kv_engine::KvEngine;
use bytedb_query::document::doc_engine::DocEngine;

pub struct Server {
    config: Config,
    query_engine: Arc<QueryEngine>,
    kv_engine: Arc<KvEngine>,
    doc_engine: Arc<DocEngine>,
    credentials: Arc<Credentials>,
    session_manager: Arc<SessionManager>,
    semaphore: Arc<Semaphore>,
    snapshot_manager: Arc<SnapshotManager>,
    wal: Arc<LogManager>,

    workers: parking_lot::Mutex<Vec<WorkerHandle>>,
}

impl Server {
    pub fn new(config: Config) -> Self {
        std::fs::create_dir_all(&config.data_dir).ok();

        let snapshot_format = match config.snapshot_format.as_str() {
            "json" => SnapshotFormat::Json,
            _ => SnapshotFormat::Binary,
        };
        let snapshot_manager = Arc::new(SnapshotManager::new(
            config.snapshot_dir(),
            config.snapshot_write_threshold,
            config.snapshot_interval_secs,
            snapshot_format,
        ));

        let wal_path = config.wal_path();
        let log_manager = Arc::new(
            LogManager::new(&wal_path).expect("Failed to open WAL")
        );

        match config.durability.to_ascii_lowercase().as_str() {
            "relaxed" => {
                log_manager.set_durability_mode(DurabilityMode::Relaxed);
                warn!("Durability mode: RELAXED (commits ack before fsync; recent commits may be lost on crash)");
            }
            _ => {
                log_manager.set_durability_mode(DurabilityMode::Strict);
                info!("Durability mode: STRICT (fsync on every commit)");
            }
        }
        if config.group_commit_delay_us > 0 {
            log_manager.set_group_commit_delay_us(config.group_commit_delay_us);
            info!("Group commit delay set to {} us", config.group_commit_delay_us);
        }

        let pitr_marker = config.data_dir.join("pitr_target.txt");
        let pitr_target: Option<u64> = if pitr_marker.exists() {
            match std::fs::read_to_string(&pitr_marker) {
                Ok(s) => match s.trim().parse::<u64>() {
                    Ok(lsn) => {
                        info!("PITR target LSN={} detected", lsn);
                        Some(lsn)
                    }
                    Err(e) => {
                        warn!("Invalid pitr_target.txt contents: {}", e);
                        None
                    }
                },
                Err(e) => {
                    warn!("Failed to read pitr_target.txt: {}", e);
                    None
                }
            }
        } else {
            None
        };

        if pitr_marker.exists() {
            if let Err(e) = std::fs::remove_file(&pitr_marker) {
                warn!("Failed to remove pitr_target.txt after replay: {}", e);
            }
        }

        let database = Arc::new(Database::new("bytedb"));
        let txn_manager = Arc::new(TransactionManager::new());
        let wal_for_engine = Arc::clone(&log_manager);
        let mut engine_owned = QueryEngine::with_wal(database, txn_manager, wal_for_engine);

        match bytedb_query::executor::diskstore::DiskStore::open(
            config.data_dir.clone(),
            "bytedb",
        ) {
            Ok(ds) => {
                engine_owned.attach_disk_store(ds);
                info!("Disk store attached at {:?}", config.data_dir);
            }
            Err(e) => {
                warn!("Failed to open disk store: {}. Continuing in-memory only.", e);
            }
        }

        let recovery = match pitr_target {
            Some(lsn) => {
                info!("PITR target LSN={} set, recovering to that point", lsn);
                RecoveryManager::recover_to_lsn(&log_manager, lsn)
            }
            None => RecoveryManager::recover(&log_manager),
        };
        match recovery {
            Ok(result) => {
                if !result.redo_records.is_empty() {
                    info!("Replaying {} WAL redo records on top of disk state", result.redo_records.len());
                    engine_owned.replay_wal_recovery(&result);
                }
            }
            Err(e) => {
                warn!("WAL recovery failed, continuing with disk state: {}", e);
            }
        }

        let query_engine = Arc::new(engine_owned);

        query_engine.set_statement_timeout_ms(config.statement_timeout_ms);
        query_engine.set_resource_limits(
            config.max_scan_rows,
            config.max_query_memory_mb.saturating_mul(1024 * 1024),
        );

        if let Ok(Some(snapshot)) = snapshot_manager.load_latest() {
            info!("Restoring from snapshot (LSN: {}, {} tables)", snapshot.header.lsn, snapshot.tables.len());
            Self::restore_from_snapshot(&query_engine, &snapshot);
            info!("Snapshot restored successfully");
        }

        let kv_engine = Arc::new(KvEngine::new());
        let doc_engine = Arc::new(DocEngine::new());

        let admin_password = config.admin_password.clone()
            .filter(|p| !p.is_empty())
            .or_else(|| std::env::var("BYTEDB_ADMIN_PASSWORD").ok().filter(|p| !p.is_empty()));
        let credentials = match admin_password {
            Some(pw) => {
                info!("Admin password configured from startup options");
                Arc::new(Credentials::with_admin(&pw))
            }
            None => {
                let generated = crate::auth::credentials::generate_password();
                warn!("No admin password set (--admin-password or BYTEDB_ADMIN_PASSWORD).");
                warn!("Generated one-time admin password: {}", generated);
                warn!("Set an explicit password before running in production.");
                Arc::new(Credentials::with_admin(&generated))
            }
        };
        let session_manager = Arc::new(SessionManager::new());
        let semaphore = Arc::new(Semaphore::new(config.max_connections));

        let vacuum_pass = {
            let engine = Arc::clone(&query_engine);
            let txn_mgr = engine.txn_manager_arc();
            vacuum::MvccVacuum::new(txn_mgr, move || engine.snapshot_version_stores())
        };
        let workers = vec![
            wal_flusher::start(
                Arc::clone(&log_manager),
                wal_flusher::WalFlusherConfig::default(),
            ),
            vacuum::start(vacuum_pass, vacuum::VacuumConfig::default()),
        ];

        Server {
            config,
            query_engine,
            kv_engine,
            doc_engine,
            credentials,
            session_manager,
            semaphore,
            snapshot_manager,
            wal: log_manager,
            workers: parking_lot::Mutex::new(workers),
        }
    }

    fn restore_from_snapshot(engine: &QueryEngine, snapshot: &FullSnapshot) {
        use std::collections::HashMap;
        use bytedb_core::tuple::schema::SequenceGenerator;
        let mut tables = engine.tables().write();
        for table_snap in &snapshot.tables {
            if tables.contains_key(&table_snap.name) {
                info!(
                    "Table '{}' already restored from disk store; keeping disk state over snapshot",
                    table_snap.name
                );
                continue;
            }
            let schema = table_snap.schema.clone();
            let index = Arc::new(BPlusTree::new(format!("{}_pk", table_snap.name), 128));
            for (key, value) in &table_snap.entries {
                let _ = index.insert(key.clone(), value.clone());
            }
            let mut sequences: HashMap<String, Arc<SequenceGenerator>> = HashMap::new();
            for c in &schema.columns {
                if c.auto_increment {
                    let start = table_snap.sequences.iter()
                        .find(|(n, _)| n == &c.name)
                        .map(|(_, v)| *v)
                        .unwrap_or(1);
                    sequences.insert(c.name.clone(), Arc::new(SequenceGenerator::new(start)));
                }
            }
            let check_exprs: Vec<_> = schema.check_constraints.iter()
                .filter_map(|s| bytedb_query::parser::parser::Parser::parse_expression(s).ok())
                .collect();
            let meta = TableMeta::new(table_snap.name.clone(), schema.clone(), table_snap.table_id);
            let _ = engine.database().create_table(meta);
            tables.insert(table_snap.name.clone(), Arc::new(TableData {
                schema,
                index,
                version_store: Arc::new(VersionStore::new()),
                check_exprs,
                sequences,
                secondary_indexes: Vec::new(),
                write_lock: Arc::new(parking_lot::Mutex::new(())),
            }));
        }
    }

    fn create_snapshot(engine: &QueryEngine, snapshot_manager: &SnapshotManager) {
        let tables = engine.tables().read();
        let mut table_snapshots = Vec::with_capacity(tables.len());

        for (name, table_data) in tables.iter() {
            let entries = table_data.index.scan_all().unwrap_or_default();
            let sequences: Vec<(String, i64)> = table_data.sequences.iter()
                .map(|(c, s)| (c.clone(), s.counter.load(std::sync::atomic::Ordering::SeqCst)))
                .collect();
            table_snapshots.push(TableSnapshot {
                name: name.clone(),
                table_id: table_data.schema.columns.len() as u32,
                schema: table_data.schema.clone(),
                entries,
                sequences,
            });
        }
        drop(tables);

        let header = snapshot_manager.create_snapshot_header(0, table_snapshots.len() as u32);
        let snapshot = FullSnapshot { header, tables: table_snapshots };

        match snapshot_manager.save(&snapshot) {
            Ok(path) => info!("Snapshot saved: {:?}", path),
            Err(e) => error!("Failed to save snapshot: {}", e),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let addr = self.config.addr();
        let listener = TcpListener::bind(&addr).await?;
        info!("ByteDB server listening on {}", addr);

        let timeout_secs = self.config.connection_timeout_secs;

        #[cfg(feature = "tls")]
        let tls_acceptor: Option<tokio_rustls::TlsAcceptor> =
            match (&self.config.tls_cert, &self.config.tls_key) {
                (Some(cert), Some(key)) => match crate::tls::build_acceptor(cert, key) {
                    Ok(acc) => {
                        info!("TLS enabled (cert: {:?})", cert);
                        Some(acc)
                    }
                    Err(e) => {
                        error!("TLS setup failed: {}", e);
                        std::process::exit(1);
                    }
                },
                (None, None) => {
                    warn!("TLS not configured; connections are PLAINTEXT");
                    None
                }
                _ => {
                    error!("Both --tls-cert and --tls-key must be provided to enable TLS");
                    std::process::exit(1);
                }
            };

        let snap_interval = self.snapshot_manager.interval();
        let snapshots_disabled = self.config.no_snapshot || snap_interval.as_secs() == 0;
        if snapshots_disabled {
            info!("Background snapshots disabled (no_snapshot={}, interval_secs={})",
                self.config.no_snapshot, snap_interval.as_secs());
        } else {
            let snap_engine = Arc::clone(&self.query_engine);
            let snap_manager = Arc::clone(&self.snapshot_manager);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(snap_interval);
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    Self::create_snapshot(&snap_engine, &snap_manager);
                }
            });
        }

        if self.config.metrics_port > 0 {
            let metrics_addr = format!("{}:{}", self.config.host, self.config.metrics_port);
            let m_engine = Arc::clone(&self.query_engine);
            let m_wal = Arc::clone(&self.wal);
            let m_sem = Arc::clone(&self.semaphore);
            let max_conns = self.config.max_connections as u64;
            tokio::spawn(async move {
                match TcpListener::bind(&metrics_addr).await {
                    Ok(l) => {
                        info!("Metrics endpoint listening on http://{}/metrics", metrics_addr);
                        loop {
                            let (mut sock, _) = match l.accept().await {
                                Ok(pair) => pair,
                                Err(_) => continue,
                            };
                            let active = max_conns.saturating_sub(m_sem.available_permits() as u64);
                            let body = crate::metrics::MetricsSnapshot::gather(&m_engine, &m_wal, active, max_conns)
                                .to_prometheus();
                            let mut scratch = [0u8; 1024];
                            let _ = tokio::time::timeout(
                                tokio::time::Duration::from_millis(200),
                                sock.read(&mut scratch),
                            ).await;
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                body.len(),
                                body
                            );
                            let _ = sock.write_all(resp.as_bytes()).await;
                            let _ = sock.shutdown().await;
                        }
                    }
                    Err(e) => error!("Failed to bind metrics port {}: {}", metrics_addr, e),
                }
            });
        }

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    let (stream, peer_addr) = accept_result?;
                    let _ = stream.set_nodelay(true);
                    info!("New connection from {}", peer_addr);

                    let permit = match self.semaphore.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            warn!("Max connections reached, rejecting {}", peer_addr);
                            drop(stream);
                            continue;
                        }
                    };

                    let query_engine = Arc::clone(&self.query_engine);
                    let kv_engine = Arc::clone(&self.kv_engine);
                    let doc_engine = Arc::clone(&self.doc_engine);
                    let credentials = Arc::clone(&self.credentials);
                    let session_manager = Arc::clone(&self.session_manager);

                    #[cfg(feature = "tls")]
                    let tls_for_conn = tls_acceptor.clone();

                    tokio::spawn(async move {
                        let _permit = permit;

                        #[cfg(feature = "tls")]
                        let result = match tls_for_conn {
                            Some(acceptor) => match acceptor.accept(stream).await {
                                Ok(tls_stream) => handle_connection(
                                    tls_stream,
                                    query_engine,
                                    kv_engine,
                                    doc_engine,
                                    credentials,
                                    session_manager,
                                    timeout_secs,
                                ).await,
                                Err(e) => {
                                    warn!("TLS handshake failed for {}: {}", peer_addr, e);
                                    Ok(())
                                }
                            },
                            None => handle_connection(
                                stream,
                                query_engine,
                                kv_engine,
                                doc_engine,
                                credentials,
                                session_manager,
                                timeout_secs,
                            ).await,
                        };

                        #[cfg(not(feature = "tls"))]
                        let result = handle_connection(
                            stream,
                            query_engine,
                            kv_engine,
                            doc_engine,
                            credentials,
                            session_manager,
                            timeout_secs,
                        ).await;

                        match result {
                            Ok(()) => {}
                            Err(e) => error!("Connection error from {}: {}", peer_addr, e),
                        }
                        info!("Connection closed: {}", peer_addr);
                    });
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Shutting down gracefully...");

                    {
                        let mut ws = self.workers.lock();
                        for w in ws.iter_mut() {
                            w.shutdown();
                        }
                    }

                    let _ = self.wal.flush();
                    if !self.config.no_shutdown_snapshot {
                        Self::create_snapshot(&self.query_engine, &self.snapshot_manager);
                        info!("Final snapshot saved. Goodbye.");
                    } else {
                        info!("Shutdown snapshot skipped (--no-shutdown-snapshot). Goodbye.");
                    }
                    break;
                }
            }
        }

        Ok(())
    }
}
