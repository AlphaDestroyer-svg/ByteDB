use std::collections::HashMap;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::cmp::Ordering;
use std::thread_local;

use bytedb_core::catalog::database::Database;
use bytedb_core::catalog::table::TableMeta;
use bytedb_core::index::btree::BPlusTree;
use bytedb_core::index::secondary::SecondaryIndex;
use bytedb_core::mvcc::transaction::{IsolationLevel, TransactionManager, TxnId};
use bytedb_core::mvcc::version_store::VersionStore;
use bytedb_core::tuple::schema::{Column, Schema};
use bytedb_core::tuple::tuple::{Tuple, raw_filter_matches, read_int64_at, read_value_at};
use bytedb_core::tuple::value::{DataType, Value};
use bytedb_core::wal::log_manager::LogManager;
use bytedb_core::wal::log_record::LogRecord;

use bytedb_core::stats::{compute_table_stats, TableStats, DEFAULT_HISTOGRAM_BUCKETS, DEFAULT_MCV_COUNT};
use bytedb_core::metrics::{LatencyHistogram, Timer};

use crate::error::{QueryError, Result};
use crate::executor::batch::{SelectionVector, deserialize_batch};
use crate::executor::context::QueryContext;
use crate::parser::ast::*;
use crate::planner::cost::StatsCatalog;
use crate::planner::logical_plan::build_logical_plan;
use crate::planner::optimizer::{optimize_with_catalog, IndexCatalog, IndexInfo};
use crate::planner::physical_plan::PhysicalPlan;
use rayon::prelude::*;
use bytedb_core::dbstore::{OP_DEL, OP_PUT};

const PARALLEL_SCAN_THRESHOLD: usize = 16384;
const LOG_COMPACT_THRESHOLD_BYTES: u64 = 8 * 1024 * 1024;

thread_local! {
    static CURRENT_TXN_ID: std::cell::Cell<Option<TxnId>> = std::cell::Cell::new(None);
    static PENDING_DELTAS: std::cell::RefCell<Vec<(String, u8, Vec<u8>, Vec<u8>)>> = std::cell::RefCell::new(Vec::new());
}

fn current_txn_id() -> Option<TxnId> {
    CURRENT_TXN_ID.with(|c| c.get())
}

const STMT_CACHE_SIZE: usize = 256;

struct StmtCache {
    entries: Vec<(u64, Statement)>,
}

impl StmtCache {
    fn new() -> Self {
        StmtCache {
            entries: Vec::with_capacity(STMT_CACHE_SIZE),
        }
    }

    fn get(&self, hash: u64) -> Option<&Statement> {
        self.entries.iter().find(|(h, _)| *h == hash).map(|(_, s)| s)
    }

    fn insert(&mut self, hash: u64, stmt: Statement) {
        if self.entries.len() >= STMT_CACHE_SIZE {
            self.entries.remove(0);
        }
        self.entries.push((hash, stmt));
    }
}

fn hash_sql(sql: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in sql.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[derive(Debug, Clone)]
pub enum ExecutionResult {
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Modified {
        count: u64,
    },
    Ok(String),
}

pub struct QueryEngine {
    database: Arc<Database>,
    txn_manager: Arc<TransactionManager>,
    tables: Arc<parking_lot::RwLock<HashMap<String, Arc<TableData>>>>,

    db_tables: Arc<parking_lot::RwLock<HashMap<String, HashMap<String, Arc<TableData>>>>>,
    wal: Option<Arc<LogManager>>,
    stmt_cache: parking_lot::Mutex<StmtCache>,

    databases: Arc<parking_lot::RwLock<std::collections::HashSet<String>>>,
    current_db: Arc<parking_lot::RwLock<String>>,
    disk_store: Option<Arc<crate::executor::diskstore::DiskStore>>,

    stats: Arc<parking_lot::RwLock<HashMap<String, TableStats>>>,

    active_ctx: parking_lot::RwLock<Option<Arc<QueryContext>>>,

    query_latency: Arc<LatencyHistogram>,
    slow_query_threshold_ms: parking_lot::RwLock<Option<u64>>,
    slow_query_log: parking_lot::RwLock<Vec<SlowQueryEntry>>,
    slow_query_log_capacity: usize,

    txn_undo: parking_lot::Mutex<HashMap<TxnId, HashMap<(String, Vec<u8>), Option<Vec<u8>>>>>,
    txn_deltas: parking_lot::Mutex<HashMap<TxnId, Vec<(String, u8, Vec<u8>, Vec<u8>)>>>,
    rowids: parking_lot::Mutex<HashMap<String, u64>>,
}

#[derive(Debug, Clone)]
pub struct SlowQueryEntry {
    pub sql: String,
    pub duration_micros: u64,
    pub txn_id: Option<TxnId>,
    pub timestamp_micros: u64,
}

pub struct TableData {
    pub schema: Schema,
    pub index: Arc<BPlusTree>,
    pub version_store: Arc<VersionStore>,
    pub check_exprs: Vec<Expr>,
    pub sequences: HashMap<String, Arc<bytedb_core::tuple::schema::SequenceGenerator>>,
    pub secondary_indexes: Vec<Arc<SecondaryIndex>>,
}

impl TableData {
    pub fn read_visible_entries(&self, txn_manager: &TransactionManager, txn_id: Option<TxnId>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if let Some(tid) = txn_id {
            if self.version_store.key_count() == 0 {
                return self.index.scan_all()
                    .map_err(|e| QueryError::Execution(e.to_string()));
            }
            let snapshot = txn_manager.get_snapshot(tid)
                .map_err(|e| QueryError::Execution(e.to_string()))?;
            let resolved = self.version_store.snapshot_resolved(tid, snapshot.start_ts, &snapshot.active_txns);
            let base = self.index.scan_all()
                .map_err(|e| QueryError::Execution(e.to_string()))?;
            let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(base.len());
            let mut seen_keys: std::collections::HashSet<&[u8], ahash::RandomState> = std::collections::HashSet::with_capacity_and_hasher(base.len(), ahash::RandomState::new());
            for (k, v) in &base {
                seen_keys.insert(k.as_slice());
                match resolved.get(k.as_slice()) {
                    None => out.push((k.clone(), v.clone())),
                    Some(None) => {}
                    Some(Some(t)) => out.push((k.clone(), t.serialize())),
                }
            }
            for (k, opt) in &resolved {
                if let Some(t) = opt {
                    if !seen_keys.contains(k.as_slice()) {
                        out.push((k.clone(), t.serialize()));
                    }
                }
            }
            Ok(out)
        } else {
            self.index.scan_all()
                .map_err(|e| QueryError::Execution(e.to_string()))
        }
    }

    pub fn read_visible_one(&self, txn_manager: &TransactionManager, txn_id: Option<TxnId>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(tid) = txn_id {
            let snapshot = txn_manager.get_snapshot(tid)
                .map_err(|e| QueryError::Execution(e.to_string()))?;
            match self.version_store.lookup_for_read(key, tid, snapshot.start_ts, &snapshot.active_txns) {
                bytedb_core::mvcc::version_store::ReadResult::Visible(t) => Ok(Some(t.serialize())),
                bytedb_core::mvcc::version_store::ReadResult::Tombstone => Ok(None),
                bytedb_core::mvcc::version_store::ReadResult::NoVersions => {
                    self.index.search(key).map_err(|e| QueryError::Execution(e.to_string()))
                }
            }
        } else {
            self.index.search(key)
                .map_err(|e| QueryError::Execution(e.to_string()))
        }
    }
}

impl QueryEngine {
    pub fn new(database: Arc<Database>, txn_manager: Arc<TransactionManager>) -> Self {
        let default_name = database.name().to_string();
        let mut dbs = std::collections::HashSet::new();
        dbs.insert(default_name.clone());
        QueryEngine {
            database,
            txn_manager,
            tables: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            db_tables: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            wal: None,
            stmt_cache: parking_lot::Mutex::new(StmtCache::new()),
            databases: Arc::new(parking_lot::RwLock::new(dbs)),
            current_db: Arc::new(parking_lot::RwLock::new(default_name)),
            disk_store: None,
            stats: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            active_ctx: parking_lot::RwLock::new(None),
            query_latency: Arc::new(LatencyHistogram::default()),
            slow_query_threshold_ms: parking_lot::RwLock::new(None),
            slow_query_log: parking_lot::RwLock::new(Vec::new()),
            slow_query_log_capacity: 256,
            txn_undo: parking_lot::Mutex::new(HashMap::new()),
            txn_deltas: parking_lot::Mutex::new(HashMap::new()),
            rowids: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    pub fn with_wal(database: Arc<Database>, txn_manager: Arc<TransactionManager>, wal: Arc<LogManager>) -> Self {
        let default_name = database.name().to_string();
        let mut dbs = std::collections::HashSet::new();
        dbs.insert(default_name.clone());
        QueryEngine {
            database,
            txn_manager,
            tables: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            db_tables: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            wal: Some(wal),
            stmt_cache: parking_lot::Mutex::new(StmtCache::new()),
            databases: Arc::new(parking_lot::RwLock::new(dbs)),
            current_db: Arc::new(parking_lot::RwLock::new(default_name)),
            disk_store: None,
            stats: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            active_ctx: parking_lot::RwLock::new(None),
            query_latency: Arc::new(LatencyHistogram::default()),
            slow_query_threshold_ms: parking_lot::RwLock::new(None),
            slow_query_log: parking_lot::RwLock::new(Vec::new()),
            slow_query_log_capacity: 256,
            txn_undo: parking_lot::Mutex::new(HashMap::new()),
            txn_deltas: parking_lot::Mutex::new(HashMap::new()),
            rowids: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    pub fn attach_disk_store(&mut self, store: Arc<crate::executor::diskstore::DiskStore>) {

        let mut dbs = self.databases.write();
        for n in store.registry().list() {
            dbs.insert(n);
        }
        drop(dbs);
        self.disk_store = Some(store);
        let _ = self.restore_current_db_from_disk();
    }

    pub fn disk_store(&self) -> Option<Arc<crate::executor::diskstore::DiskStore>> {
        self.disk_store.as_ref().map(Arc::clone)
    }

    fn restore_current_db_from_disk(&self) -> Result<()> {
        let Some(ds) = self.disk_store.as_ref() else { return Ok(()); };
        let mut tables = self.tables.write();
        tables.clear();
        for tcat in ds.list_tables() {
            let entries = ds.load_table_data(&tcat.name)
                .map_err(|e| QueryError::Execution(e.to_string()))?;
            let index = Arc::new(BPlusTree::new(format!("{}_pk", tcat.name), 128));
            for (k, v) in entries {
                let _ = index.insert(k, v);
            }
            let mut sequences: HashMap<String, Arc<bytedb_core::tuple::schema::SequenceGenerator>> = HashMap::new();
            for c in &tcat.schema.columns {
                if c.auto_increment {
                    let start = tcat.sequences.iter()
                        .find(|(n, _)| n == &c.name)
                        .map(|(_, v)| *v)
                        .unwrap_or(1);
                    sequences.insert(c.name.clone(), Arc::new(bytedb_core::tuple::schema::SequenceGenerator::new(start)));
                }
            }

            let secondary_indexes = Self::build_secondary_indexes(&tcat.schema, &index, &tcat.indexes)
                .map_err(|e| QueryError::Execution(e.to_string()))?;

            let meta = TableMeta::new(tcat.name.clone(), tcat.schema.clone(), tcat.table_id);
            let _ = self.database.create_table(meta);
            tables.insert(tcat.name.clone(), Arc::new(TableData {
                schema: tcat.schema,
                index,
                version_store: Arc::new(VersionStore::new()),
                check_exprs: Vec::new(),
                sequences,
                secondary_indexes,
            }));
        }
        Ok(())
    }

    fn build_secondary_indexes(
        schema: &Schema,
        pk_index: &Arc<BPlusTree>,
        defs: &[bytedb_core::dbstore::IndexDef],
    ) -> std::result::Result<Vec<Arc<SecondaryIndex>>, bytedb_core::error::CoreError> {
        if defs.is_empty() {
            return Ok(Vec::new());
        }
        let rows = pk_index.scan_all()?;
        let mut out = Vec::with_capacity(defs.len());
        for def in defs {
            let col_idxs: Vec<usize> = def
                .columns
                .iter()
                .filter_map(|c| schema.column_index(c))
                .collect();
            if col_idxs.len() != def.columns.len() {
                continue;
            }
            let sec = Arc::new(SecondaryIndex::new(def.name.clone(), col_idxs.clone(), def.unique));
            for (pk, data) in &rows {
                if let Some(values) = Tuple::deserialize_to_vec(data) {
                    let key_vals: Vec<&Value> = col_idxs.iter().map(|&i| &values[i]).collect();
                    sec.insert(&key_vals, pk)?;
                }
            }
            out.push(sec);
        }
        Ok(out)
    }

    fn switch_to_database(&self, name: &str) -> Result<()> {
        let current = self.current_db.read().clone();
        if current == name { return Ok(()); }

        {
            let mut tables = self.tables.write();
            let mut cache = self.db_tables.write();
            let snapshot = std::mem::take(&mut *tables);
            cache.insert(current.clone(), snapshot);
        }

        let cached = self.db_tables.write().remove(name);
        if let Some(map) = cached {
            *self.tables.write() = map;
        } else if let Some(ds) = &self.disk_store {
            ds.switch_database(name)
                .map_err(|e| QueryError::Execution(e.to_string()))?;
            *self.current_db.write() = name.to_string();
            self.restore_current_db_from_disk()?;
            return Ok(());
        }
        *self.current_db.write() = name.to_string();
        if let Some(ds) = &self.disk_store {
            let _ = ds.switch_database(name);
        }
        Ok(())
    }

    fn flush_table_to_disk(&self, table: &str) {
        let Some(ds) = self.disk_store.as_ref() else { return; };
        let tables = self.tables.read();
        let Some(td) = tables.get(table) else { return; };
        if let Ok(entries) = td.index.scan_all() {
            let _ = ds.compact_table(table, &entries);
        }
        self.flush_sequences_for(table, &td, ds);
    }

    fn flush_sequences_for(&self, table: &str, td: &TableData, ds: &crate::executor::diskstore::DiskStore) {
        let seqs: Vec<(String, i64)> = td.sequences.iter()
            .map(|(c, s)| (c.clone(), s.counter.load(std::sync::atomic::Ordering::SeqCst)))
            .collect();
        if !seqs.is_empty() {
            let _ = ds.flush_table_sequences(table, seqs);
        }
    }

    fn log_put(&self, table: &str, key: &[u8], data: &[u8]) {
        if self.disk_store.is_some() {
            PENDING_DELTAS.with(|p| p.borrow_mut().push((table.to_string(), OP_PUT, key.to_vec(), data.to_vec())));
        }
    }

    fn log_del(&self, table: &str, key: &[u8]) {
        if self.disk_store.is_some() {
            PENDING_DELTAS.with(|p| p.borrow_mut().push((table.to_string(), OP_DEL, key.to_vec(), Vec::new())));
        }
    }

    fn clear_pending_deltas(&self) {
        PENDING_DELTAS.with(|p| p.borrow_mut().clear());
    }

    fn stash_or_persist_deltas(&self, txn_id: Option<TxnId>) {
        match txn_id {
            Some(tid) => {
                let deltas = PENDING_DELTAS.with(|p| std::mem::take(&mut *p.borrow_mut()));
                if !deltas.is_empty() {
                    self.txn_deltas.lock().entry(tid).or_default().extend(deltas);
                }
            }
            None => self.persist_pending_deltas(),
        }
    }

    fn commit_txn_deltas(&self, txn_id: TxnId) {
        let deltas = self.txn_deltas.lock().remove(&txn_id).unwrap_or_default();
        self.write_deltas_to_disk(deltas);
    }

    fn discard_txn_deltas(&self, txn_id: TxnId) {
        self.txn_deltas.lock().remove(&txn_id);
    }

    fn persist_pending_deltas(&self) {
        let deltas = PENDING_DELTAS.with(|p| std::mem::take(&mut *p.borrow_mut()));
        self.write_deltas_to_disk(deltas);
    }

    fn write_deltas_to_disk(&self, deltas: Vec<(String, u8, Vec<u8>, Vec<u8>)>) {
        let Some(ds) = self.disk_store.as_ref() else {
            return;
        };
        if deltas.is_empty() {
            return;
        }
        let mut by_table: HashMap<String, Vec<(u8, Vec<u8>, Vec<u8>)>> = HashMap::new();
        for (t, op, k, v) in deltas {
            by_table.entry(t).or_default().push((op, k, v));
        }
        for (table, table_deltas) in by_table {
            let _ = ds.append_table_log(&table, &table_deltas);
            let td = self.tables.read().get(&table).cloned();
            if let Some(td) = td {
                self.flush_sequences_for(&table, &td, ds);
                if ds.table_log_bytes(&table) >= LOG_COMPACT_THRESHOLD_BYTES {
                    if let Ok(entries) = td.index.scan_all() {
                        let _ = ds.compact_table(&table, &entries);
                    }
                }
            }
        }
    }

    pub fn execute_sql(&self, sql: &str, txn_id: Option<TxnId>) -> Result<ExecutionResult> {
        let timer = Timer::start();
        let hash = hash_sql(sql);
        let cached = {
            let cache = self.stmt_cache.lock();
            cache.get(hash).cloned()
        };
        let stmt = if let Some(s) = cached {
            s
        } else {
            let mut parser = crate::parser::parser::Parser::new(sql)
                .map_err(|e| QueryError::Parse(e.to_string()))?;
            let s = parser.parse()?;
            let mut cache = self.stmt_cache.lock();
            cache.insert(hash, s.clone());
            s
        };
        let res = self.execute(stmt, txn_id);
        let elapsed_us = timer.elapsed_micros();
        self.query_latency.record_micros(elapsed_us);
        self.maybe_record_slow_query(sql, elapsed_us, txn_id);
        res
    }

    pub fn query_latency(&self) -> Arc<LatencyHistogram> {
        Arc::clone(&self.query_latency)
    }

    pub fn set_slow_query_threshold_ms(&self, threshold_ms: Option<u64>) {
        *self.slow_query_threshold_ms.write() = threshold_ms;
    }

    pub fn slow_query_log(&self) -> Vec<SlowQueryEntry> {
        self.slow_query_log.read().clone()
    }

    pub fn clear_slow_query_log(&self) {
        self.slow_query_log.write().clear();
    }

    fn maybe_record_slow_query(&self, sql: &str, elapsed_us: u64, txn_id: Option<TxnId>) {
        let threshold = match *self.slow_query_threshold_ms.read() {
            Some(t) => t,
            None => return,
        };
        if elapsed_us / 1000 < threshold {
            return;
        }
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        let entry = SlowQueryEntry {
            sql: sql.to_string(),
            duration_micros: elapsed_us,
            txn_id,
            timestamp_micros: now_us,
        };
        let mut log = self.slow_query_log.write();
        if log.len() >= self.slow_query_log_capacity {
            log.remove(0);
        }
        log.push(entry);
    }

    pub fn execute_sql_with_ctx(
        &self,
        sql: &str,
        txn_id: Option<TxnId>,
        ctx: Arc<QueryContext>,
    ) -> Result<ExecutionResult> {
        self.set_active_ctx(Some(ctx.clone()));
        let res = self.execute_sql(sql, txn_id);
        self.set_active_ctx(None);
        if let Err(_) = ctx.check() {
            return ctx.check().map(|_| unreachable!());
        }
        res
    }

    pub fn execute_with_ctx(
        &self,
        stmt: Statement,
        txn_id: Option<TxnId>,
        ctx: Arc<QueryContext>,
    ) -> Result<ExecutionResult> {
        self.set_active_ctx(Some(ctx.clone()));
        let res = self.execute(stmt, txn_id);
        self.set_active_ctx(None);
        if let Err(_) = ctx.check() {
            return ctx.check().map(|_| unreachable!());
        }
        res
    }

    fn set_active_ctx(&self, ctx: Option<Arc<QueryContext>>) {
        *self.active_ctx.write() = ctx;
    }

    pub fn active_ctx(&self) -> Option<Arc<QueryContext>> {
        self.active_ctx.read().as_ref().map(Arc::clone)
    }

    fn poll_ctx(&self) -> Result<()> {
        if let Some(ctx) = self.active_ctx.read().as_ref() {
            ctx.check()?;
        }
        Ok(())
    }

    fn account_scan_row(&self) -> Result<()> {
        if let Some(ctx) = self.active_ctx.read().as_ref() {
            ctx.account_scan_row()?;
            let usage = ctx.usage();
            if usage.scan_rows % 1024 == 0 {
                ctx.check()?;
            }
        }
        Ok(())
    }

    fn account_memory(&self, bytes: u64) -> Result<()> {
        if let Some(ctx) = self.active_ctx.read().as_ref() {
            ctx.account_memory(bytes)?;
        }
        Ok(())
    }

    fn parallel_filter_leaves(
        &self,
        leaves: Vec<Vec<(Vec<u8>, Vec<u8>)>>,
        schema: &Schema,
        filter: Option<&Expr>,
        needed_columns: Option<&[usize]>,
        fast_filter: Option<(usize, BinOp, Value)>,
    ) -> Result<Vec<Vec<Value>>> {
        let ctx = self.active_ctx.read().as_ref().map(Arc::clone);
        let fast_op: Option<(usize, u8, Value)> = fast_filter.map(|(ci, op, v)| {
            let code = match op {
                BinOp::Eq => 0, BinOp::Neq => 1, BinOp::Lt => 2,
                BinOp::Gt => 3, BinOp::Lte => 4, BinOp::Gte => 5, _ => 255,
            };
            (ci, code, v)
        });

        let total: usize = leaves.iter().map(|l| l.len()).sum();
        if let Some(c) = &ctx {
            c.account_scan_rows(total as u64)?;
        }

        let per_leaf: Vec<Result<Vec<Vec<Value>>>> = leaves
            .par_iter()
            .map(|leaf| {
                if let Some(c) = &ctx {
                    c.check()?;
                }
                let mut out = Vec::new();
                for (_k, data) in leaf {
                    if let Some((ci, code, lit)) = &fast_op {
                        match raw_filter_matches(data, *ci, *code, lit) {
                            Some(true) => {
                                if let Some(t) = Tuple::deserialize(data) {
                                    out.push(t.into_vec());
                                }
                                continue;
                            }
                            Some(false) => continue,
                            None => {}
                        }
                    }
                    let tuple = if let Some(nc) = needed_columns {
                        Tuple::deserialize_columns(data, nc)
                    } else {
                        Tuple::deserialize(data)
                    };
                    if let Some(tuple) = tuple {
                        let matches = match filter {
                            Some(f) => self.eval_predicate(f, &tuple, schema),
                            None => true,
                        };
                        if matches {
                            out.push(tuple.into_vec());
                        }
                    }
                }
                Ok(out)
            })
            .collect();

        let mut rows = Vec::new();
        for part in per_leaf {
            rows.extend(part?);
        }
        Ok(rows)
    }

    #[allow(dead_code)]
    fn account_temp_spill(&self, bytes: u64) -> Result<()> {
        if let Some(ctx) = self.active_ctx.read().as_ref() {
            ctx.account_temp_spill(bytes)?;
        }
        Ok(())
    }

    pub fn wal_handle(&self) -> Option<Arc<LogManager>> {
        self.wal.as_ref().map(Arc::clone)
    }

    fn wal_append(&self, record: LogRecord) {
        if let Some(ref wal) = self.wal {
            let _ = wal.append(record);
        }
    }

    fn wal_flush(&self) {
        if let Some(ref wal) = self.wal {
            let _ = wal.flush();
        }
    }

    pub fn execute(&self, stmt: Statement, txn_id: Option<TxnId>) -> Result<ExecutionResult> {
        struct TxnGuard;
        impl Drop for TxnGuard {
            fn drop(&mut self) {
                CURRENT_TXN_ID.with(|c| c.set(None));
            }
        }
        CURRENT_TXN_ID.with(|c| c.set(txn_id));
        let _guard = TxnGuard;
        match stmt {
            Statement::Begin(isolation) => {
                let iso = isolation.unwrap_or(IsolationLevel::ReadCommitted);
                let id = self.txn_manager.begin(iso);
                self.wal_append(LogRecord::Begin { txn_id: id });
                Ok(ExecutionResult::Ok(format!("Transaction {} started", id)))
            }
            Statement::Commit => {
                if let Some(txn_id) = txn_id {
                    match self.txn_manager.commit(txn_id) {
                        Ok(_) => {
                            self.clear_txn_undo(txn_id);
                            self.commit_txn_deltas(txn_id);
                            self.wal_append(LogRecord::Commit { txn_id });
                            self.wal_flush();
                            Ok(ExecutionResult::Ok("COMMIT".into()))
                        }
                        Err(e) => {
                            self.rollback_txn_effects(txn_id);
                            self.wal_append(LogRecord::Abort { txn_id });
                            self.wal_flush();
                            Err(QueryError::Execution(e.to_string()))
                        }
                    }
                } else {
                    Err(QueryError::Execution("No active transaction".into()))
                }
            }
            Statement::Rollback => {
                if let Some(txn_id) = txn_id {
                    self.rollback_txn_effects(txn_id);
                    self.wal_append(LogRecord::Abort { txn_id });
                    self.wal_flush();
                    self.txn_manager.abort(txn_id)
                        .map_err(|e| QueryError::Execution(e.to_string()))?;
                    Ok(ExecutionResult::Ok("ROLLBACK".into()))
                } else {
                    Err(QueryError::Execution("No active transaction".into()))
                }
            }
            Statement::ShowTables => {
                let tables = self.database.list_tables();
                let rows: Vec<Vec<Value>> = tables.into_iter()
                    .map(|t| vec![Value::Text(t)])
                    .collect();
                Ok(ExecutionResult::Rows {
                    columns: vec!["table_name".into()],
                    rows,
                })
            }
            Statement::Describe(name) | Statement::ShowColumns(name) => {
                let tables = self.tables.read();
                let table_data = tables.get(&name)
                    .ok_or_else(|| QueryError::Execution(format!("Table '{}' not found", name)))?;
                let rows: Vec<Vec<Value>> = table_data.schema.columns.iter().map(|c| {
                    vec![
                        Value::Text(c.name.clone()),
                        Value::Text(format!("{:?}", c.data_type)),
                        Value::Bool(c.nullable),
                        Value::Bool(c.primary_key),
                    ]
                }).collect();
                Ok(ExecutionResult::Rows {
                    columns: vec!["name".into(), "type".into(), "nullable".into(), "primary_key".into()],
                    rows,
                })
            }
            Statement::ShowCreateTable(name) => {
                let tables = self.tables.read();
                let table_data = tables.get(&name)
                    .ok_or_else(|| QueryError::Execution(format!("Table '{}' not found", name)))?;
                let mut ddl = format!("CREATE TABLE {} (\n", name);
                for (i, col) in table_data.schema.columns.iter().enumerate() {
                    if i > 0 { ddl.push_str(",\n"); }
                    ddl.push_str(&format!("  {} {:?}", col.name, col.data_type));
                    if col.primary_key { ddl.push_str(" PRIMARY KEY"); }
                    if !col.nullable { ddl.push_str(" NOT NULL"); }
                }
                ddl.push_str("\n)");
                Ok(ExecutionResult::Rows {
                    columns: vec!["Create Table".into()],
                    rows: vec![vec![Value::Text(ddl)]],
                })
            }
            Statement::Truncate(name) => {
                {
                    let tables = self.tables.read();
                    let table_data = tables.get(&name)
                        .ok_or_else(|| QueryError::Execution(format!("Table '{}' not found", name)))?;
                    let keys: Vec<Vec<u8>> = table_data.index.scan_all()
                        .map_err(|e| QueryError::Execution(e.to_string()))?
                        .into_iter().map(|(k, _)| k).collect();
                    for key in keys {
                        table_data.index.delete(&key)
                            .map_err(|e| QueryError::Execution(e.to_string()))?;
                    }
                }
                let td = self.tables.read().get(&name).cloned();
                if let Some(td) = td {
                    if !td.secondary_indexes.is_empty() {
                        let fresh: Vec<Arc<SecondaryIndex>> = td.secondary_indexes.iter()
                            .map(|s| Arc::new(SecondaryIndex::new(s.name.clone(), s.columns.clone(), s.unique)))
                            .collect();
                        let new_td = TableData {
                            schema: td.schema.clone(),
                            index: Arc::clone(&td.index),
                            version_store: Arc::clone(&td.version_store),
                            check_exprs: td.check_exprs.clone(),
                            sequences: td.sequences.clone(),
                            secondary_indexes: fresh,
                        };
                        self.tables.write().insert(name.clone(), Arc::new(new_td));
                    }
                }
                self.flush_table_to_disk(&name);
                Ok(ExecutionResult::Ok(format!("TRUNCATE TABLE")))
            }
            Statement::KvGet(_) | Statement::KvSet(_, _) | Statement::KvDelete(_) | Statement::KvScan(_, _) => {
                Err(QueryError::Execution("Use KV engine for KV operations".into()))
            }
            Statement::DocInsert(_) | Statement::DocFind(_) | Statement::DocUpdate(_) | Statement::DocDelete(_) => {
                Err(QueryError::Execution("Use Document engine for DOC operations".into()))
            }
            Statement::AlterTable(alter) => self.exec_alter_table(alter),
            Statement::CreateDatabase { name, if_not_exists } => {
                let mut dbs = self.databases.write();
                if dbs.contains(&name) {
                    if if_not_exists {
                        return Ok(ExecutionResult::Ok(format!("Database {} already exists", name)));
                    }
                    return Err(QueryError::Execution(format!("Database '{}' already exists", name)));
                }
                if let Some(ds) = &self.disk_store {
                    ds.create_database(&name)
                        .map_err(|e| QueryError::Execution(e.to_string()))?;
                }
                dbs.insert(name.clone());
                Ok(ExecutionResult::Ok(format!("Database {} created", name)))
            }
            Statement::DropDatabase { name, if_exists } => {
                let current = self.current_db.read().clone();
                if name == current {
                    return Err(QueryError::Execution("Cannot drop the current database; USE another first".into()));
                }
                let mut dbs = self.databases.write();
                if !dbs.contains(&name) {
                    if if_exists {
                        return Ok(ExecutionResult::Ok("OK".into()));
                    }
                    return Err(QueryError::Execution(format!("Database '{}' not found", name)));
                }
                if let Some(ds) = &self.disk_store {
                    ds.drop_database(&name)
                        .map_err(|e| QueryError::Execution(e.to_string()))?;
                }
                dbs.remove(&name);
                self.db_tables.write().remove(&name);
                Ok(ExecutionResult::Ok(format!("Database {} dropped", name)))
            }
            Statement::UseDatabase(name) => {
                let dbs = self.databases.read();
                if !dbs.contains(&name) {
                    return Err(QueryError::Execution(format!("Database '{}' not found", name)));
                }
                drop(dbs);
                self.switch_to_database(&name)?;
                Ok(ExecutionResult::Ok(format!("Now using database {}", name)))
            }
            Statement::ShowDatabases => {
                let dbs = self.databases.read();
                let mut names: Vec<String> = dbs.iter().cloned().collect();
                names.sort();
                let rows: Vec<Vec<Value>> = names.into_iter().map(|n| vec![Value::Text(n)]).collect();
                Ok(ExecutionResult::Rows { columns: vec!["database".into()], rows })
            }
            Statement::Analyze(table) => self.exec_analyze(table.as_deref()),
            Statement::Backup { path } => self.exec_backup(&path),
            Statement::Restore { path, to_lsn } => self.exec_restore(&path, to_lsn),
            Statement::Migrate => self.exec_migrate(),
            Statement::ShowStats(table) => self.exec_show_stats(table.as_deref()),
            Statement::Union(left, right, all) => self.exec_union(*left, *right, all, txn_id),
            Statement::Intersect(left, right, all) => self.exec_intersect(*left, *right, all, txn_id),
            Statement::Except(left, right, all) => self.exec_except(*left, *right, all, txn_id),
            Statement::Explain(inner, analyze) => {
                use crate::planner::cost::cost_plan;
                let logical = build_logical_plan(&inner)?;
                let stats_snapshot = self.stats_snapshot();
                let physical = optimize_with_catalog(logical.clone(), &stats_snapshot, &self.index_snapshot())?;
                let pc = cost_plan(&physical, &stats_snapshot);
                let estimated_rows = pc.rows.round() as u64;
                let estimated_cost = pc.total_cost;
                let plan_text = format!("{:#?}", physical);
                let mut lines: Vec<String> = Vec::new();
                lines.push(format!(
                    "Plan  (estimated_rows={}  estimated_cost={:.2})",
                    estimated_rows, estimated_cost
                ));
                if analyze {
                    let timer = Timer::start();
                    let result = self.execute(*inner, txn_id)?;
                    let elapsed_us = timer.elapsed_micros();
                    let actual_rows = match &result {
                        ExecutionResult::Rows { rows, .. } => rows.len() as u64,
                        ExecutionResult::Modified { count } => *count,
                        ExecutionResult::Ok(_) => 0,
                    };
                    let factor = if estimated_rows > 0 {
                        actual_rows as f64 / estimated_rows as f64
                    } else if actual_rows == 0 {
                        1.0
                    } else {
                        f64::INFINITY
                    };
                    lines.push(format!(
                        "Actual  rows={}  time={:.3} ms  est/actual_factor={:.2}",
                        actual_rows,
                        elapsed_us as f64 / 1000.0,
                        factor
                    ));
                }
                for l in plan_text.lines() {
                    lines.push(l.to_string());
                }
                Ok(ExecutionResult::Rows {
                    columns: vec!["QUERY PLAN".into()],
                    rows: lines.into_iter().map(|l| vec![Value::Text(l)]).collect(),
                })
            }
            Statement::Insert(ins) => {
                let r = self.exec_insert(&ins.table, ins.columns, ins.source, ins.on_conflict, ins.returning, txn_id);
                if r.is_ok() { self.stash_or_persist_deltas(txn_id); } else { self.clear_pending_deltas(); }
                r
            }
            Statement::Update(upd) => {
                let r = self.exec_update(&upd.table, upd.assignments, upd.where_clause, upd.returning, txn_id);
                if r.is_ok() { self.stash_or_persist_deltas(txn_id); } else { self.clear_pending_deltas(); }
                r
            }
            Statement::Delete(del) => {
                let r = self.exec_delete(&del.table, del.where_clause, del.returning, txn_id);
                if r.is_ok() { self.stash_or_persist_deltas(txn_id); } else { self.clear_pending_deltas(); }
                r
            }
            Statement::Select(ref select) if !select.ctes.is_empty() || matches!(select.from, FromClause::Subquery(_)) => {
                let ctes = select.ctes.clone();
                let mut cte_names = Vec::new();
                for cte in &ctes {
                    let cte_result = self.execute(Statement::Select(cte.query.clone()), txn_id)?;
                    if let ExecutionResult::Rows { columns, rows } = cte_result {
                        self.materialize_cte(&cte.name, &columns, rows)?;
                        cte_names.push(cte.name.clone());
                    }
                }
                if let FromClause::Subquery(ref subquery) = select.from {
                    let sub_result = self.execute(Statement::Select(*subquery.clone()), txn_id)?;
                    if let ExecutionResult::Rows { columns, rows } = sub_result {
                        let alias = select.from_alias.clone().unwrap_or_else(|| "__subquery__".to_string());
                        self.materialize_cte(&alias, &columns, rows)?;
                        cte_names.push(alias);
                    }
                }
                let mut main_select = select.clone();
                main_select.ctes = Vec::new();
                if matches!(main_select.from, FromClause::Subquery(_)) {
                    let alias = main_select.from_alias.clone().unwrap_or_else(|| "__subquery__".to_string());
                    main_select.from = FromClause::Table(alias);
                }
                let result = self.execute(Statement::Select(main_select), txn_id);
                for name in &cte_names {
                    self.tables.write().remove(name);
                }
                result
            }
            _ => {
                let stmt = Self::rewrite_aliases(stmt);
                let logical = build_logical_plan(&stmt)?;
                let stats_snapshot = self.stats_snapshot();
                let physical = optimize_with_catalog(logical, &stats_snapshot, &self.index_snapshot())?;
                self.execute_physical(physical, txn_id)
            }
        }
    }

    fn execute_physical(&self, plan: PhysicalPlan, txn_id: Option<TxnId>) -> Result<ExecutionResult> {
        match plan {
            PhysicalPlan::CreateTable(ct) => self.exec_create_table(ct),
            PhysicalPlan::DropTable(dt) => self.exec_drop_table(dt),
            PhysicalPlan::CreateIndex(ci) => self.exec_create_index(ci),
            PhysicalPlan::DropIndex(name) => self.exec_drop_index(&name),
            PhysicalPlan::Insert { table, columns, source } => {
                self.exec_insert(&table, columns, source, None, None, txn_id)
            }
            PhysicalPlan::Update { table, assignments, filter } => {
                self.exec_update(&table, assignments, filter, None, txn_id)
            }
            PhysicalPlan::Delete { table, filter } => {
                self.exec_delete(&table, filter, None, txn_id)
            }
            PhysicalPlan::Project { input, columns } => {
                let result = self.execute_physical(*input, txn_id)?;
                self.apply_projection(result, &columns)
            }
            PhysicalPlan::SeqScan { table, filter, limit, needed_columns } => {
                self.exec_seq_scan(&table, filter.as_ref(), txn_id, limit, needed_columns.as_deref())
            }
            PhysicalPlan::Filter { input, predicate } => {
                let result = self.execute_physical(*input, txn_id)?;
                self.apply_filter(result, &predicate)
            }
            PhysicalPlan::Sort { input, order_by } => {
                let result = self.execute_physical(*input, txn_id)?;
                self.apply_sort(result, &order_by)
            }
            PhysicalPlan::Limit { input, count, offset } => {
                match *input {
                    PhysicalPlan::Sort { input: sort_input, order_by } if offset == 0 => {
                        match *sort_input {
                            PhysicalPlan::SeqScan { ref table, ref filter, .. } if order_by.len() == 1 => {
                                let table_name = table.clone();
                                let filter_clone = filter.clone();
                                match self.exec_scan_top_n(&table_name, filter_clone.as_ref(), txn_id, &order_by, count) {
                                    Ok(r) => Ok(r),
                                    Err(_) => {
                                        let result = self.execute_physical(*sort_input, txn_id)?;
                                        self.apply_top_n(result, &order_by, count)
                                    }
                                }
                            }
                            other => {
                                let result = self.execute_physical(other, txn_id)?;
                                self.apply_top_n(result, &order_by, count)
                            }
                        }
                    }
                    other => {
                        let result = self.execute_physical(other, txn_id)?;
                        self.apply_limit(result, count, offset)
                    }
                }
            }
            PhysicalPlan::HashJoin { left, right, condition, join_type } => {
                self.exec_hash_join(*left, *right, condition, join_type, txn_id)
            }
            PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
                self.exec_hash_join(*left, *right, condition, join_type, txn_id)
            }
            PhysicalPlan::HashAggregate { input, group_by, aggregates, having } => {
                if !group_by.is_empty() {
                    if let PhysicalPlan::SeqScan { ref table, ref filter, .. } = *input {
                        if filter.is_none() && txn_id.is_none() && having.is_none() {
                            let tables = self.tables.read();
                            if let Some(td) = tables.get(table) {
                                let col_names: Vec<String> = td.schema.columns.iter().map(|c| c.name.clone()).collect();
                                let result = self.try_fast_aggregate(td, &col_names, &group_by, &aggregates);
                                if let Some(r) = result {
                                    return Ok(r);
                                }
                            }
                        }
                    }
                }
                self.exec_hash_aggregate(*input, group_by, aggregates, having, txn_id)
            }
            PhysicalPlan::IndexScan { table, index_name, column, op, value, filter, limit } => {
                self.exec_index_scan(&table, &index_name, &column, op, &value, filter.as_ref(), limit, txn_id)
            }
            PhysicalPlan::Distinct { input } => {
                let result = self.execute_physical(*input, txn_id)?;
                self.apply_distinct(result)
            }
        }
    }

    fn exec_create_table(&self, ct: CreateTableStmt) -> Result<ExecutionResult> {
        let columns: Vec<Column> = ct.columns.iter().map(|c| {
            let mut col = Column::new(c.name.clone(), c.data_type);
            if !c.nullable {
                col = col.not_null();
            }
            if c.primary_key {
                col = col.primary_key();
            }
            if c.unique {
                col = col.unique();
            }
            if c.auto_increment {
                col = col.auto_increment();
            }
            if let Some(ref default_expr) = c.default {
                let default_val = self.eval_expr_simple(default_expr);
                col = col.with_default(default_val);
            }
            if let Some(len) = c.max_length {
                col = col.with_max_length(len);
            }
            col
        }).collect();

        let mut schema = Schema::new(ct.name.clone(), columns);
        schema.check_constraints = ct.check_constraints.iter().map(|e| format!("{:?}", e)).collect();
        schema.foreign_keys = ct.foreign_keys.iter().map(|fk| {
            bytedb_core::tuple::schema::ForeignKey {
                columns: fk.columns.clone(),
                ref_table: fk.ref_table.clone(),
                ref_columns: fk.ref_columns.clone(),
                on_delete: ast_fk_action_to_core(fk.on_delete),
                on_update: ast_fk_action_to_core(fk.on_update),
            }
        }).collect();

        for c in &ct.columns {
            if let Some((rt, rc)) = &c.references {
                schema.foreign_keys.push(bytedb_core::tuple::schema::ForeignKey {
                    columns: vec![c.name.clone()],
                    ref_table: rt.clone(),
                    ref_columns: vec![rc.clone()],
                    on_delete: ast_fk_action_to_core(c.on_delete),
                    on_update: ast_fk_action_to_core(c.on_update),
                });
            }
        }

        let mut sequences: HashMap<String, Arc<bytedb_core::tuple::schema::SequenceGenerator>> = HashMap::new();
        for c in &schema.columns {
            if c.auto_increment {
                sequences.insert(c.name.clone(), Arc::new(bytedb_core::tuple::schema::SequenceGenerator::new(1)));
            }
        }

        let table_id = self.tables.read().len() as u32 + 1;
        let meta = TableMeta::new(ct.name.clone(), schema.clone(), table_id);

        self.database.create_table(meta)
            .map_err(|e| QueryError::Execution(e.to_string()))?;

        let table_data = TableData {
            schema: schema.clone(),
            index: Arc::new(BPlusTree::new(format!("{}_pk", ct.name), 128)),
            version_store: Arc::new(VersionStore::new()),
            check_exprs: ct.check_constraints.clone(),
            sequences: sequences.clone(),
            secondary_indexes: Vec::new(),
        };
        self.tables.write().insert(ct.name.clone(), Arc::new(table_data));

        if let Some(ds) = &self.disk_store {
            let seqs: Vec<(String, i64)> = sequences.iter()
                .map(|(c, s)| (c.clone(), s.counter.load(std::sync::atomic::Ordering::SeqCst)))
                .collect();
            let _ = ds.upsert_table(&ct.name, table_id, &schema, seqs);
            let _ = ds.flush_table_data(&ct.name, &[]);
        }

        Ok(ExecutionResult::Ok(format!("Table {} created", ct.name)))
    }

    fn exec_drop_table(&self, dt: DropTableStmt) -> Result<ExecutionResult> {
        match self.database.drop_table(&dt.name) {
            Ok(_) => {
                self.tables.write().remove(&dt.name);
                self.stats.write().remove(&dt.name);
                self.rowids.lock().remove(&dt.name);
                if let Some(ds) = &self.disk_store {
                    let _ = ds.drop_table(&dt.name);
                }
                Ok(ExecutionResult::Ok(format!("Table {} dropped", dt.name)))
            }
            Err(e) => {
                if dt.if_exists {
                    Ok(ExecutionResult::Ok("OK".into()))
                } else {
                    Err(QueryError::Execution(e.to_string()))
                }
            }
        }
    }

    fn exec_create_index(&self, ci: CreateIndexStmt) -> Result<ExecutionResult> {
        let td = {
            let tables = self.tables.read();
            Arc::clone(tables.get(&ci.table)
                .ok_or_else(|| QueryError::Execution(format!("Table '{}' not found", ci.table)))?)
        };

        if td.secondary_indexes.iter().any(|s| s.name == ci.name) {
            return Err(QueryError::Execution(format!("Index '{}' already exists", ci.name)));
        }

        let col_idxs: Vec<usize> = ci.columns.iter()
            .map(|c| td.schema.column_index(c))
            .collect::<Option<Vec<usize>>>()
            .ok_or_else(|| QueryError::Execution("indexed column not found".into()))?;

        let sec = Arc::new(SecondaryIndex::new(ci.name.clone(), col_idxs.clone(), ci.unique));
        let rows = td.index.scan_all().map_err(|e| QueryError::Execution(e.to_string()))?;
        for (pk, data) in &rows {
            if let Some(values) = Tuple::deserialize_to_vec(data) {
                let key_vals: Vec<&Value> = col_idxs.iter().filter_map(|&i| values.get(i)).collect();
                if key_vals.len() == col_idxs.len() {
                    sec.insert(&key_vals, pk).map_err(|e| QueryError::Execution(e.to_string()))?;
                }
            }
        }

        let mut new_secs = td.secondary_indexes.clone();
        new_secs.push(sec);
        let new_td = TableData {
            schema: td.schema.clone(),
            index: Arc::clone(&td.index),
            version_store: Arc::clone(&td.version_store),
            check_exprs: td.check_exprs.clone(),
            sequences: td.sequences.clone(),
            secondary_indexes: new_secs.clone(),
        };
        self.tables.write().insert(ci.table.clone(), Arc::new(new_td));

        if let Some(ds) = &self.disk_store {
            let defs = Self::index_defs_for(&td.schema, &new_secs);
            let _ = ds.upsert_table_indexes(&ci.table, defs);
        }

        Ok(ExecutionResult::Ok(format!("Index {} created on {}", ci.name, ci.table)))
    }

    fn exec_drop_index(&self, name: &str) -> Result<ExecutionResult> {
        let target = {
            let tables = self.tables.read();
            tables.iter()
                .find(|(_, td)| td.secondary_indexes.iter().any(|s| s.name == name))
                .map(|(t, _)| t.clone())
        };
        let Some(tname) = target else {
            return Ok(ExecutionResult::Ok(format!("Index {} not found", name)));
        };
        let td = {
            let tables = self.tables.read();
            Arc::clone(tables.get(&tname).unwrap())
        };
        let new_secs: Vec<Arc<SecondaryIndex>> = td.secondary_indexes.iter()
            .filter(|s| s.name != name)
            .cloned()
            .collect();
        let new_td = TableData {
            schema: td.schema.clone(),
            index: Arc::clone(&td.index),
            version_store: Arc::clone(&td.version_store),
            check_exprs: td.check_exprs.clone(),
            sequences: td.sequences.clone(),
            secondary_indexes: new_secs.clone(),
        };
        self.tables.write().insert(tname.clone(), Arc::new(new_td));

        if let Some(ds) = &self.disk_store {
            let defs = Self::index_defs_for(&td.schema, &new_secs);
            let _ = ds.upsert_table_indexes(&tname, defs);
        }

        Ok(ExecutionResult::Ok(format!("Index {} dropped", name)))
    }

    fn index_defs_for(schema: &Schema, secs: &[Arc<SecondaryIndex>]) -> Vec<bytedb_core::dbstore::IndexDef> {
        secs.iter().map(|s| bytedb_core::dbstore::IndexDef {
            name: s.name.clone(),
            columns: s.columns.iter()
                .filter_map(|&i| schema.columns.get(i).map(|c| c.name.clone()))
                .collect(),
            unique: s.unique,
        }).collect()
    }

    fn update_secondary_indexes_insert(td: &TableData, values: &[Value], pk: &[u8]) -> Result<()> {
        for sec in &td.secondary_indexes {
            let key_vals: Vec<&Value> = sec.columns.iter().filter_map(|&i| values.get(i)).collect();
            if key_vals.len() == sec.columns.len() {
                sec.insert(&key_vals, pk).map_err(|e| QueryError::Execution(e.to_string()))?;
            }
        }
        Ok(())
    }

    fn update_secondary_indexes_delete(td: &TableData, values: &[Value], pk: &[u8]) {
        for sec in &td.secondary_indexes {
            let key_vals: Vec<&Value> = sec.columns.iter().filter_map(|&i| values.get(i)).collect();
            if key_vals.len() == sec.columns.len() {
                let _ = sec.remove(&key_vals, pk);
            }
        }
    }

    fn update_secondary_indexes_update(td: &TableData, old_values: &[Value], new_values: &[Value], pk: &[u8]) -> Result<()> {
        for sec in &td.secondary_indexes {
            let old_vals: Vec<&Value> = sec.columns.iter().filter_map(|&i| old_values.get(i)).collect();
            let new_vals: Vec<&Value> = sec.columns.iter().filter_map(|&i| new_values.get(i)).collect();
            if old_vals.len() != sec.columns.len() || new_vals.len() != sec.columns.len() {
                continue;
            }
            if old_vals == new_vals {
                continue;
            }
            let _ = sec.remove(&old_vals, pk);
            sec.insert(&new_vals, pk).map_err(|e| QueryError::Execution(e.to_string()))?;
        }
        Ok(())
    }

    fn exec_alter_table(&self, alter: AlterTableStmt) -> Result<ExecutionResult> {
        let mut tables = self.tables.write();
        let table_data = tables.get_mut(&alter.table)
            .ok_or_else(|| QueryError::Execution(format!("Table '{}' not found", alter.table)))?;

        match alter.action {
            AlterTableAction::AddColumn(col_def) => {
                let mut col = Column::new(col_def.name.clone(), col_def.data_type);
                if !col_def.nullable {
                    col = col.not_null();
                }
                if col_def.primary_key {
                    col = col.primary_key();
                }
                if let Some(len) = col_def.max_length {
                    col = col.with_max_length(len);
                }

                let mut columns = table_data.schema.columns.clone();
                columns.push(col);
                let new_td = TableData {
                    schema: Schema::new(table_data.schema.table_name.clone(), columns),
                    index: Arc::clone(&table_data.index),
                    version_store: Arc::clone(&table_data.version_store),
                    check_exprs: table_data.check_exprs.clone(),
                    sequences: table_data.sequences.clone(),
                    secondary_indexes: table_data.secondary_indexes.clone(),
                };
                *table_data = Arc::new(new_td);

                Ok(ExecutionResult::Ok(format!("Column '{}' added", col_def.name)))
            }
            AlterTableAction::DropColumn(col_name) => {
                let idx = table_data.schema.column_index(&col_name)
                    .ok_or_else(|| QueryError::Execution(format!("Column '{}' not found", col_name)))?;

                let pk_cols = table_data.schema.primary_key_columns();
                if pk_cols.contains(&idx) {
                    return Err(QueryError::Execution("Cannot drop primary key column".into()));
                }

                let mut columns = table_data.schema.columns.clone();
                columns.remove(idx);
                let new_td = TableData {
                    schema: Schema::new(table_data.schema.table_name.clone(), columns),
                    index: Arc::clone(&table_data.index),
                    version_store: Arc::clone(&table_data.version_store),
                    check_exprs: table_data.check_exprs.clone(),
                    sequences: table_data.sequences.clone(),
                    secondary_indexes: Vec::new(),
                };
                *table_data = Arc::new(new_td);
                if let Some(ds) = &self.disk_store {
                    let _ = ds.upsert_table_indexes(&alter.table, Vec::new());
                }

                Ok(ExecutionResult::Ok(format!("Column '{}' dropped", col_name)))
            }
            AlterTableAction::RenameColumn { old_name, new_name } => {
                let idx = table_data.schema.column_index(&old_name)
                    .ok_or_else(|| QueryError::Execution(format!("Column '{}' not found", old_name)))?;

                let mut columns = table_data.schema.columns.clone();
                columns[idx].name = new_name.clone();
                let new_td = TableData {
                    schema: Schema::new(table_data.schema.table_name.clone(), columns),
                    index: Arc::clone(&table_data.index),
                    version_store: Arc::clone(&table_data.version_store),
                    check_exprs: table_data.check_exprs.clone(),
                    sequences: table_data.sequences.clone(),
                    secondary_indexes: Vec::new(),
                };
                *table_data = Arc::new(new_td);
                if let Some(ds) = &self.disk_store {
                    let _ = ds.upsert_table_indexes(&alter.table, Vec::new());
                }

                Ok(ExecutionResult::Ok(format!("Column '{}' renamed to '{}'", old_name, new_name)))
            }
        }
    }

    fn exec_union(&self, left: Statement, right: Statement, all: bool, txn_id: Option<TxnId>) -> Result<ExecutionResult> {
        let left_result = self.execute(left, txn_id)?;
        let right_result = self.execute(right, txn_id)?;

        match (left_result, right_result) {
            (ExecutionResult::Rows { columns, rows: left_rows }, ExecutionResult::Rows { rows: right_rows, .. }) => {
                let mut rows = left_rows;
                rows.extend(right_rows);
                if all {
                    Ok(ExecutionResult::Rows { columns, rows })
                } else {
                    let distinct_result = self.apply_distinct(ExecutionResult::Rows { columns, rows })?;
                    Ok(distinct_result)
                }
            }
            _ => Err(QueryError::Execution("UNION requires two SELECT statements".into())),
        }
    }

    fn exec_intersect(&self, left: Statement, right: Statement, all: bool, txn_id: Option<TxnId>) -> Result<ExecutionResult> {
        let left_result = self.execute(left, txn_id)?;
        let right_result = self.execute(right, txn_id)?;

        match (left_result, right_result) {
            (ExecutionResult::Rows { columns, rows: left_rows }, ExecutionResult::Rows { rows: right_rows, .. }) => {
                let mut right_set: HashMap<Vec<u8>, usize> = HashMap::new();
                for row in &right_rows {
                    let key = self.serialize_row(row);
                    *right_set.entry(key).or_insert(0) += 1;
                }
                let mut rows = Vec::new();
                for row in &left_rows {
                    let key = self.serialize_row(row);
                    if let Some(count) = right_set.get_mut(&key) {
                        if *count > 0 {
                            rows.push(row.clone());
                            if !all { right_set.remove(&key); }
                            else { *count -= 1; }
                        }
                    }
                }
                Ok(ExecutionResult::Rows { columns, rows })
            }
            _ => Err(QueryError::Execution("INTERSECT requires two SELECT statements".into())),
        }
    }

    fn exec_except(&self, left: Statement, right: Statement, all: bool, txn_id: Option<TxnId>) -> Result<ExecutionResult> {
        let left_result = self.execute(left, txn_id)?;
        let right_result = self.execute(right, txn_id)?;

        match (left_result, right_result) {
            (ExecutionResult::Rows { columns, rows: left_rows }, ExecutionResult::Rows { rows: right_rows, .. }) => {
                let mut right_set: HashMap<Vec<u8>, usize> = HashMap::new();
                for row in &right_rows {
                    let key = self.serialize_row(row);
                    *right_set.entry(key).or_insert(0) += 1;
                }
                let mut rows = Vec::new();
                for row in &left_rows {
                    let key = self.serialize_row(row);
                    if let Some(count) = right_set.get_mut(&key) {
                        if *count > 0 {
                            if all { *count -= 1; }
                            else { right_set.remove(&key); }
                            continue;
                        }
                    }
                    rows.push(row.clone());
                }
                Ok(ExecutionResult::Rows { columns, rows })
            }
            _ => Err(QueryError::Execution("EXCEPT requires two SELECT statements".into())),
        }
    }

    fn serialize_row(&self, row: &[Value]) -> Vec<u8> {
        let mut buf = Vec::new();
        for v in row {
            match v {
                Value::Int64(n) => { buf.push(1); buf.extend_from_slice(&n.to_be_bytes()); }
                Value::Float64(f) => { buf.push(2); buf.extend_from_slice(&f.to_bits().to_be_bytes()); }
                Value::Text(s) => { buf.push(3); buf.extend_from_slice(s.as_bytes()); buf.push(0); }
                Value::Bool(b) => { buf.push(4); buf.push(*b as u8); }
                Value::Null => { buf.push(0); }
                _ => { buf.push(5); }
            }
        }
        buf
    }

    fn exec_insert(&self, table: &str, columns: Option<Vec<String>>, source: InsertSource, on_conflict: Option<OnConflict>, returning: Option<Vec<SelectColumn>>, txn_id: Option<TxnId>) -> Result<ExecutionResult> {
        let rows_to_insert: Vec<Vec<Value>> = match source {
            InsertSource::Values(values) => {
                let table_data = {
                    let tables = self.tables.read();
                    tables.get(table)
                        .ok_or_else(|| QueryError::Execution(format!("Table '{}' not found", table)))?
                        .clone()
                };
                let num_cols = table_data.schema.columns.len();

                let col_indices: Option<Vec<usize>> = columns.as_ref().map(|cols| {
                    cols.iter().map(|c| {
                        table_data.schema.column_index(c)
                            .ok_or_else(|| QueryError::Execution(format!("Column '{}' not found", c)))
                    }).collect::<Result<Vec<_>>>()
                }).transpose()?;

                let mut rows = Vec::new();
                for row_exprs in &values {
                    let mut row_values = vec![Value::Null; num_cols];
                    if let Some(ref indices) = col_indices {
                        for (i, e) in row_exprs.iter().enumerate() {
                            if i < indices.len() {
                                row_values[indices[i]] = self.eval_insert_value(e, &table_data.schema, indices[i]);
                            }
                        }
                    } else {
                        for (i, e) in row_exprs.iter().enumerate() {
                            if i < num_cols {
                                row_values[i] = self.eval_insert_value(e, &table_data.schema, i);
                            }
                        }
                    }
                    rows.push(row_values);
                }
                rows
            }
            InsertSource::Select(select_stmt) => {
                let select_result = self.execute(Statement::Select(select_stmt), txn_id)?;
                match select_result {
                    ExecutionResult::Rows { rows, .. } => rows,
                    _ => return Err(QueryError::Execution("INSERT ... SELECT did not return rows".into())),
                }
            }
        };

        let table_data = {
            let tables = self.tables.read();
            tables.get(table)
                .ok_or_else(|| QueryError::Execution(format!("Table '{}' not found", table)))?
                .clone()
        };

        let table_id = table_data.schema.columns.len() as u32;
        let pk_cols = table_data.schema.primary_key_columns();
        let num_cols = table_data.schema.columns.len();
        let mut count = 0u64;
        let mut returned_rows: Vec<Vec<Value>> = Vec::new();

        for mut row_values in rows_to_insert {
            row_values.resize(num_cols, Value::Null);

            for (i, col) in table_data.schema.columns.iter().enumerate() {
                if row_values[i].is_null() {
                    if col.auto_increment {
                        if let Some(seq) = table_data.sequences.get(&col.name) {
                            row_values[i] = Value::Int64(seq.next());
                        }
                    } else if let Some(ref default) = col.default {
                        row_values[i] = default.clone();
                    }
                } else if col.auto_increment {
                    if let Value::Int64(n) = &row_values[i] {
                        if let Some(seq) = table_data.sequences.get(&col.name) {
                            seq.observe(*n);
                        }
                    }
                }

                if !row_values[i].is_null() {
                    if let Some(max_len) = table_data.schema.columns[i].max_length {
                        if let Value::Text(s) = &row_values[i] {
                            if s.len() > max_len {
                                row_values[i] = Value::Text(s[..max_len].to_string());
                            }
                        }
                    }
                    if let Value::Text(s) = &row_values[i] {
                        match col.data_type {
                            DataType::Date => {
                                if let Some(d) = bytedb_core::tuple::value::parse_date(s) {
                                    row_values[i] = Value::Date(d);
                                }
                            }
                            DataType::Timestamp => {
                                if let Some(ts) = bytedb_core::tuple::value::parse_timestamp(s) {
                                    row_values[i] = Value::Timestamp(ts);
                                }
                            }
                            DataType::Uuid => {
                                if let Some(b) = bytedb_core::tuple::value::parse_uuid(s) {
                                    row_values[i] = Value::Uuid(b);
                                }
                            }
                            DataType::Decimal => {
                                if let Some((m, sc)) = bytedb_core::tuple::value::parse_decimal(s) {
                                    row_values[i] = Value::Decimal(m, sc);
                                }
                            }
                            _ => {}
                        }
                    } else if let Value::Int64(n) = &row_values[i] {
                        if matches!(col.data_type, DataType::Decimal) {
                            row_values[i] = Value::Decimal(*n as i128, 0);
                        }
                    } else if let Value::Float64(f) = &row_values[i] {
                        if matches!(col.data_type, DataType::Decimal) {
                            if let Some((m, sc)) = bytedb_core::tuple::value::parse_decimal(&f.to_string()) {
                                row_values[i] = Value::Decimal(m, sc);
                            }
                        }
                    }
                }
                if row_values[i].is_null() && !col.nullable {
                    return Err(QueryError::not_null_violation(&col.name));
                }
            }

            for (i, col) in table_data.schema.columns.iter().enumerate() {
                if col.unique && !col.primary_key && !row_values[i].is_null() {
                    let val = &row_values[i];
                    if let Some(sec) = table_data.secondary_indexes.iter()
                        .find(|s| s.columns.as_slice() == [i])
                    {
                        let hits = sec.lookup_eq(&[val])
                            .map_err(|e| QueryError::Execution(e.to_string()))?;
                        if !hits.is_empty() {
                            return Err(QueryError::unique_violation(&col.name));
                        }
                    } else {
                        let mut violated = false;
                        table_data.index.for_each(|_key, other_data| {
                            if let Some(ov) = bytedb_core::tuple::tuple::read_value_at(other_data, i) {
                                if &ov == val {
                                    violated = true;
                                    return false;
                                }
                            }
                            true
                        }).ok();
                        if violated {
                            return Err(QueryError::unique_violation(&col.name));
                        }
                    }
                }
            }

            {
                let tup = Tuple::new(row_values.clone());
                for chk in &table_data.check_exprs {
                    if !self.eval_predicate(chk, &tup, &table_data.schema) {
                        return Err(QueryError::check_violation(table));
                    }
                }
            }

            for fk in &table_data.schema.foreign_keys {
                let mut child_vals: Vec<Value> = Vec::new();
                let mut any_null = false;
                for cname in &fk.columns {
                    if let Some(idx) = table_data.schema.column_index(cname) {
                        if row_values[idx].is_null() { any_null = true; break; }
                        child_vals.push(row_values[idx].clone());
                    }
                }
                if any_null { continue; }
                let tables = self.tables.read();
                let parent = tables.get(&fk.ref_table)
                    .ok_or_else(|| QueryError::Execution(format!("Foreign key references unknown table '{}'", fk.ref_table)))?;
                let parent_idxs: Vec<usize> = fk.ref_columns.iter()
                    .filter_map(|c| parent.schema.column_index(c)).collect();
                let parent_pk = parent.schema.primary_key_columns();
                let found = if !parent_pk.is_empty() && parent_idxs == parent_pk {
                    let probe = Tuple::new(child_vals.clone());
                    let cols: Vec<usize> = (0..child_vals.len()).collect();
                    let pk_key = probe.key_bytes(&cols);
                    parent.index.search(&pk_key)
                        .map_err(|e| QueryError::Execution(e.to_string()))?
                        .is_some()
                } else {
                    let mut hit = false;
                    parent.index.for_each(|_k, pdata| {
                        let mut all_eq = true;
                        for (j, pi) in parent_idxs.iter().enumerate() {
                            match bytedb_core::tuple::tuple::read_value_at(pdata, *pi) {
                                Some(pv) if &pv == &child_vals[j] => {}
                                _ => { all_eq = false; break; }
                            }
                        }
                        if all_eq { hit = true; return false; }
                        true
                    }).ok();
                    hit
                };
                if !found {
                    return Err(QueryError::fk_violation(&fk.ref_table));
                }
            }

            let tuple = Tuple::new(row_values.clone());
            let key = if pk_cols.is_empty() {
                self.next_rowid(table, &table_data)
            } else {
                tuple.key_bytes(&pk_cols)
            };
            let data = tuple.serialize();

            let existing = table_data.index.search(&key)
                .map_err(|e| QueryError::Execution(e.to_string()))?;

            if existing.is_some() {
                if let Some(ref oc) = on_conflict {
                    match &oc.action {
                        ConflictAction::DoNothing => continue,
                        ConflictAction::DoUpdate(assignments) => {
                            let old_values = Tuple::deserialize_to_vec(existing.as_ref().unwrap());
                            let mut existing_tuple = Tuple::deserialize(existing.as_ref().unwrap()).unwrap();
                            for (col_name, expr) in assignments {
                                if let Some(idx) = table_data.schema.column_index(col_name) {
                                    let new_val = self.eval_value(expr, &existing_tuple, &table_data.schema);
                                    existing_tuple.set(idx, new_val);
                                }
                            }
                            let new_data = existing_tuple.serialize();
                            if let Some(ov) = &old_values {
                                Self::update_secondary_indexes_update(&table_data, ov, &existing_tuple.to_vec(), &key)?;
                            }
                            self.record_undo(txn_id, table, &key, existing.clone());
                            self.log_put(table, &key, &new_data);
                            table_data.index.insert(key, new_data)
                                .map_err(|e| QueryError::Execution(e.to_string()))?;
                            if returning.is_some() {
                                returned_rows.push(existing_tuple.to_vec());
                            }
                            count += 1;
                            continue;
                        }
                    }
                } else if !pk_cols.is_empty() {
                    return Err(QueryError::Execution(format!(
                        "duplicate key value violates primary key constraint on table '{}'",
                        table
                    )));
                }
            }

            if let Some(tid) = txn_id {
                self.wal_append(LogRecord::Insert {
                    txn_id: tid,
                    table_id,
                    page_id: 0,
                    slot: count as u16,
                    data: data.clone(),
                });
                let snapshot = self.txn_manager.get_snapshot(tid)
                    .map_err(|e| QueryError::Execution(e.to_string()))?;
                table_data.version_store.insert(key.clone(), tuple.clone(), tid, snapshot.start_ts);
                self.ssi_write(Some(tid), table, &key);
            }

            Self::update_secondary_indexes_insert(&table_data, &row_values, &key)?;

            self.record_undo(txn_id, table, &key, existing.clone());
            self.log_put(table, &key, &data);
            table_data.index.insert(key, data)
                .map_err(|e| QueryError::Execution(e.to_string()))?;

            if returning.is_some() {
                returned_rows.push(row_values);
            }
            count += 1;
        }

        if let Some(ref ret_cols) = returning {
            let col_names: Vec<String> = table_data.schema.columns.iter().map(|c| c.name.clone()).collect();
            return self.build_returning_result(ret_cols, &returned_rows, &col_names, &table_data.schema);
        }

        Ok(ExecutionResult::Modified { count })
    }

    fn exec_update(&self, table: &str, assignments: Vec<(String, Expr)>, filter: Option<Expr>, returning: Option<Vec<SelectColumn>>, txn_id: Option<TxnId>) -> Result<ExecutionResult> {
        let table_data = {
            let tables = self.tables.read();
            tables.get(table)
                .ok_or_else(|| QueryError::Execution(format!("Table '{}' not found", table)))?
                .clone()
        };

        let table_id = table_data.schema.columns.len() as u32;

        let entries: Vec<(Vec<u8>, Vec<u8>)> = if let Some(key) = self.try_pk_lookup(filter.as_ref(), &table_data.schema) {
            self.ssi_read(txn_id, table, &key);
            match table_data.read_visible_one(&self.txn_manager, txn_id, &key)? {
                Some(data) => vec![(key, data)],
                None => vec![],
            }
        } else {
            self.ssi_predicate(txn_id, table);
            table_data.read_visible_entries(&self.txn_manager, txn_id)?
        };

        let mut count = 0u64;
        let mut returned_rows: Vec<Vec<Value>> = Vec::new();

        for (key, data) in entries {
            if let Some(mut tuple) = Tuple::deserialize(&data) {
                let matches = if let Some(ref f) = filter {
                    self.eval_predicate(f, &tuple, &table_data.schema)
                } else {
                    true
                };

                if matches {
                    self.ssi_read(txn_id, table, &key);
                    let old_data = data.clone();
                    self.record_undo(txn_id, table, &key, Some(old_data.clone()));
                    let old_row_values = tuple.to_vec();
                    for (col_name, expr) in &assignments {
                        if let Some(idx) = table_data.schema.column_index(col_name) {
                            let new_val = self.eval_value(expr, &tuple, &table_data.schema);
                            tuple.set(idx, new_val);
                        }
                    }

                    for (i, col) in table_data.schema.columns.iter().enumerate() {
                        if !col.nullable {
                            if let Some(val) = tuple.get(i) {
                                if val.is_null() {
                                    return Err(QueryError::not_null_violation(&col.name));
                                }
                            }
                        }
                    }

                    for (i, col) in table_data.schema.columns.iter().enumerate() {
                        if col.unique && !col.primary_key {
                            if let Some(val) = tuple.get(i) {
                                if !val.is_null() {
                                    if let Some(sec) = table_data.secondary_indexes.iter()
                                        .find(|s| s.columns.as_slice() == [i])
                                    {
                                        let hits = sec.lookup_eq(&[val])
                                            .map_err(|e| QueryError::Execution(e.to_string()))?;
                                        if hits.iter().any(|pk| pk != &key) {
                                            return Err(QueryError::Execution(
                                                format!("UNIQUE constraint violated for column '{}'", col.name)
                                            ));
                                        }
                                    } else {
                                        let mut violated = false;
                                        table_data.index.for_each(|other_key, other_data| {
                                            if other_key == key.as_slice() { return true; }
                                            if let Some(ov) = bytedb_core::tuple::tuple::read_value_at(other_data, i) {
                                                if &ov == val { violated = true; return false; }
                                            }
                                            true
                                        }).ok();
                                        if violated {
                                            return Err(QueryError::Execution(
                                                format!("UNIQUE constraint violated for column '{}'", col.name)
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }

                    for chk in &table_data.check_exprs {
                        if !self.eval_predicate(chk, &tuple, &table_data.schema) {
                            return Err(QueryError::Execution(
                                format!("CHECK constraint failed on table '{}'", table)
                            ));
                        }
                    }

                    for fk in &table_data.schema.foreign_keys {
                        let mut child_vals: Vec<Value> = Vec::new();
                        let mut any_null = false;
                        for cname in &fk.columns {
                            if let Some(idx) = table_data.schema.column_index(cname) {
                                let v = tuple.get(idx).cloned().unwrap_or(Value::Null);
                                if v.is_null() { any_null = true; break; }
                                child_vals.push(v);
                            }
                        }
                        if any_null { continue; }
                        let tables = self.tables.read();
                        let parent = tables.get(&fk.ref_table)
                            .ok_or_else(|| QueryError::Execution(format!("FK references unknown table '{}'", fk.ref_table)))?;
                        let parent_idxs: Vec<usize> = fk.ref_columns.iter()
                            .filter_map(|c| parent.schema.column_index(c)).collect();
                        let parent_all = parent.index.scan_all()
                            .map_err(|e| QueryError::Execution(e.to_string()))?;
                        let mut found = false;
                        for (_, pdata) in &parent_all {
                            if let Some(pt) = Tuple::deserialize(pdata) {
                                let mut all_eq = true;
                                for (j, pi) in parent_idxs.iter().enumerate() {
                                    if pt.get(*pi) != Some(&child_vals[j]) { all_eq = false; break; }
                                }
                                if all_eq { found = true; break; }
                            }
                        }
                        if !found {
                            return Err(QueryError::Execution(
                                format!("FOREIGN KEY violation: no matching row in '{}'", fk.ref_table)
                            ));
                        }
                    }

                    let new_data = tuple.serialize();

                    if let Some(tid) = txn_id {
                        let snapshot = self.txn_manager.get_snapshot(tid)
                            .map_err(|e| QueryError::Execution(e.to_string()))?;
                        table_data.version_store.ensure_base(&key, Tuple::new(old_row_values.clone()));
                        table_data.version_store.try_update(
                            key.clone(),
                            tuple.clone(),
                            tid,
                            snapshot.start_ts,
                            snapshot.start_ts,
                            &snapshot.active_txns,
                        )?;
                        self.ssi_write(Some(tid), table, &key);
                        self.wal_append(LogRecord::Update {
                            txn_id: tid,
                            table_id,
                            page_id: 0,
                            slot: count as u16,
                            old_data,
                            new_data: new_data.clone(),
                        });
                    }

                    Self::update_secondary_indexes_update(&table_data, &old_row_values, &tuple.to_vec(), &key)?;

                    self.log_put(table, &key, &new_data);
                    table_data.index.insert(key, new_data)
                        .map_err(|e| QueryError::Execution(e.to_string()))?;

                    if returning.is_some() {
                        returned_rows.push(tuple.to_vec());
                    }
                    count += 1;
                }
            }
        }

        if let Some(ref ret_cols) = returning {
            let col_names: Vec<String> = table_data.schema.columns.iter().map(|c| c.name.clone()).collect();
            return self.build_returning_result(ret_cols, &returned_rows, &col_names, &table_data.schema);
        }

        Ok(ExecutionResult::Modified { count })
    }

    fn exec_delete(&self, table: &str, filter: Option<Expr>, returning: Option<Vec<SelectColumn>>, txn_id: Option<TxnId>) -> Result<ExecutionResult> {
        let table_data = {
            let tables = self.tables.read();
            tables.get(table)
                .ok_or_else(|| QueryError::Execution(format!("Table '{}' not found", table)))?
                .clone()
        };

        let table_id = table_data.schema.columns.len() as u32;

        let entries: Vec<(Vec<u8>, Vec<u8>)> = if let Some(key) = self.try_pk_lookup(filter.as_ref(), &table_data.schema) {
            self.ssi_read(txn_id, table, &key);
            match table_data.read_visible_one(&self.txn_manager, txn_id, &key)? {
                Some(data) => vec![(key, data)],
                None => vec![],
            }
        } else {
            self.ssi_predicate(txn_id, table);
            table_data.read_visible_entries(&self.txn_manager, txn_id)?
        };

        let mut count = 0u64;
        let mut keys_to_delete = Vec::new();
        let mut returned_rows: Vec<Vec<Value>> = Vec::new();

        for (key, data) in entries {
            if let Some(tuple) = Tuple::deserialize(&data) {
                let matches = if let Some(ref f) = filter {
                    self.eval_predicate(f, &tuple, &table_data.schema)
                } else {
                    true
                };
                if matches {
                    self.ssi_read(txn_id, table, &key);
                    self.record_undo(txn_id, table, &key, Some(data.clone()));

                    let mut cascade_targets: Vec<(String, bytedb_core::tuple::schema::FkAction, Vec<u8>, Vec<usize>)> = Vec::new();
                    let tables = self.tables.read();
                    for (other_name, other_td) in tables.iter() {
                        if other_name == table { continue; }
                        for fk in &other_td.schema.foreign_keys {
                            if fk.ref_table != table { continue; }
                            let parent_idxs: Vec<usize> = fk.ref_columns.iter()
                                .filter_map(|c| table_data.schema.column_index(c)).collect();
                            let mut parent_vals: Vec<Value> = Vec::new();
                            let mut any_null = false;
                            for pi in &parent_idxs {
                                let v = tuple.get(*pi).cloned().unwrap_or(Value::Null);
                                if v.is_null() { any_null = true; break; }
                                parent_vals.push(v);
                            }
                            if any_null { continue; }
                            let child_idxs: Vec<usize> = fk.columns.iter()
                                .filter_map(|c| other_td.schema.column_index(c)).collect();
                            let child_all = other_td.index.scan_all()
                                .map_err(|e| QueryError::Execution(e.to_string()))?;
                            for (ck, cdata) in &child_all {
                                if let Some(ct) = Tuple::deserialize(cdata) {
                                    let mut all_eq = true;
                                    for (j, ci) in child_idxs.iter().enumerate() {
                                        if ct.get(*ci) != Some(&parent_vals[j]) { all_eq = false; break; }
                                    }
                                    if all_eq {
                                        match fk.on_delete {
                                            bytedb_core::tuple::schema::FkAction::Restrict => {
                                                return Err(QueryError::fk_referenced(other_name));
                                            }
                                            other => {
                                                cascade_targets.push((other_name.clone(), other, ck.clone(), child_idxs.clone()));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    for (cname, action, ck, child_idxs) in &cascade_targets {
                        let ctd = tables.get(cname).unwrap();
                        match action {
                            bytedb_core::tuple::schema::FkAction::Cascade => {
                                let child_prev = ctd.index.search(ck).map_err(|e| QueryError::Execution(e.to_string()))?;
                                if let Some(d) = &child_prev {
                                    if let Some(cvals) = Tuple::deserialize_to_vec(d) {
                                        Self::update_secondary_indexes_delete(ctd, &cvals, ck);
                                    }
                                }
                                self.record_undo(txn_id, cname, ck, child_prev);
                                self.log_del(cname, ck);
                                ctd.index.delete(ck).map_err(|e| QueryError::Execution(e.to_string()))?;
                            }
                            bytedb_core::tuple::schema::FkAction::SetNull => {
                                if let Some(d) = ctd.index.search(ck).map_err(|e| QueryError::Execution(e.to_string()))? {
                                    if let Some(mut ct) = Tuple::deserialize(&d) {
                                        let old_vals = ct.to_vec();
                                        for ci in child_idxs {
                                            ct.set(*ci, Value::Null);
                                        }
                                        Self::update_secondary_indexes_update(ctd, &old_vals, &ct.to_vec(), ck)?;
                                        let serialized = ct.serialize();
                                        self.record_undo(txn_id, cname, ck, Some(d.clone()));
                                        self.log_put(cname, ck, &serialized);
                                        ctd.index.insert(ck.clone(), serialized)
                                            .map_err(|e| QueryError::Execution(e.to_string()))?;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    if let Some(tid) = txn_id {
                        let snapshot = self.txn_manager.get_snapshot(tid)
                            .map_err(|e| QueryError::Execution(e.to_string()))?;
                        table_data.version_store.ensure_base(&key, tuple.clone());
                        table_data.version_store.try_delete(&key, tid, snapshot.start_ts, snapshot.start_ts, &snapshot.active_txns)?;
                        self.ssi_write(Some(tid), table, &key);
                        self.wal_append(LogRecord::Delete {
                            txn_id: tid,
                            table_id,
                            page_id: 0,
                            slot: count as u16,
                            old_data: data,
                        });
                    }
                    if returning.is_some() {
                        returned_rows.push(tuple.to_vec());
                    }
                    Self::update_secondary_indexes_delete(&table_data, &tuple.to_vec(), &key);
                    keys_to_delete.push(key);
                    count += 1;
                }
            }
        }

        for key in keys_to_delete {
            self.log_del(table, &key);
            table_data.index.delete(&key)
                .map_err(|e| QueryError::Execution(e.to_string()))?;
        }

        if let Some(ref ret_cols) = returning {
            let col_names: Vec<String> = table_data.schema.columns.iter().map(|c| c.name.clone()).collect();
            return self.build_returning_result(ret_cols, &returned_rows, &col_names, &table_data.schema);
        }

        Ok(ExecutionResult::Modified { count })
    }

    fn exec_information_schema_tables(&self, filter: Option<&Expr>) -> Result<ExecutionResult> {
        let tables = self.tables.read();
        let columns = vec!["table_catalog".into(), "table_schema".into(), "table_name".into(), "table_type".into()];
        let schema = Schema::new("", columns.iter().map(|c: &String| Column::new(c.clone(), DataType::Text)).collect());
        let mut rows: Vec<Vec<Value>> = tables.keys().map(|name| {
            vec![
                Value::Text("bytedb".into()),
                Value::Text("public".into()),
                Value::Text(name.clone()),
                Value::Text("BASE TABLE".into()),
            ]
        }).collect();

        if let Some(f) = filter {
            rows.retain(|row| {
                let tuple = Tuple::new(row.clone());
                self.eval_predicate(f, &tuple, &schema)
            });
        }

        Ok(ExecutionResult::Rows { columns, rows })
    }

    fn exec_information_schema_columns(&self, filter: Option<&Expr>) -> Result<ExecutionResult> {
        let tables = self.tables.read();
        let columns = vec![
            "table_catalog".into(), "table_schema".into(), "table_name".into(),
            "column_name".into(), "ordinal_position".into(), "is_nullable".into(),
            "data_type".into(),
        ];
        let schema = Schema::new("", columns.iter().map(|c: &String| Column::new(c.clone(), DataType::Text)).collect());
        let mut rows: Vec<Vec<Value>> = Vec::new();

        for (table_name, td) in tables.iter() {
            for (i, col) in td.schema.columns.iter().enumerate() {
                rows.push(vec![
                    Value::Text("bytedb".into()),
                    Value::Text("public".into()),
                    Value::Text(table_name.clone()),
                    Value::Text(col.name.clone()),
                    Value::Int64((i + 1) as i64),
                    Value::Text(if col.nullable { "YES" } else { "NO" }.into()),
                    Value::Text(format!("{:?}", col.data_type)),
                ]);
            }
        }

        if let Some(f) = filter {
            rows.retain(|row| {
                let tuple = Tuple::new(row.clone());
                self.eval_predicate(f, &tuple, &schema)
            });
        }

        Ok(ExecutionResult::Rows { columns, rows })
    }

    fn materialize_cte(&self, name: &str, columns: &[String], rows: Vec<Vec<Value>>) -> Result<()> {
        let col_schemas: Vec<Column> = columns.iter().enumerate().map(|(i, c)| {
            Column {
                name: c.clone(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: i == 0,
                unique: false,
                auto_increment: false,
                default: None,
                max_length: None,
            }
        }).collect();

        let schema = Schema::new(name, col_schemas);
        let index = Arc::new(BPlusTree::new(name, 64));
        let version_store = Arc::new(VersionStore::new());
        let table_data = TableData { schema, index, version_store, check_exprs: Vec::new(), sequences: HashMap::new(), secondary_indexes: Vec::new() };

        for (i, row) in rows.into_iter().enumerate() {
            let tuple = Tuple::new(row);
            let key = (i as i64).to_be_bytes().to_vec();
            let data = tuple.serialize();
            table_data.index.insert(key, data)
                .map_err(|e| QueryError::Execution(e.to_string()))?;
        }

        self.tables.write().insert(name.to_string(), Arc::new(table_data));
        Ok(())
    }

    fn build_returning_result(&self, ret_cols: &[SelectColumn], rows: &[Vec<Value>], col_names: &[String], schema: &Schema) -> Result<ExecutionResult> {
        let is_star = matches!(ret_cols.first(), Some(SelectColumn::Star));
        if is_star {
            return Ok(ExecutionResult::Rows {
                columns: col_names.to_vec(),
                rows: rows.to_vec(),
            });
        }

        let mut out_col_names = Vec::new();
        let mut out_rows: Vec<Vec<Value>> = Vec::new();

        for col in ret_cols {
            match col {
                SelectColumn::Star => unreachable!(),
                SelectColumn::Expr(expr, alias) => {
                    let name = alias.clone().unwrap_or_else(|| match expr {
                        Expr::Column(c) => c.clone(),
                        _ => "?column?".to_string(),
                    });
                    out_col_names.push(name);
                }
            }
        }

        for row in rows {
            let tuple = Tuple::new(row.clone());
            let mut out_row = Vec::new();
            for col in ret_cols {
                match col {
                    SelectColumn::Star => unreachable!(),
                    SelectColumn::Expr(expr, _) => {
                        out_row.push(self.eval_value(expr, &tuple, schema));
                    }
                }
            }
            out_rows.push(out_row);
        }

        Ok(ExecutionResult::Rows { columns: out_col_names, rows: out_rows })
    }

    #[allow(clippy::too_many_arguments)]
    fn exec_index_scan(
        &self,
        table: &str,
        index_name: &str,
        column: &str,
        op: BinOp,
        value: &Expr,
        filter: Option<&Expr>,
        limit: Option<usize>,
        txn_id: Option<TxnId>,
    ) -> Result<ExecutionResult> {
        let table_data = {
            let tables = self.tables.read();
            tables.get(table)
                .ok_or_else(|| QueryError::Execution(format!("Table '{}' not found", table)))?
                .clone()
        };

        let sec = table_data.secondary_indexes.iter().find(|s| s.name == index_name).cloned();
        let safe = txn_id.is_none() || table_data.version_store.key_count() == 0;
        let sec = match (sec, safe) {
            (Some(s), true) => s,
            _ => return self.exec_seq_scan(table, filter, txn_id, limit, None),
        };

        let Some(col_idx) = table_data.schema.column_index(column) else {
            return self.exec_seq_scan(table, filter, txn_id, limit, None);
        };
        let col_type = table_data.schema.columns[col_idx].data_type;
        if matches!(col_type, DataType::Decimal | DataType::Json) {
            return self.exec_seq_scan(table, filter, txn_id, limit, None);
        }
        let probe = cast_value(self.eval_expr_simple(value), col_type);
        if probe.is_null() {
            return self.exec_seq_scan(table, filter, txn_id, limit, None);
        }

        let pks = match op {
            BinOp::Eq => sec.lookup_eq(&[&probe]),
            BinOp::Lt | BinOp::Lte => sec.lookup_range(None, Some(&probe)),
            BinOp::Gt | BinOp::Gte => sec.lookup_range(Some(&probe), None),
            _ => return self.exec_seq_scan(table, filter, txn_id, limit, None),
        }.map_err(|e| QueryError::Execution(e.to_string()))?;

        self.ssi_predicate(txn_id, table);
        let col_names: Vec<String> = table_data.schema.columns.iter().map(|c| c.name.clone()).collect();
        let mut rows = Vec::new();
        for pk in pks {
            let data_opt = if let Some(tid) = txn_id {
                table_data.read_visible_one(&self.txn_manager, Some(tid), &pk)?
            } else {
                table_data.index.search(&pk).map_err(|e| QueryError::Execution(e.to_string()))?
            };
            if let Some(data) = data_opt {
                if let Some(tuple) = Tuple::deserialize(&data) {
                    self.account_scan_row()?;
                    if let Some(f) = filter {
                        if !self.eval_predicate(f, &tuple, &table_data.schema) {
                            continue;
                        }
                    }
                    self.ssi_read(txn_id, table, &pk);
                    rows.push(tuple.into_vec());
                    if let Some(lim) = limit {
                        if rows.len() >= lim {
                            break;
                        }
                    }
                }
            }
        }

        Ok(ExecutionResult::Rows { columns: col_names, rows })
    }

    fn exec_seq_scan(&self, table: &str, filter: Option<&Expr>, txn_id: Option<TxnId>, limit: Option<usize>, needed_columns: Option<&[usize]>) -> Result<ExecutionResult> {
        if table == "information_schema.tables" {
            return self.exec_information_schema_tables(filter);
        }
        if table == "information_schema.columns" {
            return self.exec_information_schema_columns(filter);
        }

        let table_data = {
            let tables = self.tables.read();
            tables.get(table)
                .ok_or_else(|| QueryError::Execution(format!("Table '{}' not found", table)))?
                .clone()
        };

        let col_names: Vec<String> = table_data.schema.columns.iter()
            .map(|c| c.name.clone())
            .collect();

        let mut rows = Vec::new();

        if let Some(tid) = txn_id {
            if let Some(key) = self.try_pk_lookup(filter, &table_data.schema) {
                self.ssi_read(Some(tid), table, &key);
                if let Some(data) = table_data.read_visible_one(&self.txn_manager, Some(tid), &key)? {
                    if let Some(tuple) = Tuple::deserialize(&data) {
                        self.account_scan_row()?;
                        rows.push(tuple.into_vec());
                    }
                }
            } else if let Some((lo, hi)) = self.try_pk_range(filter, &table_data.schema) {
                self.ssi_predicate(Some(tid), table);
                let snapshot = self.txn_manager.get_snapshot(tid)
                    .map_err(|e| QueryError::Execution(e.to_string()))?;
                let base = table_data.index.range_scan(&lo, &hi)
                    .map_err(|e| QueryError::Execution(e.to_string()))?;
                let resolved = if table_data.version_store.key_count() == 0 {
                    None
                } else {
                    Some(table_data.version_store.snapshot_resolved(tid, snapshot.start_ts, &snapshot.active_txns))
                };
                for (k, v) in base {
                    let data = match &resolved {
                        None => v,
                        Some(map) => match map.get(&k) {
                            None => v,
                            Some(None) => continue,
                            Some(Some(t)) => t.serialize(),
                        },
                    };
                    if let Some(tuple) = Tuple::deserialize(&data) {
                        self.account_scan_row()?;
                        if let Some(f) = filter {
                            if !self.eval_predicate(f, &tuple, &table_data.schema) { continue; }
                        }
                        self.ssi_read(Some(tid), table, &k);
                        rows.push(tuple.into_vec());
                        if let Some(lim) = limit {
                            if rows.len() >= lim { break; }
                        }
                    }
                }
                if let Some(map) = resolved {
                    for (k, opt) in &map {
                        if let Some(t) = opt {
                            if k.as_slice() >= lo.as_slice() && k.as_slice() <= hi.as_slice() {
                                if table_data.index.search(k).map_err(|e| QueryError::Execution(e.to_string()))?.is_none() {
                                    let data = t.serialize();
                                    if let Some(tuple) = Tuple::deserialize(&data) {
                                        if let Some(f) = filter {
                                            if !self.eval_predicate(f, &tuple, &table_data.schema) { continue; }
                                        }
                                        self.ssi_read(Some(tid), table, k);
                                        rows.push(tuple.into_vec());
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                self.ssi_predicate(Some(tid), table);
                let entries = table_data.read_visible_entries(&self.txn_manager, Some(tid))?;
                let fast_filter = filter.and_then(|f| self.try_fast_filter(f, &table_data.schema));
                let op_code: u8 = fast_filter.as_ref().map(|(_, op, _)| match op {
                    BinOp::Eq => 0, BinOp::Neq => 1, BinOp::Lt => 2,
                    BinOp::Gt => 3, BinOp::Lte => 4, BinOp::Gte => 5, _ => 255,
                }).unwrap_or(255);
                for (key_ref, data) in &entries {
                    if let Some((col_idx, _, ref lit_val)) = fast_filter {
                        match raw_filter_matches(data, col_idx, op_code, lit_val) {
                            Some(true) => {
                                if let Some(tuple) = Tuple::deserialize(data) {
                                    self.account_scan_row()?;
                                    self.ssi_read(Some(tid), table, key_ref);
                                    rows.push(tuple.into_vec());
                                    if let Some(lim) = limit {
                                        if rows.len() >= lim { break; }
                                    }
                                }
                                continue;
                            }
                            Some(false) => { self.account_scan_row()?; continue; }
                            None => {}
                        }
                    }
                    if let Some(tuple) = Tuple::deserialize(data) {
                        self.account_scan_row()?;
                        let matches = if let Some(f) = filter {
                            self.eval_predicate(f, &tuple, &table_data.schema)
                        } else {
                            true
                        };
                        if matches {
                            self.ssi_read(Some(tid), table, key_ref);
                            rows.push(tuple.into_vec());
                            if let Some(lim) = limit {
                                if rows.len() >= lim { break; }
                            }
                        }
                    }
                }
            }
        } else if let Some(key) = self.try_pk_lookup(filter, &table_data.schema) {
            if let Ok(Some(data)) = table_data.index.search(&key) {
                if let Some(tuple) = Tuple::deserialize(&data) {
                    rows.push(tuple.into_vec());
                }
            }
        } else {
            let schema = &table_data.schema;
            let scan_limit = limit;
            let fast_filter = filter.and_then(|f| self.try_fast_filter(f, schema));

            if scan_limit.is_none()
                && filter.is_some()
                && table_data.index.approx_len() >= PARALLEL_SCAN_THRESHOLD
            {
                let leaves = table_data.index.collect_leaves();
                let parallel_rows = self.parallel_filter_leaves(
                    leaves,
                    schema,
                    filter,
                    needed_columns,
                    fast_filter.clone(),
                )?;
                return Ok(ExecutionResult::Rows { columns: col_names, rows: parallel_rows });
            }

            let scan_err: std::cell::RefCell<Option<QueryError>> = std::cell::RefCell::new(None);
            if let Some((col_idx, op, ref lit_val)) = fast_filter {
                let op_code: u8 = match op {
                    BinOp::Eq => 0,
                    BinOp::Neq => 1,
                    BinOp::Lt => 2,
                    BinOp::Gt => 3,
                    BinOp::Lte => 4,
                    BinOp::Gte => 5,
                    _ => 255,
                };
                table_data.index.for_each(|_key, data| {
                    if let Err(e) = self.account_scan_row() {
                        *scan_err.borrow_mut() = Some(e);
                        return false;
                    }
                    match raw_filter_matches(data, col_idx, op_code, lit_val) {
                        Some(true) => {
                            if let Some(tuple) = Tuple::deserialize(data) {
                                rows.push(tuple.into_vec());
                                if let Some(lim) = scan_limit {
                                    if rows.len() >= lim { return false; }
                                }
                            }
                        }
                        Some(false) => {}
                        None => {}
                    }
                    true
                }).map_err(|e| QueryError::Execution(e.to_string()))?;
            } else if filter.is_none() && scan_limit.is_none() && needed_columns.is_none() {
                rows.reserve(table_data.index.approx_len());
                let ctx = self.active_ctx.read().as_ref().map(Arc::clone);
                let has_scan_limit = ctx.as_ref().map(|c| c.limits().max_scan_rows.is_some()).unwrap_or(false);
                let mut since_check: u32 = 0;
                table_data.index.for_each(|_key, data| {
                    if let Some(c) = &ctx {
                        if has_scan_limit {
                            if let Err(e) = c.account_scan_row() {
                                *scan_err.borrow_mut() = Some(e);
                                return false;
                            }
                        } else {
                            since_check += 1;
                            if since_check >= 1024 {
                                since_check = 0;
                                if let Err(e) = c.check() {
                                    *scan_err.borrow_mut() = Some(e);
                                    return false;
                                }
                            }
                        }
                    }
                    if let Some(values) = Tuple::deserialize_to_vec(data) {
                        rows.push(values);
                    }
                    true
                }).map_err(|e| QueryError::Execution(e.to_string()))?;
                if let Some(c) = &ctx {
                    if !has_scan_limit {
                        let _ = c.account_scan_rows(rows.len() as u64);
                    }
                }
            } else {
                table_data.index.for_each(|_key, data| {
                    if let Err(e) = self.account_scan_row() {
                        *scan_err.borrow_mut() = Some(e);
                        return false;
                    }
                    let tuple = if let Some(nc) = needed_columns {
                        Tuple::deserialize_columns(data, nc)
                    } else {
                        Tuple::deserialize(data)
                    };
                    if let Some(tuple) = tuple {
                        let matches = if let Some(f) = filter {
                            self.eval_predicate(f, &tuple, schema)
                        } else {
                            true
                        };
                        if matches {
                            rows.push(tuple.into_vec());
                            if let Some(lim) = scan_limit {
                                if rows.len() >= lim { return false; }
                            }
                        }
                    }
                    true
                }).map_err(|e| QueryError::Execution(e.to_string()))?;
            }
            if let Some(e) = scan_err.into_inner() {
                return Err(e);
            }
        }

        Ok(ExecutionResult::Rows { columns: col_names, rows })
    }

    fn exec_scan_top_n(&self, table: &str, filter: Option<&Expr>, txn_id: Option<TxnId>, order_by: &[OrderByExpr], n: usize) -> Result<ExecutionResult> {
        let tables = self.tables.read();
        let table_data = tables.get(table)
            .ok_or_else(|| QueryError::Execution(format!("Table '{}' not found", table)))?;

        let col_names: Vec<String> = table_data.schema.columns.iter()
            .map(|c| c.name.clone())
            .collect();

        let sort_col_name = match &order_by[0].expr {
            Expr::Column(name) => name,
            _ => return Err(QueryError::Execution("Non-column ORDER BY".into())),
        };
        let sort_col_idx = table_data.schema.column_index(sort_col_name)
            .ok_or_else(|| QueryError::Execution(format!("Column '{}' not found", sort_col_name)))?;
        let asc = order_by[0].ascending;

        let schema = &table_data.schema;
        let fast_filter = filter.and_then(|f| self.try_fast_filter(f, schema));
        let op_code: u8 = fast_filter.as_ref().map(|(_, op, _)| match op {
            BinOp::Eq => 0, BinOp::Neq => 1, BinOp::Lt => 2,
            BinOp::Gt => 3, BinOp::Lte => 4, BinOp::Gte => 5, _ => 255,
        }).unwrap_or(255);

        let mut heap: BinaryHeap<(i64, usize)> = BinaryHeap::with_capacity(n + 1);
        let mut kept_data: Vec<Vec<u8>> = Vec::with_capacity(n + 1);
        let mut all_int = true;

        let mut process = |data: &[u8]| -> bool {
            let matches = if let Some((col_idx, _, ref lit_val)) = fast_filter {
                raw_filter_matches(data, col_idx, op_code, lit_val).unwrap_or(false)
            } else {
                true
            };
            if matches {
                match read_int64_at(data, sort_col_idx) {
                    Some(k) => {
                        let heap_key = if asc { k } else { -k };
                        if heap.len() < n {
                            let idx = kept_data.len();
                            kept_data.push(data.to_vec());
                            heap.push((heap_key, idx));
                        } else if let Some(&(top_key, _)) = heap.peek() {
                            if heap_key < top_key {
                                let (_, old_idx) = heap.pop().unwrap();
                                kept_data[old_idx].clear();
                                let idx = kept_data.len();
                                kept_data.push(data.to_vec());
                                heap.push((heap_key, idx));
                            }
                        }
                    }
                    None => { all_int = false; return false; }
                }
            }
            true
        };

        if let Some(tid) = txn_id {
            self.txn_manager.ssi_record_predicate(tid, table);
            let snapshot = self.txn_manager.get_snapshot(tid)
                .map_err(|e| QueryError::Execution(e.to_string()))?;
            let mut keep = true;
            table_data.index.for_each(|key, data| {
                if !keep { return false; }
                match table_data.version_store.lookup_for_read(key, tid, snapshot.start_ts, &snapshot.active_txns) {
                    bytedb_core::mvcc::version_store::ReadResult::Visible(t) => {
                        let serialized = t.serialize();
                        if !process(&serialized) { keep = false; }
                    }
                    bytedb_core::mvcc::version_store::ReadResult::Tombstone => {}
                    bytedb_core::mvcc::version_store::ReadResult::NoVersions => {
                        if !process(data) { keep = false; }
                    }
                }
                keep
            }).map_err(|e| QueryError::Execution(e.to_string()))?;
            if keep {
                let mut continue_loop = true;
                table_data.version_store.for_each_visible(tid, snapshot.start_ts, &snapshot.active_txns, |k, t| {
                    if !continue_loop { return false; }
                    if table_data.index.search(k).ok().flatten().is_some() {
                        return true;
                    }
                    let data = t.serialize();
                    if !process(&data) { continue_loop = false; }
                    continue_loop
                });
            }
        } else {
            table_data.index.for_each(|_key, data| {
                process(data)
            }).map_err(|e| QueryError::Execution(e.to_string()))?;
        }

        if !all_int {
            return Err(QueryError::Execution("Non-int sort column".into()));
        }

        let mut entries: Vec<(i64, Vec<Value>)> = heap.into_vec().into_iter()
            .filter_map(|(_, idx)| {
                let data = &kept_data[idx];
                if data.is_empty() { return None; }
                let tuple = Tuple::deserialize(data)?;
                let k = match tuple.get(sort_col_idx) {
                    Some(Value::Int64(v)) => *v,
                    _ => i64::MIN,
                };
                Some((k, tuple.to_vec()))
            })
            .collect();

        entries.sort_unstable_by(|(a, _), (b, _)| if asc { a.cmp(b) } else { b.cmp(a) });
        let result: Vec<Vec<Value>> = entries.into_iter().map(|(_, row)| row).collect();

        Ok(ExecutionResult::Rows { columns: col_names, rows: result })
    }

    fn try_fast_filter(&self, expr: &Expr, schema: &Schema) -> Option<(usize, BinOp, Value)> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Gt | BinOp::Lte | BinOp::Gte => {}
                    _ => return None,
                }
                match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(name), Expr::Literal(lit)) => {
                        let idx = schema.column_index(name)?;
                        let val = self.eval_expr_simple(&Expr::Literal(lit.clone()));
                        Some((idx, *op, val))
                    }
                    (Expr::Literal(lit), Expr::Column(name)) => {
                        let idx = schema.column_index(name)?;
                        let val = self.eval_expr_simple(&Expr::Literal(lit.clone()));
                        let flipped_op = match op {
                            BinOp::Lt => BinOp::Gt,
                            BinOp::Gt => BinOp::Lt,
                            BinOp::Lte => BinOp::Gte,
                            BinOp::Gte => BinOp::Lte,
                            other => *other,
                        };
                        Some((idx, flipped_op, val))
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn try_pk_range(&self, filter: Option<&Expr>, schema: &Schema) -> Option<(Vec<u8>, Vec<u8>)> {
        let filter = filter?;
        let pk_indices = schema.primary_key_columns();
        if pk_indices.len() != 1 {
            return None;
        }
        let pk_name = schema.columns[pk_indices[0]].name.clone();

        fn extract_bound(expr: &Expr, pk_name: &str) -> Option<(BinOp, LiteralValue)> {
            if let Expr::BinaryOp { left, op, right } = expr {
                match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(n), Expr::Literal(lit)) if n == pk_name => Some((*op, lit.clone())),
                    (Expr::Literal(lit), Expr::Column(n)) if n == pk_name => {
                        let flipped = match op {
                            BinOp::Lt => BinOp::Gt,
                            BinOp::Gt => BinOp::Lt,
                            BinOp::Lte => BinOp::Gte,
                            BinOp::Gte => BinOp::Lte,
                            other => *other,
                        };
                        Some((flipped, lit.clone()))
                    }
                    _ => None,
                }
            } else {
                None
            }
        }

        if let Expr::BinaryOp { left, op: BinOp::And, right } = filter {
            let l = extract_bound(left, &pk_name)?;
            let r = extract_bound(right, &pk_name)?;
            let (lo_lit, hi_lit) = match (l.0, r.0) {
                (BinOp::Gte, BinOp::Lte) | (BinOp::Gt, BinOp::Lte) => (l.1, r.1),
                (BinOp::Lte, BinOp::Gte) | (BinOp::Lte, BinOp::Gt) => (r.1, l.1),
                _ => return None,
            };
            return Some((self.literal_to_key_bytes(&lo_lit), self.literal_to_key_bytes(&hi_lit)));
        }
        None
    }

    fn try_pk_lookup(&self, filter: Option<&Expr>, schema: &Schema) -> Option<Vec<u8>> {
        let filter = filter?;
        let pk_indices = schema.primary_key_columns();
        if pk_indices.len() != 1 {
            return None;
        }
        let pk_name = &schema.columns[pk_indices[0]].name;

        match filter {
            Expr::BinaryOp { left, op: BinOp::Eq, right } => {
                match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(name), Expr::Literal(lit)) if name == pk_name => {
                        Some(self.literal_to_key_bytes(lit))
                    }
                    (Expr::Literal(lit), Expr::Column(name)) if name == pk_name => {
                        Some(self.literal_to_key_bytes(lit))
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn literal_to_key_bytes(&self, lit: &LiteralValue) -> Vec<u8> {
        let value = match lit {
            LiteralValue::Integer(n) => Value::Int64(*n),
            LiteralValue::Float(f) => Value::Float64(*f),
            LiteralValue::String(s) => Value::Text(s.clone()),
            LiteralValue::Bool(b) => Value::Bool(*b),
            LiteralValue::Null => Value::Null,
            LiteralValue::HexBlob(bytes) => Value::Bytes(bytes.clone()),
        };
        let tuple = Tuple::new(vec![value]);
        tuple.key_bytes(&[0])
    }

    fn apply_projection(&self, result: ExecutionResult, columns: &[SelectColumn]) -> Result<ExecutionResult> {
        match result {
            ExecutionResult::Rows { columns: col_names, rows } => {
                if columns.iter().any(|c| matches!(c, SelectColumn::Star)) {
                    return Ok(ExecutionResult::Rows { columns: col_names, rows });
                }

                let has_window = columns.iter().any(|c| matches!(c, SelectColumn::Expr(Expr::WindowFunction { .. }, _)));
                if has_window {
                    return self.apply_projection_with_windows(col_names, rows, columns);
                }

                let schema = Schema::new("", col_names.iter().map(|c| Column::new(c.clone(), DataType::Text)).collect());

                let mut new_col_names = Vec::new();
                for col in columns {
                    match col {
                        SelectColumn::Star => {
                            for name in &col_names {
                                new_col_names.push(name.clone());
                            }
                        }
                        SelectColumn::Expr(Expr::Column(name), alias) => {
                            new_col_names.push(alias.clone().unwrap_or_else(|| name.clone()));
                        }
                        SelectColumn::Expr(Expr::QualifiedColumn(_table, col_name), alias) => {
                            new_col_names.push(alias.clone().unwrap_or_else(|| col_name.clone()));
                        }
                        SelectColumn::Expr(Expr::Function { name, args }, alias) => {
                            let arg_name = if args.is_empty() { "*".to_string() } else {
                                match &args[0] {
                                    Expr::Column(c) => c.clone(),
                                    _ => "*".to_string(),
                                }
                            };
                            let func_col = format!("{}({})", name.to_lowercase(), arg_name);
                            new_col_names.push(alias.clone().unwrap_or(func_col));
                        }
                        SelectColumn::Expr(_, alias) => {
                            new_col_names.push(alias.clone().unwrap_or_else(|| "?".into()));
                        }
                    }
                }

                enum ProjStep<'a> {
                    StarAll,
                    Idx(usize),
                    Eval(&'a Expr),
                }
                let find_col_idx = |name: &str| -> Option<usize> {
                    col_names.iter().position(|c| c == name)
                        .or_else(|| {
                            let suffix = format!(".{}", name);
                            col_names.iter().position(|c| c.ends_with(&suffix))
                        })
                };
                let mut plan: Vec<ProjStep> = Vec::with_capacity(columns.len());
                for col in columns {
                    match col {
                        SelectColumn::Star => plan.push(ProjStep::StarAll),
                        SelectColumn::Expr(Expr::Column(name), _) => {
                            let idx = find_col_idx(name).unwrap_or(0);
                            plan.push(ProjStep::Idx(idx));
                        }
                        SelectColumn::Expr(Expr::QualifiedColumn(table, col_name), _) => {
                            let qualified = format!("{}.{}", table, col_name);
                            let idx = col_names.iter().position(|c| c == &qualified)
                                .or_else(|| find_col_idx(col_name))
                                .unwrap_or(0);
                            plan.push(ProjStep::Idx(idx));
                        }
                        SelectColumn::Expr(expr @ Expr::Function { name: fname, args }, _) => {
                            let arg_name = if args.is_empty() { "*".to_string() } else {
                                match &args[0] { Expr::Column(c) => c.clone(), _ => "*".to_string() }
                            };
                            let func_col = format!("{}({})", fname.to_lowercase(), arg_name);
                            if let Some(idx) = col_names.iter().position(|c| c == &func_col) {
                                plan.push(ProjStep::Idx(idx));
                            } else {
                                plan.push(ProjStep::Eval(expr));
                            }
                        }
                        SelectColumn::Expr(expr, _) => plan.push(ProjStep::Eval(expr)),
                    }
                }

                let out_width: usize = plan.iter().map(|s| match s {
                    ProjStep::StarAll => col_names.len(),
                    _ => 1,
                }).sum();

                let new_rows: Vec<Vec<Value>> = rows.into_iter().map(|row| {
                    let mut out = Vec::with_capacity(out_width);
                    let mut tuple_cache: Option<Tuple> = None;
                    for step in &plan {
                        match step {
                            ProjStep::StarAll => out.extend(row.iter().cloned()),
                            ProjStep::Idx(i) => out.push(row.get(*i).cloned().unwrap_or(Value::Null)),
                            ProjStep::Eval(expr) => {
                                let t = tuple_cache.get_or_insert_with(|| Tuple::new(row.clone()));
                                out.push(self.eval_value(expr, t, &schema));
                            }
                        }
                    }
                    out
                }).collect();

                Ok(ExecutionResult::Rows { columns: new_col_names, rows: new_rows })
            }
            other => Ok(other),
        }
    }

    fn apply_projection_with_windows(&self, col_names: Vec<String>, rows: Vec<Vec<Value>>, columns: &[SelectColumn]) -> Result<ExecutionResult> {
        let schema = Schema::new("", col_names.iter().map(|c| Column::new(c.clone(), DataType::Text)).collect());
        let num_rows = rows.len();

        let mut new_col_names = Vec::new();
        for col in columns {
            match col {
                SelectColumn::Star => new_col_names.extend(col_names.clone()),
                SelectColumn::Expr(Expr::Column(name), alias) => {
                    new_col_names.push(alias.clone().unwrap_or_else(|| name.clone()));
                }
                SelectColumn::Expr(Expr::WindowFunction { name, .. }, alias) => {
                    new_col_names.push(alias.clone().unwrap_or_else(|| name.to_lowercase()));
                }
                SelectColumn::Expr(Expr::Function { name, args }, alias) => {
                    let arg_name = if args.is_empty() { "*".to_string() } else {
                        match &args[0] { Expr::Column(c) => c.clone(), _ => "*".to_string() }
                    };
                    new_col_names.push(alias.clone().unwrap_or_else(|| format!("{}({})", name.to_lowercase(), arg_name)));
                }
                SelectColumn::Expr(_, alias) => {
                    new_col_names.push(alias.clone().unwrap_or_else(|| "?".into()));
                }
            }
        }

        let mut window_results: Vec<Vec<Value>> = Vec::new();
        for col in columns {
            if let SelectColumn::Expr(Expr::WindowFunction { name, args, partition_by, order_by }, _) = col {
                let values = self.compute_window_function(name, args, partition_by, order_by, &rows, &col_names, &schema);
                window_results.push(values);
            }
        }

        let mut new_rows: Vec<Vec<Value>> = Vec::with_capacity(num_rows);
        for row_idx in 0..num_rows {
            let tuple = Tuple::new(rows[row_idx].clone());
            let mut new_row = Vec::new();
            let mut win_idx = 0;
            for col in columns {
                match col {
                    SelectColumn::Star => new_row.extend(rows[row_idx].clone()),
                    SelectColumn::Expr(Expr::WindowFunction { .. }, _) => {
                        new_row.push(window_results[win_idx][row_idx].clone());
                        win_idx += 1;
                    }
                    SelectColumn::Expr(Expr::Column(name), _) => {
                        let idx = col_names.iter().position(|c| c == name).unwrap_or(0);
                        new_row.push(rows[row_idx].get(idx).cloned().unwrap_or(Value::Null));
                    }
                    SelectColumn::Expr(expr, _) => {
                        new_row.push(self.eval_value(expr, &tuple, &schema));
                    }
                }
            }
            new_rows.push(new_row);
        }

        Ok(ExecutionResult::Rows { columns: new_col_names, rows: new_rows })
    }

    fn compute_window_function(&self, name: &str, args: &[Expr], partition_by: &[Expr], order_by: &[OrderByExpr], rows: &[Vec<Value>], col_names: &[String], schema: &Schema) -> Vec<Value> {
        let num_rows = rows.len();
        let mut result = vec![Value::Null; num_rows];

        let partition_keys: Vec<Vec<Value>> = rows.iter().map(|row| {
            let tuple = Tuple::new(row.clone());
            partition_by.iter().map(|e| self.eval_value(e, &tuple, schema)).collect()
        }).collect();

        let sort_keys: Vec<Vec<Value>> = rows.iter().map(|row| {
            let tuple = Tuple::new(row.clone());
            order_by.iter().map(|o| self.eval_value(&o.expr, &tuple, schema)).collect()
        }).collect();

        let mut partitions: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        for i in 0..num_rows {
            let key = self.serialize_partition_key(&partition_keys[i]);
            partitions.entry(key).or_default().push(i);
        }

        let func_upper = name.to_uppercase();
        for (_key, mut indices) in partitions {
            if !order_by.is_empty() {
                indices.sort_by(|&a, &b| {
                    for (i, ob) in order_by.iter().enumerate() {
                        let va = &sort_keys[a][i];
                        let vb = &sort_keys[b][i];
                        let cmp = va.compare(vb).unwrap_or(Ordering::Equal);
                        let cmp = if ob.ascending { cmp } else { cmp.reverse() };
                        if cmp != Ordering::Equal { return cmp; }
                    }
                    Ordering::Equal
                });
            }

            match func_upper.as_str() {
                "ROW_NUMBER" => {
                    for (rank, &idx) in indices.iter().enumerate() {
                        result[idx] = Value::Int64((rank + 1) as i64);
                    }
                }
                "RANK" => {
                    let mut rank = 1;
                    for i in 0..indices.len() {
                        if i > 0 && sort_keys[indices[i]] != sort_keys[indices[i - 1]] {
                            rank = i + 1;
                        }
                        result[indices[i]] = Value::Int64(rank as i64);
                    }
                }
                "DENSE_RANK" => {
                    let mut rank = 1;
                    for i in 0..indices.len() {
                        if i > 0 && sort_keys[indices[i]] != sort_keys[indices[i - 1]] {
                            rank += 1;
                        }
                        result[indices[i]] = Value::Int64(rank as i64);
                    }
                }
                "SUM" => {
                    let col_idx = self.resolve_window_arg(args, col_names, schema);
                    let mut running_sum: f64 = 0.0;
                    for &idx in &indices {
                        if let Some(ci) = col_idx {
                            match &rows[idx][ci] {
                                Value::Int64(v) => running_sum += *v as f64,
                                Value::Float64(v) => running_sum += *v,
                                _ => {}
                            }
                        }
                        result[idx] = Value::Float64(running_sum);
                    }
                }
                "AVG" => {
                    let col_idx = self.resolve_window_arg(args, col_names, schema);
                    let mut running_sum: f64 = 0.0;
                    let mut count = 0i64;
                    for &idx in &indices {
                        if let Some(ci) = col_idx {
                            match &rows[idx][ci] {
                                Value::Int64(v) => { running_sum += *v as f64; count += 1; }
                                Value::Float64(v) => { running_sum += *v; count += 1; }
                                _ => {}
                            }
                        }
                        result[idx] = if count > 0 { Value::Float64(running_sum / count as f64) } else { Value::Null };
                    }
                }
                "COUNT" => {
                    let col_idx = self.resolve_window_arg(args, col_names, schema);
                    let mut count = 0i64;
                    for &idx in &indices {
                        if let Some(ci) = col_idx {
                            if !rows[idx][ci].is_null() { count += 1; }
                        } else {
                            count += 1;
                        }
                        result[idx] = Value::Int64(count);
                    }
                }
                "MIN" => {
                    let col_idx = self.resolve_window_arg(args, col_names, schema);
                    let mut min_val = Value::Null;
                    for &idx in &indices {
                        if let Some(ci) = col_idx {
                            let v = &rows[idx][ci];
                            if !v.is_null() {
                                min_val = match &min_val {
                                    Value::Null => v.clone(),
                                    cur => if v.compare(cur) == Some(Ordering::Less) { v.clone() } else { cur.clone() },
                                };
                            }
                        }
                        result[idx] = min_val.clone();
                    }
                }
                "MAX" => {
                    let col_idx = self.resolve_window_arg(args, col_names, schema);
                    let mut max_val = Value::Null;
                    for &idx in &indices {
                        if let Some(ci) = col_idx {
                            let v = &rows[idx][ci];
                            if !v.is_null() {
                                max_val = match &max_val {
                                    Value::Null => v.clone(),
                                    cur => if v.compare(cur) == Some(Ordering::Greater) { v.clone() } else { cur.clone() },
                                };
                            }
                        }
                        result[idx] = max_val.clone();
                    }
                }
                "LAG" => {
                    let col_idx = self.resolve_window_arg(args, col_names, schema);
                    let offset = if args.len() >= 2 {
                        match &args[1] { Expr::Literal(LiteralValue::Integer(n)) => *n as usize, _ => 1 }
                    } else { 1 };
                    for (i, &idx) in indices.iter().enumerate() {
                        if i >= offset {
                            let prev_idx = indices[i - offset];
                            result[idx] = col_idx.map(|ci| rows[prev_idx][ci].clone()).unwrap_or(Value::Null);
                        } else {
                            result[idx] = Value::Null;
                        }
                    }
                }
                "LEAD" => {
                    let col_idx = self.resolve_window_arg(args, col_names, schema);
                    let offset = if args.len() >= 2 {
                        match &args[1] { Expr::Literal(LiteralValue::Integer(n)) => *n as usize, _ => 1 }
                    } else { 1 };
                    for (i, &idx) in indices.iter().enumerate() {
                        if i + offset < indices.len() {
                            let next_idx = indices[i + offset];
                            result[idx] = col_idx.map(|ci| rows[next_idx][ci].clone()).unwrap_or(Value::Null);
                        } else {
                            result[idx] = Value::Null;
                        }
                    }
                }
                _ => {}
            }
        }

        result
    }

    fn resolve_window_arg(&self, args: &[Expr], col_names: &[String], _schema: &Schema) -> Option<usize> {
        if args.is_empty() { return None; }
        match &args[0] {
            Expr::Column(name) => col_names.iter().position(|c| c == name),
            _ => None,
        }
    }

    fn serialize_partition_key(&self, values: &[Value]) -> Vec<u8> {
        let mut buf = Vec::new();
        for v in values {
            match v {
                Value::Int64(n) => { buf.push(1); buf.extend_from_slice(&n.to_be_bytes()); }
                Value::Float64(f) => { buf.push(2); buf.extend_from_slice(&f.to_bits().to_be_bytes()); }
                Value::Text(s) => { buf.push(3); buf.extend_from_slice(s.as_bytes()); buf.push(0); }
                Value::Bool(b) => { buf.push(4); buf.push(*b as u8); }
                Value::Null => { buf.push(0); }
                _ => { buf.push(5); }
            }
        }
        buf
    }

    fn apply_filter(&self, result: ExecutionResult, predicate: &Expr) -> Result<ExecutionResult> {
        match result {
            ExecutionResult::Rows { columns, rows } => {
                let schema = Schema::new("", columns.iter().map(|c| Column::new(c.clone(), DataType::Text)).collect());
                let mut filtered: Vec<Vec<Value>> = Vec::with_capacity(rows.len() / 2);
                for row in rows.into_iter() {
                    let tuple = Tuple::new(row);
                    if self.eval_predicate(predicate, &tuple, &schema) {
                        filtered.push(tuple.into_vec());
                    }
                }
                Ok(ExecutionResult::Rows { columns, rows: filtered })
            }
            other => Ok(other),
        }
    }

    fn apply_distinct(&self, result: ExecutionResult) -> Result<ExecutionResult> {
        match result {
            ExecutionResult::Rows { columns, rows } => {
                let mut seen = std::collections::HashSet::new();
                let mut unique_rows = Vec::new();
                for (i, row) in rows.into_iter().enumerate() {
                    if i % 1024 == 0 { self.poll_ctx()?; }
                    let key: Vec<u8> = row.iter().flat_map(|v| {
                        let mut buf = Vec::new();
                        match v {
                            Value::Null => buf.push(0),
                            Value::Bool(b) => { buf.push(1); buf.push(*b as u8); }
                            Value::Int64(n) => { buf.push(2); buf.extend_from_slice(&n.to_le_bytes()); }
                            Value::Float64(f) => { buf.push(3); buf.extend_from_slice(&f.to_le_bytes()); }
                            Value::Text(s) => { buf.push(4); buf.extend_from_slice(s.as_bytes()); buf.push(0); }
                            _ => { buf.push(5); buf.extend_from_slice(&format!("{:?}", v).as_bytes()); buf.push(0); }
                        }
                        buf
                    }).collect();
                    if seen.insert(key) {
                        unique_rows.push(row);
                    }
                }
                Ok(ExecutionResult::Rows { columns, rows: unique_rows })
            }
            other => Ok(other),
        }
    }

    fn apply_sort(&self, result: ExecutionResult, order_by: &[OrderByExpr]) -> Result<ExecutionResult> {
        match result {
            ExecutionResult::Rows { columns, mut rows } => {
                let col_indices: Vec<(usize, bool, Option<bool>)> = order_by.iter().filter_map(|o| {
                    if let Expr::Column(name) = &o.expr {
                        columns.iter().position(|c| c == name).map(|i| (i, o.ascending, o.nulls_first))
                    } else {
                        None
                    }
                }).collect();

                if col_indices.len() == order_by.len() {
                    if col_indices.len() == 1 && col_indices[0].2.is_none() {
                        let (idx, asc, _) = col_indices[0];
                        let all_int = rows.iter().all(|r| matches!(r.get(idx), Some(Value::Int64(_)) | Some(Value::Null)));
                        if all_int {
                            let mut keyed: Vec<(i64, usize)> = rows.iter().enumerate().map(|(i, r)| {
                                let k = match r.get(idx) {
                                    Some(Value::Int64(n)) => *n,
                                    _ => i64::MIN,
                                };
                                (k, i)
                            }).collect();
                            if asc {
                                keyed.sort_unstable_by_key(|&(k, _)| k);
                            } else {
                                keyed.sort_unstable_by_key(|&(k, _)| std::cmp::Reverse(k));
                            }
                            let sorted: Vec<Vec<Value>> = keyed.into_iter().map(|(_, i)| {
                                std::mem::take(&mut rows[i])
                            }).collect();
                            return Ok(ExecutionResult::Rows { columns, rows: sorted });
                        }
                    }

                    rows.sort_unstable_by(|a, b| {
                        for &(idx, asc, nulls_first) in &col_indices {
                            let va = a.get(idx).unwrap_or(&Value::Null);
                            let vb = b.get(idx).unwrap_or(&Value::Null);
                            let a_null = va.is_null();
                            let b_null = vb.is_null();
                            if a_null || b_null {
                                if a_null && b_null { continue; }
                                let nf = nulls_first.unwrap_or(!asc);
                                return if a_null {
                                    if nf { Ordering::Less } else { Ordering::Greater }
                                } else {
                                    if nf { Ordering::Greater } else { Ordering::Less }
                                };
                            }
                            let ord = va.cmp_fast(vb);
                            if ord != Ordering::Equal {
                                return if asc { ord } else { ord.reverse() };
                            }
                        }
                        Ordering::Equal
                    });
                } else {
                    let schema = Schema::new("", columns.iter().map(|c| Column::new(c.clone(), DataType::Text)).collect());
                    rows.sort_by(|a, b| {
                        for ob in order_by {
                            let tuple_a = Tuple::new(a.clone());
                            let tuple_b = Tuple::new(b.clone());
                            let va = self.eval_value(&ob.expr, &tuple_a, &schema);
                            let vb = self.eval_value(&ob.expr, &tuple_b, &schema);
                            let a_null = va.is_null();
                            let b_null = vb.is_null();
                            if a_null || b_null {
                                if a_null && b_null { continue; }
                                let nf = ob.nulls_first.unwrap_or(!ob.ascending);
                                return if a_null {
                                    if nf { Ordering::Less } else { Ordering::Greater }
                                } else {
                                    if nf { Ordering::Greater } else { Ordering::Less }
                                };
                            }
                            let ord = va.compare(&vb).unwrap_or(Ordering::Equal);
                            if ord != Ordering::Equal {
                                return if ob.ascending { ord } else { ord.reverse() };
                            }
                        }
                        Ordering::Equal
                    });
                }

                Ok(ExecutionResult::Rows { columns, rows })
            }
            other => Ok(other),
        }
    }

    fn apply_limit(&self, result: ExecutionResult, count: usize, offset: usize) -> Result<ExecutionResult> {
        match result {
            ExecutionResult::Rows { columns, rows } => {
                let rows: Vec<Vec<Value>> = rows.into_iter().skip(offset).take(count).collect();
                Ok(ExecutionResult::Rows { columns, rows })
            }
            other => Ok(other),
        }
    }

    fn apply_top_n(&self, result: ExecutionResult, order_by: &[OrderByExpr], n: usize) -> Result<ExecutionResult> {
        match result {
            ExecutionResult::Rows { columns, rows } => {
                let col_indices: Vec<(usize, bool)> = order_by.iter().filter_map(|o| {
                    if let Expr::Column(name) = &o.expr {
                        columns.iter().position(|c| c == name).map(|i| (i, o.ascending))
                    } else {
                        None
                    }
                }).collect();

                if col_indices.is_empty() {
                    let rows: Vec<Vec<Value>> = rows.into_iter().take(n).collect();
                    return Ok(ExecutionResult::Rows { columns, rows });
                }

                if rows.len() <= n {
                    let mut rows = rows;
                    rows.sort_unstable_by(|a, b| {
                        for &(idx, asc) in &col_indices {
                            let va = a.get(idx).unwrap_or(&Value::Null);
                            let vb = b.get(idx).unwrap_or(&Value::Null);
                            let ord = va.cmp_fast(vb);
                            if ord != Ordering::Equal {
                                return if asc { ord } else { ord.reverse() };
                            }
                        }
                        Ordering::Equal
                    });
                    return Ok(ExecutionResult::Rows { columns, rows });
                }

                if col_indices.len() == 1 {
                    let (idx, asc) = col_indices[0];
                    let all_int = rows.iter().all(|r| matches!(r.get(idx), Some(Value::Int64(_)) | Some(Value::Null)));
                    if all_int {
                        let mut heap: BinaryHeap<(i64, usize)> = BinaryHeap::with_capacity(n + 1);
                        for (i, row) in rows.iter().enumerate() {
                            let k = match row.get(idx) {
                                Some(Value::Int64(v)) => *v,
                                _ => i64::MIN,
                            };
                            let heap_key = if asc { k } else { -k };
                            heap.push((heap_key, i));
                            if heap.len() > n {
                                heap.pop();
                            }
                        }
                        let mut indices: Vec<usize> = heap.into_vec().into_iter().map(|(_, i)| i).collect();
                        indices.sort_unstable_by(|&a, &b| {
                            let ka = match rows[a].get(idx) { Some(Value::Int64(v)) => *v, _ => i64::MIN };
                            let kb = match rows[b].get(idx) { Some(Value::Int64(v)) => *v, _ => i64::MIN };
                            if asc { ka.cmp(&kb) } else { kb.cmp(&ka) }
                        });
                        let mut rows = rows;
                        let result: Vec<Vec<Value>> = indices.into_iter().map(|i| {
                            std::mem::take(&mut rows[i])
                        }).collect();
                        return Ok(ExecutionResult::Rows { columns, rows: result });
                    }
                }

                struct HeapRow {
                    row: Vec<Value>,
                    col_indices: *const Vec<(usize, bool)>,
                }

                impl PartialEq for HeapRow {
                    fn eq(&self, other: &Self) -> bool {
                        self.cmp(other) == Ordering::Equal
                    }
                }
                impl Eq for HeapRow {}

                impl PartialOrd for HeapRow {
                    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                        Some(self.cmp(other))
                    }
                }

                impl Ord for HeapRow {
                    fn cmp(&self, other: &Self) -> Ordering {
                        let indices = unsafe { &*self.col_indices };
                        for &(idx, asc) in indices {
                            let va = self.row.get(idx).unwrap_or(&Value::Null);
                            let vb = other.row.get(idx).unwrap_or(&Value::Null);
                            let ord = va.cmp_fast(vb);
                            if ord != Ordering::Equal {
                                return if asc { ord } else { ord.reverse() };
                            }
                        }
                        Ordering::Equal
                    }
                }

                let indices_ptr: *const Vec<(usize, bool)> = &col_indices;
                let mut heap: BinaryHeap<HeapRow> = BinaryHeap::with_capacity(n + 1);

                for row in rows {
                    heap.push(HeapRow { row, col_indices: indices_ptr });
                    if heap.len() > n {
                        heap.pop();
                    }
                }

                let mut result: Vec<Vec<Value>> = heap.into_iter().map(|hr| hr.row).collect();
                result.sort_unstable_by(|a, b| {
                    for &(idx, asc) in &col_indices {
                        let va = a.get(idx).unwrap_or(&Value::Null);
                        let vb = b.get(idx).unwrap_or(&Value::Null);
                        let ord = va.cmp_fast(vb);
                        if ord != Ordering::Equal {
                            return if asc { ord } else { ord.reverse() };
                        }
                    }
                    Ordering::Equal
                });

                Ok(ExecutionResult::Rows { columns, rows: result })
            }
            other => Ok(other),
        }
    }

    fn exec_hash_join(&self, left: PhysicalPlan, right: PhysicalPlan, condition: Expr, join_type: JoinType, txn_id: Option<TxnId>) -> Result<ExecutionResult> {
        let left_table = Self::plan_table_name(&left);
        let right_table = Self::plan_table_name(&right);
        let left_result = self.execute_physical(left, txn_id)?;
        let right_result = self.execute_physical(right, txn_id)?;

        let (left_cols, left_rows) = match left_result {
            ExecutionResult::Rows { columns, rows } => (columns, rows),
            _ => return Err(QueryError::Execution("JOIN requires row input".into())),
        };
        let (right_cols, right_rows) = match right_result {
            ExecutionResult::Rows { columns, rows } => (columns, rows),
            _ => return Err(QueryError::Execution("JOIN requires row input".into())),
        };

        let (left_key_idx, right_key_idx) = self.extract_join_keys(&condition, &left_cols, &right_cols)?;

        let right_all_int = right_rows.iter().all(|r| matches!(r.get(right_key_idx), Some(Value::Int64(_))));
        let left_all_int = right_all_int && left_rows.iter().all(|r| matches!(r.get(left_key_idx), Some(Value::Int64(_))));

        let mut out_cols = Vec::with_capacity(left_cols.len() + right_cols.len());
        for col in &left_cols {
            if let Some(ref tbl) = left_table {
                out_cols.push(format!("{}.{}", tbl, col));
            } else {
                out_cols.push(col.clone());
            }
        }
        for col in &right_cols {
            if let Some(ref tbl) = right_table {
                out_cols.push(format!("{}.{}", tbl, col));
            } else {
                out_cols.push(col.clone());
            }
        }
        let right_width = right_cols.len();
        let left_width = left_cols.len();
        let mut out_rows: Vec<Vec<Value>> = Vec::with_capacity(left_rows.len());

        let build_inner_on_left = left_all_int
            && matches!(join_type, JoinType::Inner)
            && left_rows.len() < right_rows.len();

        if build_inner_on_left {
            let mut hash_table: HashMap<i64, Vec<usize>, ahash::RandomState> =
                HashMap::with_capacity_and_hasher(left_rows.len(), ahash::RandomState::new());
            self.account_memory((left_rows.len() as u64) * 24)?;
            for (i, row) in left_rows.iter().enumerate() {
                if i % 1024 == 0 { self.poll_ctx()?; }
                if let Some(Value::Int64(k)) = row.get(left_key_idx) {
                    hash_table.entry(*k).or_default().push(i);
                }
            }
            for right_row in &right_rows {
                if let Some(Value::Int64(k)) = right_row.get(right_key_idx) {
                    if let Some(indices) = hash_table.get(k) {
                        for &li in indices {
                            let mut combined = Vec::with_capacity(left_width + right_width);
                            combined.extend(left_rows[li].iter().cloned());
                            combined.extend(right_row.iter().cloned());
                            out_rows.push(combined);
                        }
                    }
                }
            }
        } else if left_all_int {
            let mut hash_table: HashMap<i64, Vec<usize>, ahash::RandomState> =
                HashMap::with_capacity_and_hasher(right_rows.len(), ahash::RandomState::new());
            self.account_memory((right_rows.len() as u64) * 24)?;
            for (i, row) in right_rows.iter().enumerate() {
                if i % 1024 == 0 { self.poll_ctx()?; }
                if let Some(Value::Int64(k)) = row.get(right_key_idx) {
                    hash_table.entry(*k).or_default().push(i);
                }
            }

            match join_type {
                JoinType::Inner => {
                    for left_row in &left_rows {
                        if let Some(Value::Int64(k)) = left_row.get(left_key_idx) {
                            if let Some(indices) = hash_table.get(k) {
                                for &ri in indices {
                                    let mut combined = Vec::with_capacity(left_width + right_width);
                                    combined.extend(left_row.iter().cloned());
                                    combined.extend(right_rows[ri].iter().cloned());
                                    out_rows.push(combined);
                                }
                            }
                        }
                    }
                }
                JoinType::Left => {
                    for left_row in &left_rows {
                        if let Some(Value::Int64(k)) = left_row.get(left_key_idx) {
                            if let Some(indices) = hash_table.get(k) {
                                for &ri in indices {
                                    let mut combined = Vec::with_capacity(left_width + right_width);
                                    combined.extend(left_row.iter().cloned());
                                    combined.extend(right_rows[ri].iter().cloned());
                                    out_rows.push(combined);
                                }
                            } else {
                                let mut combined = Vec::with_capacity(left_width + right_width);
                                combined.extend(left_row.iter().cloned());
                                combined.resize(left_width + right_width, Value::Null);
                                out_rows.push(combined);
                            }
                        }
                    }
                }
                JoinType::Right => {
                    let mut left_hash: HashMap<i64, Vec<usize>, ahash::RandomState> =
                        HashMap::with_capacity_and_hasher(left_rows.len(), ahash::RandomState::new());
                    for (i, row) in left_rows.iter().enumerate() {
                        if let Some(Value::Int64(k)) = row.get(left_key_idx) {
                            left_hash.entry(*k).or_default().push(i);
                        }
                    }
                    for right_row in &right_rows {
                        if let Some(Value::Int64(k)) = right_row.get(right_key_idx) {
                            if let Some(indices) = left_hash.get(k) {
                                for &li in indices {
                                    let mut combined = Vec::with_capacity(left_width + right_width);
                                    combined.extend(left_rows[li].iter().cloned());
                                    combined.extend(right_row.iter().cloned());
                                    out_rows.push(combined);
                                }
                            } else {
                                let mut combined = vec![Value::Null; left_width];
                                combined.extend(right_row.iter().cloned());
                                out_rows.push(combined);
                            }
                        }
                    }
                }
            }
        } else {
            let mut hash_table: HashMap<Vec<u8>, Vec<usize>, ahash::RandomState> =
                HashMap::with_capacity_and_hasher(right_rows.len(), ahash::RandomState::new());
            for (i, row) in right_rows.iter().enumerate() {
                if i % 1024 == 0 { self.poll_ctx()?; }
                let key = self.hash_key(&row[right_key_idx]);
                self.account_memory(key.len() as u64 + 24)?;
                hash_table.entry(key).or_default().push(i);
            }

            match join_type {
                JoinType::Inner => {
                    for left_row in &left_rows {
                        let key = self.hash_key(&left_row[left_key_idx]);
                        if let Some(indices) = hash_table.get(&key) {
                            for &ri in indices {
                                let mut combined = Vec::with_capacity(left_width + right_width);
                                combined.extend(left_row.iter().cloned());
                                combined.extend(right_rows[ri].iter().cloned());
                                out_rows.push(combined);
                            }
                        }
                    }
                }
                JoinType::Left => {
                    for left_row in &left_rows {
                        let key = self.hash_key(&left_row[left_key_idx]);
                        if let Some(indices) = hash_table.get(&key) {
                            for &ri in indices {
                                let mut combined = Vec::with_capacity(left_width + right_width);
                                combined.extend(left_row.iter().cloned());
                                combined.extend(right_rows[ri].iter().cloned());
                                out_rows.push(combined);
                            }
                        } else {
                            let mut combined = Vec::with_capacity(left_width + right_width);
                            combined.extend(left_row.iter().cloned());
                            combined.resize(left_width + right_width, Value::Null);
                            out_rows.push(combined);
                        }
                    }
                }
                JoinType::Right => {
                    let mut left_hash: HashMap<Vec<u8>, Vec<usize>, ahash::RandomState> =
                        HashMap::with_capacity_and_hasher(left_rows.len(), ahash::RandomState::new());
                    for (i, row) in left_rows.iter().enumerate() {
                        let key = self.hash_key(&row[left_key_idx]);
                        left_hash.entry(key).or_default().push(i);
                    }
                    for right_row in &right_rows {
                        let key = self.hash_key(&right_row[right_key_idx]);
                        if let Some(indices) = left_hash.get(&key) {
                            for &li in indices {
                                let mut combined = Vec::with_capacity(left_width + right_width);
                                combined.extend(left_rows[li].iter().cloned());
                                combined.extend(right_row.iter().cloned());
                                out_rows.push(combined);
                            }
                        } else {
                            let mut combined = vec![Value::Null; left_width];
                            combined.extend(right_row.iter().cloned());
                            out_rows.push(combined);
                        }
                    }
                }
            }
        }

        Ok(ExecutionResult::Rows { columns: out_cols, rows: out_rows })
    }

    fn rewrite_aliases(stmt: Statement) -> Statement {
        match stmt {
            Statement::Select(mut select) => {
                let mut alias_map: HashMap<String, String> = HashMap::new();
                if let FromClause::Table(ref table) = select.from {
                    if let Some(ref alias) = select.from_alias {
                        alias_map.insert(alias.clone(), table.clone());
                    }
                }
                for join in &select.joins {
                    if let Some(ref alias) = join.alias {
                        alias_map.insert(alias.clone(), join.table.clone());
                    }
                }
                if alias_map.is_empty() {
                    return Statement::Select(select);
                }
                select.columns = select.columns.into_iter().map(|c| match c {
                    SelectColumn::Expr(expr, alias) => SelectColumn::Expr(Self::rewrite_expr_aliases(&expr, &alias_map), alias),
                    other => other,
                }).collect();
                select.where_clause = select.where_clause.map(|e| Self::rewrite_expr_aliases(&e, &alias_map));
                select.order_by = select.order_by.into_iter().map(|mut o| {
                    o.expr = Self::rewrite_expr_aliases(&o.expr, &alias_map);
                    o
                }).collect();
                select.group_by = select.group_by.into_iter().map(|e| Self::rewrite_expr_aliases(&e, &alias_map)).collect();
                select.having = select.having.map(|e| Self::rewrite_expr_aliases(&e, &alias_map));
                select.joins = select.joins.into_iter().map(|mut j| {
                    j.condition = Self::rewrite_expr_aliases(&j.condition, &alias_map);
                    j
                }).collect();
                Statement::Select(select)
            }
            other => other,
        }
    }

    fn rewrite_expr_aliases(expr: &Expr, alias_map: &HashMap<String, String>) -> Expr {
        match expr {
            Expr::QualifiedColumn(table, col) => {
                if let Some(real_table) = alias_map.get(table) {
                    Expr::QualifiedColumn(real_table.clone(), col.clone())
                } else {
                    expr.clone()
                }
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::rewrite_expr_aliases(left, alias_map)),
                op: *op,
                right: Box::new(Self::rewrite_expr_aliases(right, alias_map)),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(Self::rewrite_expr_aliases(inner, alias_map)),
            },
            Expr::Function { name, args } => Expr::Function {
                name: name.clone(),
                args: args.iter().map(|a| Self::rewrite_expr_aliases(a, alias_map)).collect(),
            },
            Expr::IsNull(inner) => Expr::IsNull(Box::new(Self::rewrite_expr_aliases(inner, alias_map))),
            Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(Self::rewrite_expr_aliases(inner, alias_map))),
            Expr::InList { expr: inner, list } => Expr::InList {
                expr: Box::new(Self::rewrite_expr_aliases(inner, alias_map)),
                list: list.iter().map(|e| Self::rewrite_expr_aliases(e, alias_map)).collect(),
            },
            Expr::Between { expr: inner, low, high } => Expr::Between {
                expr: Box::new(Self::rewrite_expr_aliases(inner, alias_map)),
                low: Box::new(Self::rewrite_expr_aliases(low, alias_map)),
                high: Box::new(Self::rewrite_expr_aliases(high, alias_map)),
            },
            Expr::Case { operand, when_clauses, else_result } => Expr::Case {
                operand: operand.as_ref().map(|o| Box::new(Self::rewrite_expr_aliases(o, alias_map))),
                when_clauses: when_clauses.iter().map(|(w, t)| (Self::rewrite_expr_aliases(w, alias_map), Self::rewrite_expr_aliases(t, alias_map))).collect(),
                else_result: else_result.as_ref().map(|e| Box::new(Self::rewrite_expr_aliases(e, alias_map))),
            },
            Expr::Cast { expr: inner, data_type } => Expr::Cast {
                expr: Box::new(Self::rewrite_expr_aliases(inner, alias_map)),
                data_type: *data_type,
            },
            Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists(_) | Expr::WindowFunction { .. } | Expr::Default | Expr::Interval(_) => expr.clone(),
            Expr::Literal(_) | Expr::Column(_) | Expr::JsonPath { .. } => expr.clone(),
        }
    }

    fn plan_table_name(plan: &PhysicalPlan) -> Option<String> {
        match plan {
            PhysicalPlan::SeqScan { table, .. } => Some(table.clone()),
            PhysicalPlan::IndexScan { table, .. } => Some(table.clone()),
            PhysicalPlan::Filter { input, .. } => Self::plan_table_name(input),
            PhysicalPlan::Sort { input, .. } => Self::plan_table_name(input),
            PhysicalPlan::Limit { input, .. } => Self::plan_table_name(input),
            PhysicalPlan::Distinct { input, .. } => Self::plan_table_name(input),
            _ => None,
        }
    }

    fn extract_join_keys(&self, condition: &Expr, left_cols: &[String], right_cols: &[String]) -> Result<(usize, usize)> {
        match condition {
            Expr::BinaryOp { left, op: BinOp::Eq, right } => {
                let left_name = self.expr_column_name(left);
                let right_name = self.expr_column_name(right);

                if let (Some(ln), Some(rn)) = (left_name, right_name) {
                    if let Some(li) = left_cols.iter().position(|c| c == &ln || ln.ends_with(&format!(".{}", c))) {
                        if let Some(ri) = right_cols.iter().position(|c| c == &rn || rn.ends_with(&format!(".{}", c))) {
                            return Ok((li, ri));
                        }
                    }
                    if let Some(li) = left_cols.iter().position(|c| c == &rn || rn.ends_with(&format!(".{}", c))) {
                        if let Some(ri) = right_cols.iter().position(|c| c == &ln || ln.ends_with(&format!(".{}", c))) {
                            return Ok((li, ri));
                        }
                    }
                }
                Err(QueryError::Execution("Cannot determine join keys from condition".into()))
            }
            _ => Err(QueryError::Execution("JOIN condition must be an equality".into())),
        }
    }

    fn expr_column_name(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Column(name) => Some(name.clone()),
            Expr::QualifiedColumn(_, name) => Some(name.clone()),
            _ => None,
        }
    }

    fn hash_key(&self, value: &Value) -> Vec<u8> {
        bytedb_core::index::order_key::encode_okey(&[value])
    }

    fn exec_hash_aggregate(&self, input: PhysicalPlan, group_by: Vec<Expr>, aggregates: Vec<Expr>, having: Option<Expr>, txn_id: Option<TxnId>) -> Result<ExecutionResult> {
        let input_result = self.execute_physical(input, txn_id)?;
        let (col_names, rows) = match input_result {
            ExecutionResult::Rows { columns, rows } => (columns, rows),
            _ => return Err(QueryError::Execution("Aggregate requires row input".into())),
        };

        let group_indices: Vec<usize> = group_by.iter().filter_map(|expr| {
            if let Expr::Column(name) = expr {
                col_names.iter().position(|c| c == name)
            } else {
                None
            }
        }).collect();

        let agg_functions: Vec<&Expr> = aggregates.iter().filter(|e| matches!(e, Expr::Function { .. })).collect();

        let agg_col_indices: Vec<Option<usize>> = agg_functions.iter().map(|expr| {
            if let Expr::Function { args, .. } = expr {
                if args.is_empty() { None }
                else if let Expr::Column(name) = &args[0] {
                    col_names.iter().position(|c| c == name)
                } else { None }
            } else { None }
        }).collect();

        let mut groups: HashMap<Vec<u8>, Vec<usize>, ahash::RandomState> =
            HashMap::with_capacity_and_hasher(rows.len().min(1024), ahash::RandomState::new());
        let mut keybuf = Vec::with_capacity(group_indices.len() * 16);
        for (row_idx, row) in rows.iter().enumerate() {
            if row_idx % 1024 == 0 { self.poll_ctx()?; }
            keybuf.clear();
            for &i in &group_indices {
                let part = self.hash_key(row.get(i).unwrap_or(&Value::Null));
                keybuf.extend_from_slice(&(part.len() as u32).to_le_bytes());
                keybuf.extend_from_slice(&part);
            }
            if let Some(v) = groups.get_mut(&keybuf) {
                v.push(row_idx);
            } else {
                self.account_memory(keybuf.len() as u64 + 24)?;
                groups.insert(keybuf.clone(), vec![row_idx]);
            }
        }

        let mut out_col_names = Vec::with_capacity(group_indices.len() + agg_functions.len());
        for expr in &group_by {
            if let Expr::Column(name) = expr {
                out_col_names.push(name.clone());
            }
        }
        for expr in &agg_functions {
            if let Expr::Function { name, args } = expr {
                let arg_name = if args.is_empty() { "*".to_string() } else {
                    match &args[0] {
                        Expr::Column(c) => c.clone(),
                        _ => "*".to_string(),
                    }
                };
                out_col_names.push(format!("{}({})", name.to_lowercase(), arg_name));
            }
        }

        let mut out_rows: Vec<Vec<Value>> = Vec::with_capacity(groups.len());
        for (_key, row_indices) in &groups {
            let mut row_out = Vec::with_capacity(group_indices.len() + agg_functions.len());

            for &gi in &group_indices {
                row_out.push(rows[row_indices[0]].get(gi).cloned().unwrap_or(Value::Null));
            }

            for (fi, expr) in agg_functions.iter().enumerate() {
                if let Expr::Function { name, .. } = expr {
                    let agg_val = self.compute_aggregate_fast(name, agg_col_indices[fi], row_indices, &rows);
                    row_out.push(agg_val);
                }
            }

            out_rows.push(row_out);
        }

        if let Some(ref having_expr) = having {
            let having_schema = Schema::new("", out_col_names.iter().map(|c| Column::new(c.clone(), DataType::Text)).collect());
            out_rows.retain(|row| {
                let tuple = Tuple::new(row.clone());
                self.eval_predicate(having_expr, &tuple, &having_schema)
            });
        }

        Ok(ExecutionResult::Rows { columns: out_col_names, rows: out_rows })
    }

    fn try_fast_aggregate(&self, table_data: &TableData, col_names: &[String], group_by: &[Expr], aggregates: &[Expr]) -> Option<ExecutionResult> {
        let group_indices: Vec<usize> = group_by.iter().filter_map(|expr| {
            if let Expr::Column(name) = expr {
                col_names.iter().position(|c| c == name)
            } else {
                None
            }
        }).collect();

        if group_indices.len() != group_by.len() { return None; }

        let agg_functions: Vec<(&str, Option<usize>)> = aggregates.iter().filter_map(|e| {
            if let Expr::Function { name, args } = e {
                let col_idx = if args.is_empty() { None }
                else if let Expr::Column(cname) = &args[0] {
                    col_names.iter().position(|c| c == cname)
                } else { None };
                Some((name.as_str(), col_idx))
            } else { None }
        }).collect();

        let all_int_aggs = agg_functions.iter().all(|(name, col_idx)| {
            let n = name.to_uppercase();
            n == "COUNT" || col_idx.is_some()
        });
        if !all_int_aggs { return None; }

        if agg_functions.iter().any(|(name, _)| name.to_uppercase() == "COUNT_DISTINCT") {
            return None;
        }

        struct GroupAcc {
            count: i64,
            sum: i128,
            min: i64,
            max: i64,
            group_val: Value,
        }

        let mut groups: HashMap<Vec<u8>, GroupAcc> = HashMap::new();

        let _ = table_data.index.for_each(|_key, data| {

            let group_key_bytes: Vec<u8> = group_indices.iter().filter_map(|&gi| {
                let pos = bytedb_core::tuple::tuple::column_offset(data, gi)?;
                let next_pos = bytedb_core::tuple::tuple::column_offset(data, gi + 1)
                    .unwrap_or(data.len());
                Some(data[pos..next_pos].to_vec())
            }).flatten().collect();

            let agg_val = agg_functions.iter().find_map(|(name, col_idx)| {
                let n = name.to_uppercase();
                if n != "COUNT" {
                    col_idx.and_then(|ci| read_int64_at(data, ci))
                } else { None }
            });

            let entry = groups.entry(group_key_bytes).or_insert_with(|| {
                let gv = if group_indices.len() == 1 {
                    read_value_at(data, group_indices[0]).unwrap_or(Value::Null)
                } else {
                    Value::Null
                };
                GroupAcc { count: 0, sum: 0i128, min: i64::MAX, max: i64::MIN, group_val: gv }
            });
            entry.count += 1;
            if let Some(v) = agg_val {
                entry.sum += v as i128;
                if v < entry.min { entry.min = v; }
                if v > entry.max { entry.max = v; }
            }
            true
        });

        let mut out_col_names = Vec::with_capacity(group_indices.len() + agg_functions.len());
        for expr in group_by {
            if let Expr::Column(name) = expr { out_col_names.push(name.clone()); }
        }
        for (name, args_col) in &agg_functions {
            let arg_name = args_col.map(|i| col_names[i].clone()).unwrap_or_else(|| "*".to_string());
            out_col_names.push(format!("{}({})", name.to_lowercase(), arg_name));
        }

        let mut out_rows: Vec<Vec<Value>> = Vec::with_capacity(groups.len());
        for (_, acc) in groups {
            let mut row = Vec::with_capacity(out_col_names.len());
            row.push(acc.group_val);

            for (name, _) in &agg_functions {
                let n = name.to_uppercase();
                let val = match n.as_str() {
                    "COUNT" => Value::Int64(acc.count),
                    "SUM" => int128_to_value(acc.sum),
                    "AVG" => if acc.count > 0 { Value::Float64(acc.sum as f64 / acc.count as f64) } else { Value::Null },
                    "MIN" => if acc.min == i64::MAX { Value::Null } else { Value::Int64(acc.min) },
                    "MAX" => if acc.max == i64::MIN { Value::Null } else { Value::Int64(acc.max) },
                    _ => Value::Null,
                };
                row.push(val);
            }
            out_rows.push(row);
        }

        Some(ExecutionResult::Rows { columns: out_col_names, rows: out_rows })
    }

    fn compute_aggregate_fast(&self, func_name: &str, col_idx: Option<usize>, row_indices: &[usize], rows: &[Vec<Value>]) -> Value {
        match func_name.to_uppercase().as_str() {
            "COUNT" => {
                if let Some(idx) = col_idx {
                    let count = row_indices.iter().filter(|&&ri| {
                        rows[ri].get(idx).map(|v| !v.is_null()).unwrap_or(false)
                    }).count();
                    Value::Int64(count as i64)
                } else {
                    Value::Int64(row_indices.len() as i64)
                }
            }
            "COUNT_DISTINCT" => {
                let idx = match col_idx {
                    Some(i) => i,
                    None => return Value::Int64(row_indices.len() as i64),
                };
                let mut seen = std::collections::HashSet::new();
                for &ri in row_indices {
                    if let Some(val) = rows[ri].get(idx) {
                        if val.is_null() { continue; }
                        seen.insert(self.hash_key(val));
                    }
                }
                Value::Int64(seen.len() as i64)
            }
            "SUM" => {
                let idx = match col_idx {
                    Some(i) => i,
                    None => return Value::Null,
                };
                let mut sum = 0i128;
                let mut is_float = false;
                let mut fsum = 0.0f64;
                for &ri in row_indices {
                    match rows[ri].get(idx) {
                        Some(Value::Int64(n)) => sum += *n as i128,
                        Some(Value::Float64(f)) => { is_float = true; fsum += f; }
                        _ => {}
                    }
                }
                if is_float { Value::Float64(fsum + sum as f64) } else { int128_to_value(sum) }
            }
            "AVG" => {
                let idx = match col_idx {
                    Some(i) => i,
                    None => return Value::Null,
                };
                let mut sum = 0.0f64;
                let mut count = 0i64;
                for &ri in row_indices {
                    match rows[ri].get(idx) {
                        Some(Value::Int64(n)) => { sum += *n as f64; count += 1; }
                        Some(Value::Float64(f)) => { sum += f; count += 1; }
                        _ => {}
                    }
                }
                if count > 0 { Value::Float64(sum / count as f64) } else { Value::Null }
            }
            "MIN" => {
                let idx = match col_idx {
                    Some(i) => i,
                    None => return Value::Null,
                };
                let mut min: Option<&Value> = None;
                for &ri in row_indices {
                    if let Some(val) = rows[ri].get(idx) {
                        if val.is_null() { continue; }
                        min = Some(match min {
                            None => val,
                            Some(current) => {
                                if val.cmp_fast(current) == Ordering::Less { val } else { current }
                            }
                        });
                    }
                }
                min.cloned().unwrap_or(Value::Null)
            }
            "MAX" => {
                let idx = match col_idx {
                    Some(i) => i,
                    None => return Value::Null,
                };
                let mut max: Option<&Value> = None;
                for &ri in row_indices {
                    if let Some(val) = rows[ri].get(idx) {
                        if val.is_null() { continue; }
                        max = Some(match max {
                            None => val,
                            Some(current) => {
                                if val.cmp_fast(current) == Ordering::Greater { val } else { current }
                            }
                        });
                    }
                }
                max.cloned().unwrap_or(Value::Null)
            }
            _ => Value::Null,
        }
    }

    fn eval_insert_value(&self, expr: &Expr, schema: &Schema, col_idx: usize) -> Value {
        match expr {
            Expr::Literal(LiteralValue::Integer(n)) => Value::Int64(*n),
            Expr::Literal(LiteralValue::Float(f)) => Value::Float64(*f),
            Expr::Literal(LiteralValue::String(s)) => Value::Text(s.clone()),
            Expr::Literal(LiteralValue::Bool(b)) => Value::Bool(*b),
            Expr::Literal(LiteralValue::Null) => Value::Null,
            Expr::Literal(LiteralValue::HexBlob(bytes)) => Value::Bytes(bytes.clone()),
            Expr::Default => {
                schema.columns.get(col_idx)
                    .and_then(|c| c.default.clone())
                    .unwrap_or(Value::Null)
            }
            _ => Value::Null,
        }
    }

    fn eval_expr_simple(&self, expr: &Expr) -> Value {
        match expr {
            Expr::Literal(LiteralValue::Integer(n)) => Value::Int64(*n),
            Expr::Literal(LiteralValue::Float(f)) => Value::Float64(*f),
            Expr::Literal(LiteralValue::String(s)) => Value::Text(s.clone()),
            Expr::Literal(LiteralValue::Bool(b)) => Value::Bool(*b),
            Expr::Literal(LiteralValue::Null) => Value::Null,
            _ => Value::Null,
        }
    }

    fn eval_predicate(&self, expr: &Expr, tuple: &Tuple, schema: &Schema) -> bool {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinOp::And => {
                        self.eval_predicate(left, tuple, schema) && self.eval_predicate(right, tuple, schema)
                    }
                    BinOp::Or => {
                        self.eval_predicate(left, tuple, schema) || self.eval_predicate(right, tuple, schema)
                    }
                    _ => {
                        let lval = self.eval_value(left, tuple, schema);
                        let rval = self.eval_value(right, tuple, schema);
                        match op {
                            BinOp::Eq => lval.compare(&rval) == Some(std::cmp::Ordering::Equal),
                            BinOp::Neq => lval.compare(&rval) != Some(std::cmp::Ordering::Equal),
                            BinOp::Lt => lval.compare(&rval) == Some(std::cmp::Ordering::Less),
                            BinOp::Gt => lval.compare(&rval) == Some(std::cmp::Ordering::Greater),
                            BinOp::Lte => matches!(lval.compare(&rval), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)),
                            BinOp::Gte => matches!(lval.compare(&rval), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)),
                            BinOp::Like => match (&lval, &rval) {
                                (Value::Text(s), Value::Text(pattern)) => like_match(s, pattern),
                                _ => false,
                            },
                            BinOp::Ilike => match (&lval, &rval) {
                                (Value::Text(s), Value::Text(pattern)) => ilike_match(s, pattern),
                                _ => false,
                            },
                            _ => false,
                        }
                    }
                }
            }
            Expr::IsNull(inner) => {
                let val = self.eval_value(inner, tuple, schema);
                val.is_null()
            }
            Expr::IsNotNull(inner) => {
                let val = self.eval_value(inner, tuple, schema);
                !val.is_null()
            }
            _ => {
                match self.eval_value(expr, tuple, schema) {
                    Value::Bool(b) => b,
                    Value::Null => false,
                    _ => true,
                }
            }
        }
    }

    fn eval_value(&self, expr: &Expr, tuple: &Tuple, schema: &Schema) -> Value {
        match expr {
            Expr::Column(name) => {
                if let Some(idx) = schema.column_index(name) {
                    tuple.get(idx).cloned().unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            }
            Expr::QualifiedColumn(table, col) => {
                if let Some(idx) = schema.column_index(col) {
                    tuple.get(idx).cloned().unwrap_or(Value::Null)
                } else {
                    let qualified = format!("{}.{}", table, col);
                    if let Some(idx) = schema.column_index(&qualified) {
                        tuple.get(idx).cloned().unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    }
                }
            }
            Expr::Literal(lit) => match lit {
                LiteralValue::Integer(n) => Value::Int64(*n),
                LiteralValue::Float(f) => Value::Float64(*f),
                LiteralValue::String(s) => Value::Text(s.clone()),
                LiteralValue::Bool(b) => Value::Bool(*b),
                LiteralValue::Null => Value::Null,
                LiteralValue::HexBlob(bytes) => Value::Bytes(bytes.clone()),
            },
            Expr::Interval(s) => Value::Interval(parse_interval(s).unwrap_or(0)),
            Expr::BinaryOp { left, op, right } => {
                let lval = self.eval_value(left, tuple, schema);
                let rval = self.eval_value(right, tuple, schema);
                match op {
                    BinOp::Plus => match (&lval, &rval) {
                        (Value::Int64(a), Value::Int64(b)) => Value::Int64(a + b),
                        (Value::Float64(a), Value::Float64(b)) => Value::Float64(a + b),
                        (Value::Int64(a), Value::Float64(b)) => Value::Float64(*a as f64 + b),
                        (Value::Float64(a), Value::Int64(b)) => Value::Float64(a + *b as f64),
                        (Value::Text(a), Value::Text(b)) => Value::Text(format!("{}{}", a, b)),
                        (Value::Timestamp(t), Value::Interval(i)) => Value::Timestamp(*t + i),
                        (Value::Interval(i), Value::Timestamp(t)) => Value::Timestamp(*t + i),
                        (Value::Date(d), Value::Interval(i)) => Value::Date(*d + *i as i32),
                        (Value::Interval(i), Value::Date(d)) => Value::Date(*d + *i as i32),
                        (Value::Interval(a), Value::Interval(b)) => Value::Interval(*a + *b),
                        _ => Value::Null,
                    },
                    BinOp::Minus => match (&lval, &rval) {
                        (Value::Int64(a), Value::Int64(b)) => Value::Int64(a - b),
                        (Value::Float64(a), Value::Float64(b)) => Value::Float64(a - b),
                        (Value::Int64(a), Value::Float64(b)) => Value::Float64(*a as f64 - b),
                        (Value::Float64(a), Value::Int64(b)) => Value::Float64(a - *b as f64),
                        (Value::Timestamp(t), Value::Interval(i)) => Value::Timestamp(*t - i),
                        (Value::Date(d), Value::Interval(i)) => Value::Date(*d - *i as i32),
                        (Value::Interval(a), Value::Interval(b)) => Value::Interval(*a - *b),
                        _ => Value::Null,
                    },
                    BinOp::Mul => match (&lval, &rval) {
                        (Value::Int64(a), Value::Int64(b)) => Value::Int64(a * b),
                        (Value::Float64(a), Value::Float64(b)) => Value::Float64(a * b),
                        (Value::Int64(a), Value::Float64(b)) => Value::Float64(*a as f64 * b),
                        (Value::Float64(a), Value::Int64(b)) => Value::Float64(a * *b as f64),
                        (Value::Interval(i), Value::Int64(n)) => Value::Interval(*i * n),
                        (Value::Int64(n), Value::Interval(i)) => Value::Interval(*i * n),
                        _ => Value::Null,
                    },
                    BinOp::Div => match (&lval, &rval) {
                        (Value::Int64(a), Value::Int64(b)) if *b != 0 => Value::Int64(a / b),
                        (Value::Float64(a), Value::Float64(b)) if *b != 0.0 => Value::Float64(a / b),
                        (Value::Int64(a), Value::Float64(b)) if *b != 0.0 => Value::Float64(*a as f64 / b),
                        (Value::Float64(a), Value::Int64(b)) if *b != 0 => Value::Float64(a / *b as f64),
                        (Value::Interval(i), Value::Int64(n)) if *n != 0 => Value::Interval(*i / n),
                        _ => Value::Null,
                    },
                    BinOp::Mod => match (&lval, &rval) {
                        (Value::Int64(a), Value::Int64(b)) if *b != 0 => Value::Int64(a % b),
                        (Value::Float64(a), Value::Float64(b)) if *b != 0.0 => Value::Float64(a % b),
                        _ => Value::Null,
                    },
                    BinOp::Eq => Value::Bool(lval.compare(&rval) == Some(std::cmp::Ordering::Equal)),
                    BinOp::Neq => Value::Bool(lval.compare(&rval) != Some(std::cmp::Ordering::Equal)),
                    BinOp::Lt => Value::Bool(lval.compare(&rval) == Some(std::cmp::Ordering::Less)),
                    BinOp::Gt => Value::Bool(lval.compare(&rval) == Some(std::cmp::Ordering::Greater)),
                    BinOp::Lte => Value::Bool(matches!(lval.compare(&rval), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal))),
                    BinOp::Gte => Value::Bool(matches!(lval.compare(&rval), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))),
                    BinOp::And => match (&lval, &rval) {
                        (Value::Bool(a), Value::Bool(b)) => Value::Bool(*a && *b),
                        _ => Value::Null,
                    },
                    BinOp::Or => match (&lval, &rval) {
                        (Value::Bool(a), Value::Bool(b)) => Value::Bool(*a || *b),
                        _ => Value::Null,
                    },
                    BinOp::Like => match (&lval, &rval) {
                        (Value::Text(s), Value::Text(pattern)) => {
                            Value::Bool(like_match(s, pattern))
                        }
                        _ => Value::Null,
                    },
                    BinOp::Ilike => match (&lval, &rval) {
                        (Value::Text(s), Value::Text(pattern)) => {
                            Value::Bool(ilike_match(s, pattern))
                        }
                        _ => Value::Null,
                    },
                }
            }
            Expr::UnaryOp { op, expr } => {
                let val = self.eval_value(expr, tuple, schema);
                match op {
                    UnaryOp::Neg => match val {
                        Value::Int64(n) => Value::Int64(-n),
                        Value::Float64(f) => Value::Float64(-f),
                        _ => Value::Null,
                    },
                    UnaryOp::Not => match val {
                        Value::Bool(b) => Value::Bool(!b),
                        _ => Value::Null,
                    },
                }
            }
            Expr::Function { name, args } => {
                let upper_name = name.to_uppercase();
                match upper_name.as_str() {
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => {
                        let arg_name = if args.is_empty() { "*".to_string() } else {
                            match &args[0] {
                                Expr::Column(c) => c.clone(),
                                _ => "*".to_string(),
                            }
                        };
                        let func_col = format!("{}({})", name.to_lowercase(), arg_name);
                        if let Some(idx) = schema.column_index(&func_col) {
                            return tuple.get(idx).cloned().unwrap_or(Value::Null);
                        }
                    }
                    _ => {}
                }
                match upper_name.as_str() {
                    "COALESCE" => {
                        for arg in args {
                            let val = self.eval_value(arg, tuple, schema);
                            if !val.is_null() {
                                return val;
                            }
                        }
                        Value::Null
                    }
                    "NULLIF" => {
                        if args.len() == 2 {
                            let a = self.eval_value(&args[0], tuple, schema);
                            let b = self.eval_value(&args[1], tuple, schema);
                            if a.compare(&b) == Some(std::cmp::Ordering::Equal) {
                                Value::Null
                            } else {
                                a
                            }
                        } else {
                            Value::Null
                        }
                    }
                    "GREATEST" => {
                        let mut best: Option<Value> = None;
                        for arg in args {
                            let val = self.eval_value(arg, tuple, schema);
                            if val.is_null() { continue; }
                            best = Some(match best {
                                None => val,
                                Some(b) => if val.compare(&b) == Some(std::cmp::Ordering::Greater) { val } else { b },
                            });
                        }
                        best.unwrap_or(Value::Null)
                    }
                    "LEAST" => {
                        let mut best: Option<Value> = None;
                        for arg in args {
                            let val = self.eval_value(arg, tuple, schema);
                            if val.is_null() { continue; }
                            best = Some(match best {
                                None => val,
                                Some(b) => if val.compare(&b) == Some(std::cmp::Ordering::Less) { val } else { b },
                            });
                        }
                        best.unwrap_or(Value::Null)
                    }
                    "UPPER" => {
                        if let Some(arg) = args.first() {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Text(s) => Value::Text(s.to_uppercase()),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "LOWER" => {
                        if let Some(arg) = args.first() {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Text(s) => Value::Text(s.to_lowercase()),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "BLOB_SIZE" => {
                        if let Some(arg) = args.first() {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Bytes(b) => Value::Int64(b.len() as i64),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "BLOB" => {
                        if let Some(arg) = args.first() {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Text(s) => Value::Bytes(s.into_bytes()),
                                Value::Bytes(b) => Value::Bytes(b),
                                Value::Null => Value::Null,
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "LENGTH" => {
                        if let Some(arg) = args.first() {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Text(s) => Value::Int64(s.chars().count() as i64),
                                Value::Bytes(b) => Value::Int64(b.len() as i64),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "TRIM" => {
                        if let Some(arg) = args.first() {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Text(s) => Value::Text(s.trim().to_string()),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "CONCAT" => {
                        let mut result = String::new();
                        for arg in args {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Text(s) => result.push_str(&s),
                                Value::Int64(n) => result.push_str(&n.to_string()),
                                Value::Float64(f) => result.push_str(&f.to_string()),
                                Value::Bool(b) => result.push_str(&b.to_string()),
                                _ => {}
                            }
                        }
                        Value::Text(result)
                    }
                    "CONCAT_WS" => {
                        if args.is_empty() { return Value::Null; }
                        let sep = match self.eval_value(&args[0], tuple, schema) {
                            Value::Text(s) => s,
                            _ => return Value::Null,
                        };
                        let parts: Vec<String> = args[1..].iter().filter_map(|a| {
                            match self.eval_value(a, tuple, schema) {
                                Value::Null => None,
                                Value::Text(s) => Some(s),
                                Value::Int64(n) => Some(n.to_string()),
                                Value::Float64(f) => Some(f.to_string()),
                                Value::Bool(b) => Some(b.to_string()),
                                _ => None,
                            }
                        }).collect();
                        Value::Text(parts.join(&sep))
                    }
                    "SPLIT_PART" => {
                        if args.len() == 3 {
                            let s = self.eval_value(&args[0], tuple, schema);
                            let sep = self.eval_value(&args[1], tuple, schema);
                            let n = self.eval_value(&args[2], tuple, schema);
                            match (s, sep, n) {
                                (Value::Text(s), Value::Text(sep), Value::Int64(n)) => {
                                    let parts: Vec<&str> = s.split(&sep as &str).collect();
                                    let idx = (n - 1) as usize;
                                    Value::Text(parts.get(idx).map(|p| p.to_string()).unwrap_or_default())
                                }
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "STARTS_WITH" => {
                        if args.len() == 2 {
                            let s = self.eval_value(&args[0], tuple, schema);
                            let prefix = self.eval_value(&args[1], tuple, schema);
                            match (s, prefix) {
                                (Value::Text(s), Value::Text(p)) => Value::Bool(s.starts_with(&p)),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "ENDS_WITH" => {
                        if args.len() == 2 {
                            let s = self.eval_value(&args[0], tuple, schema);
                            let suffix = self.eval_value(&args[1], tuple, schema);
                            match (s, suffix) {
                                (Value::Text(s), Value::Text(p)) => Value::Bool(s.ends_with(&p)),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "CONTAINS" => {
                        if args.len() == 2 {
                            let s = self.eval_value(&args[0], tuple, schema);
                            let needle = self.eval_value(&args[1], tuple, schema);
                            match (s, needle) {
                                (Value::Text(s), Value::Text(n)) => Value::Bool(s.contains(&n)),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "REPLACE" => {
                        if args.len() == 3 {
                            let s = self.eval_value(&args[0], tuple, schema);
                            let from = self.eval_value(&args[1], tuple, schema);
                            let to = self.eval_value(&args[2], tuple, schema);
                            match (s, from, to) {
                                (Value::Text(s), Value::Text(f), Value::Text(t)) => Value::Text(s.replace(&f, &t)),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "SUBSTRING" | "SUBSTR" => {
                        if args.len() >= 2 {
                            let s = self.eval_value(&args[0], tuple, schema);
                            let start = self.eval_value(&args[1], tuple, schema);
                            let len = if args.len() >= 3 { Some(self.eval_value(&args[2], tuple, schema)) } else { None };
                            match (s, start) {
                                (Value::Text(s), Value::Int64(start)) => {
                                    let start = (start.max(1) - 1) as usize;
                                    let result: String = match len {
                                        Some(Value::Int64(l)) => s.chars().skip(start).take(l.max(0) as usize).collect(),
                                        _ => s.chars().skip(start).collect(),
                                    };
                                    Value::Text(result)
                                }
                                (Value::Bytes(b), Value::Int64(start)) => {
                                    let start = (start.max(1) - 1) as usize;
                                    let result: Vec<u8> = match len {
                                        Some(Value::Int64(l)) => b.into_iter().skip(start).take(l.max(0) as usize).collect(),
                                        _ => b.into_iter().skip(start).collect(),
                                    };
                                    Value::Bytes(result)
                                }
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "ABS" => {
                        if let Some(arg) = args.first() {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Int64(n) => Value::Int64(n.abs()),
                                Value::Float64(f) => Value::Float64(f.abs()),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "CEIL" | "CEILING" => {
                        if let Some(arg) = args.first() {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Float64(f) => Value::Float64(f.ceil()),
                                Value::Int64(n) => Value::Int64(n),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "FLOOR" => {
                        if let Some(arg) = args.first() {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Float64(f) => Value::Float64(f.floor()),
                                Value::Int64(n) => Value::Int64(n),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "ROUND" => {
                        if let Some(arg) = args.first() {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Float64(f) => Value::Float64(f.round()),
                                Value::Int64(n) => Value::Int64(n),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "POWER" | "POW" => {
                        if args.len() == 2 {
                            let base = self.eval_value(&args[0], tuple, schema);
                            let exp = self.eval_value(&args[1], tuple, schema);
                            match (base, exp) {
                                (Value::Float64(b), Value::Float64(e)) => Value::Float64(b.powf(e)),
                                (Value::Int64(b), Value::Int64(e)) => Value::Float64((b as f64).powf(e as f64)),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "SQRT" => {
                        if let Some(arg) = args.first() {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Float64(f) if f >= 0.0 => Value::Float64(f.sqrt()),
                                Value::Int64(n) if n >= 0 => Value::Float64((n as f64).sqrt()),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "MOD" => {
                        if args.len() == 2 {
                            let a = self.eval_value(&args[0], tuple, schema);
                            let b = self.eval_value(&args[1], tuple, schema);
                            match (a, b) {
                                (Value::Int64(x), Value::Int64(y)) if y != 0 => Value::Int64(x % y),
                                (Value::Float64(x), Value::Float64(y)) if y != 0.0 => Value::Float64(x % y),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "NOW" | "CURRENT_TIMESTAMP" => {
                        Value::Timestamp(now_micros())
                    }
                    "CURRENT_DATE" => {
                        Value::Date(today_days())
                    }
                    "CURRENT_TIME" => {
                        Value::Text(chrono_now())
                    }
                    "EXTRACT" => {
                        if args.len() == 2 {
                            let field = match &args[0] {
                                Expr::Column(name) => name.to_uppercase(),
                                _ => return Value::Null,
                            };
                            let val = self.eval_value(&args[1], tuple, schema);
                            extract_from_timestamp(&field, &val)
                        } else { Value::Null }
                    }
                    "POSITION" | "STRPOS" => {
                        if args.len() == 2 {
                            let sub = self.eval_value(&args[0], tuple, schema);
                            let s = self.eval_value(&args[1], tuple, schema);
                            match (sub, s) {
                                (Value::Text(needle), Value::Text(haystack)) => {
                                    Value::Int64(haystack.find(&needle).map(|i| i as i64 + 1).unwrap_or(0))
                                }
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "LEFT" => {
                        if args.len() == 2 {
                            let s = self.eval_value(&args[0], tuple, schema);
                            let n = self.eval_value(&args[1], tuple, schema);
                            match (s, n) {
                                (Value::Text(s), Value::Int64(n)) => {
                                    Value::Text(s.chars().take(n.max(0) as usize).collect())
                                }
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "RIGHT" => {
                        if args.len() == 2 {
                            let s = self.eval_value(&args[0], tuple, schema);
                            let n = self.eval_value(&args[1], tuple, schema);
                            match (s, n) {
                                (Value::Text(s), Value::Int64(n)) => {
                                    let n = n.max(0) as usize;
                                    let len = s.chars().count();
                                    Value::Text(s.chars().skip(len.saturating_sub(n)).collect())
                                }
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "LPAD" => {
                        if args.len() >= 2 {
                            let s = match self.eval_value(&args[0], tuple, schema) { Value::Text(s) => s, _ => return Value::Null };
                            let n = match self.eval_value(&args[1], tuple, schema) { Value::Int64(n) => n.max(0) as usize, _ => return Value::Null };
                            let pad = if args.len() >= 3 {
                                match self.eval_value(&args[2], tuple, schema) { Value::Text(p) => p, _ => " ".to_string() }
                            } else { " ".to_string() };
                            let len = s.chars().count();
                            if len >= n { Value::Text(s.chars().take(n).collect()) }
                            else {
                                let needed = n - len;
                                let padding: String = pad.chars().cycle().take(needed).collect();
                                Value::Text(format!("{}{}", padding, s))
                            }
                        } else { Value::Null }
                    }
                    "RPAD" => {
                        if args.len() >= 2 {
                            let s = match self.eval_value(&args[0], tuple, schema) { Value::Text(s) => s, _ => return Value::Null };
                            let n = match self.eval_value(&args[1], tuple, schema) { Value::Int64(n) => n.max(0) as usize, _ => return Value::Null };
                            let pad = if args.len() >= 3 {
                                match self.eval_value(&args[2], tuple, schema) { Value::Text(p) => p, _ => " ".to_string() }
                            } else { " ".to_string() };
                            let len = s.chars().count();
                            if len >= n { Value::Text(s.chars().take(n).collect()) }
                            else {
                                let needed = n - len;
                                let padding: String = pad.chars().cycle().take(needed).collect();
                                Value::Text(format!("{}{}", s, padding))
                            }
                        } else { Value::Null }
                    }
                    "REPEAT" => {
                        if args.len() == 2 {
                            let s = self.eval_value(&args[0], tuple, schema);
                            let n = self.eval_value(&args[1], tuple, schema);
                            match (s, n) {
                                (Value::Text(s), Value::Int64(n)) => Value::Text(s.repeat(n.max(0) as usize)),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "REVERSE" => {
                        if let Some(arg) = args.first() {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Text(s) => Value::Text(s.chars().rev().collect()),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "INITCAP" => {
                        if let Some(arg) = args.first() {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Text(s) => {
                                    let mut cap_next = true;
                                    let result: String = s.chars().map(|c| {
                                        if cap_next { cap_next = false; c.to_uppercase().next().unwrap_or(c) }
                                        else if c.is_whitespace() { cap_next = true; c }
                                        else { c.to_lowercase().next().unwrap_or(c) }
                                    }).collect();
                                    Value::Text(result)
                                }
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
                        if let Some(arg) = args.first() {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Text(s) => Value::Int64(s.chars().count() as i64),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "OCTET_LENGTH" => {
                        if let Some(arg) = args.first() {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Text(s) => Value::Int64(s.len() as i64),
                                Value::Bytes(b) => Value::Int64(b.len() as i64),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    "SIGN" => {
                        if let Some(arg) = args.first() {
                            match self.eval_value(arg, tuple, schema) {
                                Value::Int64(n) => Value::Int64(if n > 0 { 1 } else if n < 0 { -1 } else { 0 }),
                                Value::Float64(f) => Value::Int64(if f > 0.0 { 1 } else if f < 0.0 { -1 } else { 0 }),
                                _ => Value::Null,
                            }
                        } else { Value::Null }
                    }
                    _ => Value::Null,
                }
            }
            Expr::Case { operand, when_clauses, else_result } => {
                if let Some(operand) = operand {
                    let op_val = self.eval_value(operand, tuple, schema);
                    for (when_expr, then_expr) in when_clauses {
                        let when_val = self.eval_value(when_expr, tuple, schema);
                        if op_val.compare(&when_val) == Some(std::cmp::Ordering::Equal) {
                            return self.eval_value(then_expr, tuple, schema);
                        }
                    }
                } else {
                    for (when_expr, then_expr) in when_clauses {
                        let when_val = self.eval_value(when_expr, tuple, schema);
                        match when_val {
                            Value::Bool(true) => return self.eval_value(then_expr, tuple, schema),
                            _ => {}
                        }
                    }
                }
                match else_result {
                    Some(e) => self.eval_value(e, tuple, schema),
                    None => Value::Null,
                }
            }
            Expr::Cast { expr, data_type } => {
                let val = self.eval_value(expr, tuple, schema);
                cast_value(val, *data_type)
            }
            Expr::IsNull(inner) => {
                let val = self.eval_value(inner, tuple, schema);
                Value::Bool(val.is_null())
            }
            Expr::IsNotNull(inner) => {
                let val = self.eval_value(inner, tuple, schema);
                Value::Bool(!val.is_null())
            }
            Expr::InList { expr, list } => {
                let val = self.eval_value(expr, tuple, schema);
                let found = list.iter().any(|item| {
                    let item_val = self.eval_value(item, tuple, schema);
                    val.compare(&item_val) == Some(std::cmp::Ordering::Equal)
                });
                Value::Bool(found)
            }
            Expr::Between { expr, low, high } => {
                let val = self.eval_value(expr, tuple, schema);
                let low_val = self.eval_value(low, tuple, schema);
                let high_val = self.eval_value(high, tuple, schema);
                let ge_low = matches!(val.compare(&low_val), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal));
                let le_high = matches!(val.compare(&high_val), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal));
                Value::Bool(ge_low && le_high)
            }
            Expr::Subquery(subquery) => {
                let result = self.execute(Statement::Select(*subquery.clone()), current_txn_id());
                match result {
                    Ok(ExecutionResult::Rows { rows, .. }) => {
                        if rows.len() == 1 && !rows[0].is_empty() {
                            rows[0][0].clone()
                        } else {
                            Value::Null
                        }
                    }
                    _ => Value::Null,
                }
            }
            Expr::Exists(subquery) => {
                let result = self.execute(Statement::Select(*subquery.clone()), current_txn_id());
                match result {
                    Ok(ExecutionResult::Rows { rows, .. }) => Value::Bool(!rows.is_empty()),
                    _ => Value::Bool(false),
                }
            }
            Expr::InSubquery { expr, subquery } => {
                let result = self.execute(Statement::Select(*subquery.clone()), current_txn_id());
                let val = self.eval_value(expr, tuple, schema);
                match result {
                    Ok(ExecutionResult::Rows { rows, .. }) => {
                        let found = rows.iter().any(|row| {
                            if row.is_empty() { return false; }
                            val.compare(&row[0]) == Some(std::cmp::Ordering::Equal)
                        });
                        Value::Bool(found)
                    }
                    _ => Value::Bool(false),
                }
            }
            Expr::JsonPath { .. } | Expr::WindowFunction { .. } | Expr::Default => Value::Null,
        }
    }

    pub fn txn_manager(&self) -> &TransactionManager {
        &self.txn_manager
    }

    pub fn stats(&self) -> &parking_lot::RwLock<HashMap<String, TableStats>> {
        &self.stats
    }

    fn stats_snapshot(&self) -> StatsCatalog {
        self.stats.read().clone()
    }

    #[inline]
    fn ssi_read(&self, txn_id: Option<TxnId>, table: &str, key: &[u8]) {
        if let Some(tid) = txn_id {
            self.txn_manager.ssi_record_read(tid, table, key);
        }
    }

    #[inline]
    fn ssi_predicate(&self, txn_id: Option<TxnId>, table: &str) {
        if let Some(tid) = txn_id {
            self.txn_manager.ssi_record_predicate(tid, table);
        }
    }

    #[inline]
    fn ssi_write(&self, txn_id: Option<TxnId>, table: &str, key: &[u8]) {
        if let Some(tid) = txn_id {
            self.txn_manager.ssi_record_write(tid, table, key);
        }
    }

    fn record_undo(&self, txn_id: Option<TxnId>, table: &str, key: &[u8], prev: Option<Vec<u8>>) {
        let Some(tid) = txn_id else { return; };
        self.txn_undo
            .lock()
            .entry(tid)
            .or_default()
            .entry((table.to_string(), key.to_vec()))
            .or_insert(prev);
    }

    fn clear_txn_undo(&self, txn_id: TxnId) {
        self.txn_undo.lock().remove(&txn_id);
    }

    fn next_rowid(&self, table: &str, td: &TableData) -> Vec<u8> {
        let mut map = self.rowids.lock();
        let next = match map.get_mut(table) {
            Some(c) => {
                let v = *c;
                *c += 1;
                v
            }
            None => {
                let mut max: u64 = 0;
                if let Ok(entries) = td.index.scan_all() {
                    for (k, _) in &entries {
                        if let Ok(arr) = <[u8; 8]>::try_from(k.as_slice()) {
                            max = max.max(u64::from_be_bytes(arr));
                        }
                    }
                }
                let v = max + 1;
                map.insert(table.to_string(), v + 1);
                v
            }
        };
        next.to_be_bytes().to_vec()
    }

    pub fn rollback(&self, txn_id: TxnId) {
        self.rollback_txn_effects(txn_id);
        let _ = self.txn_manager.abort(txn_id);
    }

    fn rollback_txn_effects(&self, txn_id: TxnId) {
        self.discard_txn_deltas(txn_id);
        self.clear_pending_deltas();
        let Some(undo) = self.txn_undo.lock().remove(&txn_id) else { return; };
        let tables = self.tables.read();
        let mut affected: std::collections::HashSet<String> = std::collections::HashSet::new();
        for ((table, key), prev) in &undo {
            let Some(td) = tables.get(table) else { continue; };
            affected.insert(table.clone());
            let current = td.index.search(key).ok().flatten();
            match (current, prev) {
                (Some(cur), Some(pv)) => {
                    if let (Some(cv), Some(pvv)) =
                        (Tuple::deserialize_to_vec(&cur), Tuple::deserialize_to_vec(pv))
                    {
                        let _ = Self::update_secondary_indexes_update(td, &cv, &pvv, key);
                    }
                    let _ = td.index.insert(key.clone(), pv.clone());
                }
                (Some(cur), None) => {
                    if let Some(cv) = Tuple::deserialize_to_vec(&cur) {
                        Self::update_secondary_indexes_delete(td, &cv, key);
                    }
                    let _ = td.index.delete(key);
                }
                (None, Some(pv)) => {
                    if let Some(pvv) = Tuple::deserialize_to_vec(pv) {
                        let _ = Self::update_secondary_indexes_insert(td, &pvv, key);
                    }
                    let _ = td.index.insert(key.clone(), pv.clone());
                }
                (None, None) => {}
            }
        }
        for table in &affected {
            if let Some(td) = tables.get(table) {
                td.version_store.rollback_txn(txn_id);
            }
        }
    }

    fn index_snapshot(&self) -> IndexCatalog {
        let mut cat = IndexCatalog::new();
        for (tname, td) in self.tables.read().iter() {
            if td.secondary_indexes.is_empty() {
                continue;
            }
            let infos: Vec<IndexInfo> = td.secondary_indexes.iter().map(|s| IndexInfo {
                name: s.name.clone(),
                columns: s.columns.iter()
                    .filter_map(|&i| td.schema.columns.get(i).map(|c| c.name.clone()))
                    .collect(),
                unique: s.unique,
            }).collect();
            cat.insert(tname.clone(), infos);
        }
        cat
    }

    fn collect_table_rows(&self, table: &str) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        let tables = self.tables.read();
        let td = tables.get(table)
            .ok_or_else(|| QueryError::Execution(format!("Table '{}' not found", table)))?;
        let col_names: Vec<String> = td.schema.columns.iter().map(|c| c.name.clone()).collect();
        let entries = td.index.scan_all()
            .map_err(|e| QueryError::Execution(e.to_string()))?;
        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(entries.len());
        for (_, data) in entries {
            if let Some(t) = Tuple::deserialize(&data) {
                rows.push(t.to_vec());
            }
        }
        Ok((col_names, rows))
    }

    fn exec_analyze(&self, table: Option<&str>) -> Result<ExecutionResult> {
        let target_tables: Vec<String> = if let Some(t) = table {
            if !self.tables.read().contains_key(t) {
                return Err(QueryError::Execution(format!("Table '{}' not found", t)));
            }
            vec![t.to_string()]
        } else {
            self.tables.read().keys().cloned().collect()
        };

        let mut analyzed = 0u64;
        for name in &target_tables {
            let (col_names, rows) = self.collect_table_rows(name)?;
            let s = compute_table_stats(
                name.clone(),
                &col_names,
                rows,
                DEFAULT_MCV_COUNT,
                DEFAULT_HISTOGRAM_BUCKETS,
            );
            self.stats.write().insert(name.clone(), s);
            analyzed += 1;
        }

        Ok(ExecutionResult::Ok(format!("ANALYZE {}", analyzed)))
    }

    fn exec_backup(&self, path: &str) -> Result<ExecutionResult> {
        use bytedb_core::backup::Backup;
        use bytedb_core::migration::STORAGE_FORMAT_VERSION;
        let ds = self.disk_store.as_ref().ok_or_else(|| {
            QueryError::Execution("BACKUP requires a disk-backed engine".into())
        })?;
        let wal = self.wal.as_ref().ok_or_else(|| {
            QueryError::Execution("BACKUP requires WAL".into())
        })?;
        let data_dir = ds.registry().root().to_path_buf();
        let backup_dir = std::path::PathBuf::from(path);
        let manifest = Backup::create(&data_dir, wal, &backup_dir, STORAGE_FORMAT_VERSION)
            .map_err(|e| QueryError::Execution(format!("backup failed: {}", e)))?;
        Ok(ExecutionResult::Ok(format!(
            "BACKUP at LSN {} -> {}", manifest.backup_lsn, backup_dir.display()
        )))
    }

    fn exec_restore(&self, path: &str, to_lsn: Option<u64>) -> Result<ExecutionResult> {
        use bytedb_core::backup::Backup;
        let ds = self.disk_store.as_ref().ok_or_else(|| {
            QueryError::Execution("RESTORE requires a disk-backed engine".into())
        })?;
        let target = ds.registry().root().to_path_buf();
        let backup_dir = std::path::PathBuf::from(path);
        let manifest = Backup::restore_to(&backup_dir, &target)
            .map_err(|e| QueryError::Execution(format!("restore failed: {}", e)))?;
        if let Some(lsn) = to_lsn {
            let marker = target.join("pitr_target.txt");
            std::fs::write(&marker, lsn.to_string())
                .map_err(|e| QueryError::Execution(format!("write pitr marker: {}", e)))?;
        }
        Ok(ExecutionResult::Ok(format!(
            "RESTORE from {} (backup_lsn={}{}). Restart the server to load restored data.",
            backup_dir.display(),
            manifest.backup_lsn,
            match to_lsn { Some(l) => format!(", PITR target LSN {}", l), None => String::new() },
        )))
    }

    fn exec_migrate(&self) -> Result<ExecutionResult> {
        use bytedb_core::migration::Migration;
        let ds = self.disk_store.as_ref().ok_or_else(|| {
            QueryError::Execution("MIGRATE requires a disk-backed engine".into())
        })?;
        let data_dir = ds.registry().root().to_path_buf();
        let report = Migration::migrate_to_current(&data_dir)
            .map_err(|e| QueryError::Execution(format!("migration failed: {}", e)))?;
        Ok(ExecutionResult::Ok(format!(
            "MIGRATE scanned {} files, migrated {}, backups {}",
            report.scanned.len(), report.migrated.len(), report.backups.len()
        )))
    }

    fn exec_show_stats(&self, table: Option<&str>) -> Result<ExecutionResult> {
        let columns = vec![
            "table".to_string(),
            "row_count".to_string(),
            "column".to_string(),
            "null_fraction".to_string(),
            "ndv".to_string(),
            "mcv_count".to_string(),
            "histogram_buckets".to_string(),
            "computed_at_secs".to_string(),
        ];

        let stats = self.stats.read();
        let mut rows: Vec<Vec<Value>> = Vec::new();
        let names: Vec<String> = if let Some(t) = table {
            if !stats.contains_key(t) {
                return Ok(ExecutionResult::Rows { columns, rows });
            }
            vec![t.to_string()]
        } else {
            let mut all: Vec<String> = stats.keys().cloned().collect();
            all.sort();
            all
        };

        for tname in &names {
            let Some(ts) = stats.get(tname) else { continue; };
            for col in &ts.columns {
                let buckets = if col.bucket_bounds.is_empty() {
                    0
                } else {
                    (col.bucket_bounds.len() - 1) as i64
                };
                rows.push(vec![
                    Value::Text(ts.table.clone()),
                    Value::Int64(ts.row_count as i64),
                    Value::Text(col.column.clone()),
                    Value::Float64(col.null_fraction),
                    Value::Int64(col.ndv as i64),
                    Value::Int64(col.mcv.len() as i64),
                    Value::Int64(buckets),
                    Value::Int64(ts.computed_at_secs as i64),
                ]);
            }
        }

        Ok(ExecutionResult::Rows { columns, rows })
    }

    pub fn txn_manager_arc(&self) -> Arc<TransactionManager> {
        Arc::clone(&self.txn_manager)
    }

    pub fn snapshot_version_stores(&self) -> Vec<Arc<bytedb_core::mvcc::version_store::VersionStore>> {
        self.tables
            .read()
            .values()
            .map(|t| Arc::clone(&t.version_store))
            .collect()
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    pub fn tables(&self) -> &parking_lot::RwLock<HashMap<String, Arc<TableData>>> {
        &self.tables
    }

    pub fn bulk_insert(&self, table: &str, rows: Vec<Vec<Value>>) -> Result<u64> {
        let tables = self.tables.read();
        let table_data = tables.get(table)
            .ok_or_else(|| QueryError::Execution(format!("Table '{}' not found", table)))?;

        let pk_cols = table_data.schema.primary_key_columns();
        let mut count = 0u64;

        for row_values in rows {
            let tuple = Tuple::new(row_values);
            let key = tuple.key_bytes(&pk_cols);
            let data = tuple.serialize();
            table_data.index.insert(key, data)
                .map_err(|e| QueryError::Execution(e.to_string()))?;
            count += 1;
        }

        Ok(count)
    }

    pub fn exec_seq_scan_vectorized(
        &self,
        table: &str,
        filter: Option<&Expr>,
        _txn_id: Option<TxnId>,
        limit: Option<usize>,
    ) -> Result<ExecutionResult> {
        let tables = self.tables.read();
        let table_data = tables.get(table)
            .ok_or_else(|| QueryError::Execution(format!("Table '{}' not found", table)))?;

        let col_names: Vec<String> = table_data.schema.columns.iter()
            .map(|c| c.name.clone())
            .collect();
        let num_columns = col_names.len();

        let fast_filter = filter.and_then(|f| self.try_fast_filter(f, &table_data.schema));

        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        let scan_limit = limit;

        table_data.index.for_each_leaf_batch(|values| {
            let refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
            if let Some(batch) = deserialize_batch(&refs, num_columns) {
                if let Some((col_idx, ref op, ref lit_val)) = fast_filter {
                    let sel = match lit_val {
                        Value::Int64(v) => batch.filter_int64_column(col_idx, *op, *v),
                        Value::Float64(v) => batch.filter_float64_column(col_idx, *op, *v),
                        Value::Text(v) => batch.filter_text_column(col_idx, *op, v),
                        _ => SelectionVector { indices: (0..batch.num_rows as u16).collect() },
                    };
                    let rows = batch.materialize_rows(&sel);
                    all_rows.extend(rows);
                } else {
                    let rows = batch.materialize_all_rows();
                    all_rows.extend(rows);
                }

                if let Some(lim) = scan_limit {
                    if all_rows.len() >= lim {
                        all_rows.truncate(lim);
                        return false;
                    }
                }
            }
            true
        }).map_err(|e| QueryError::Execution(e.to_string()))?;

        Ok(ExecutionResult::Rows { columns: col_names, rows: all_rows })
    }
}

fn int128_to_value(s: i128) -> Value {
    if s >= i64::MIN as i128 && s <= i64::MAX as i128 {
        Value::Int64(s as i64)
    } else {
        Value::Float64(s as f64)
    }
}

fn cast_value(val: Value, target: DataType) -> Value {
    match target {
        DataType::Int64 => match val {
            Value::Int64(_) => val,
            Value::Float64(f) => Value::Int64(f as i64),
            Value::Text(s) => s.parse::<i64>().map(Value::Int64).unwrap_or(Value::Null),
            Value::Bool(b) => Value::Int64(if b { 1 } else { 0 }),
            _ => Value::Null,
        },
        DataType::Float64 => match val {
            Value::Float64(_) => val,
            Value::Int64(n) => Value::Float64(n as f64),
            Value::Text(s) => s.parse::<f64>().map(Value::Float64).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        DataType::Text => match val {
            Value::Text(_) => val,
            Value::Int64(n) => Value::Text(n.to_string()),
            Value::Float64(f) => Value::Text(f.to_string()),
            Value::Bool(b) => Value::Text(b.to_string()),
            Value::Date(d) => Value::Text(bytedb_core::tuple::value::format_date(d)),
            Value::Decimal(m, s) => Value::Text(bytedb_core::tuple::value::format_decimal(m, s)),
            Value::Uuid(b) => Value::Text(bytedb_core::tuple::value::format_uuid(&b)),
            Value::Timestamp(ts) => Value::Text(bytedb_core::tuple::value::format_timestamp(ts)),
            Value::Interval(us) => Value::Text(format!("{} microseconds", us)),
            Value::Null => Value::Null,
            _ => Value::Text(format!("{:?}", val)),
        },
        DataType::Bool => match val {
            Value::Bool(_) => val,
            Value::Int64(n) => Value::Bool(n != 0),
            Value::Text(s) => match s.to_lowercase().as_str() {
                "true" | "t" | "1" | "yes" => Value::Bool(true),
                "false" | "f" | "0" | "no" => Value::Bool(false),
                _ => Value::Null,
            },
            _ => Value::Null,
        },
        DataType::Date => match val {
            Value::Date(_) => val,
            Value::Text(s) => bytedb_core::tuple::value::parse_date(&s).map(Value::Date).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        DataType::Timestamp => match val {
            Value::Timestamp(_) => val,
            Value::Text(s) => bytedb_core::tuple::value::parse_timestamp(&s).map(Value::Timestamp).unwrap_or(Value::Null),
            Value::Int64(n) => Value::Timestamp(n),
            _ => Value::Null,
        },
        DataType::Uuid => match val {
            Value::Uuid(_) => val,
            Value::Text(s) => bytedb_core::tuple::value::parse_uuid(&s).map(Value::Uuid).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        DataType::Decimal => match val {
            Value::Decimal(_, _) => val,
            Value::Int64(n) => Value::Decimal(n as i128, 0),
            Value::Float64(f) => bytedb_core::tuple::value::parse_decimal(&f.to_string()).map(|(m, s)| Value::Decimal(m, s)).unwrap_or(Value::Null),
            Value::Text(s) => bytedb_core::tuple::value::parse_decimal(&s).map(|(m, s)| Value::Decimal(m, s)).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        DataType::Interval => match val {
            Value::Interval(_) => val,
            Value::Text(s) => parse_interval(&s).map(Value::Interval).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        DataType::Bytes => match val {
            Value::Bytes(_) => val,
            Value::Text(s) => Value::Bytes(s.into_bytes()),
            Value::Null => Value::Null,
            _ => Value::Null,
        },
        _ => val,
    }
}

fn chrono_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let (y, m, d) = days_to_ymd(days as i64);
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, time_secs / 3600, (time_secs % 3600) / 60, time_secs % 60)
}

fn now_micros() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

fn parse_interval(s: &str) -> Option<i64> {
    let s = s.trim();
    let s_lower = s.to_lowercase();

    let us_per_unit: i64 = if s_lower.ends_with("microsecond") || s_lower.ends_with("microseconds") {
        1
    } else if s_lower.ends_with("millisecond") || s_lower.ends_with("milliseconds") {
        1000
    } else if s_lower.ends_with("second") || s_lower.ends_with("seconds") {
        1_000_000
    } else if s_lower.ends_with("minute") || s_lower.ends_with("minutes") {
        60_000_000
    } else if s_lower.ends_with("hour") || s_lower.ends_with("hours") {
        3_600_000_000
    } else if s_lower.ends_with("day") || s_lower.ends_with("days") {
        86_400_000_000
    } else if s_lower.ends_with("week") || s_lower.ends_with("weeks") {
        604_800_000_000
    } else {
        return None;
    };

    let num_str = s.trim_end_matches(|c: char| !c.is_ascii_digit()).trim();
    let n: i64 = num_str.parse().ok()?;
    Some(n.saturating_mul(us_per_unit))
}

fn today_days() -> i32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    (secs / 86400) as i32
}

#[allow(dead_code)]
fn chrono_date() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    let days = secs / 86400;
    let (y, m, d) = days_to_ymd(days as i64);
    format!("{:04}-{:02}-{:02}", y, m, d)
}

fn days_to_ymd(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

fn extract_from_timestamp(field: &str, val: &Value) -> Value {
    let s = match val {
        Value::Text(s) => s.as_str(),
        _ => return Value::Null,
    };
    let parts: Vec<&str> = s.split(|c| c == '-' || c == ' ' || c == ':' || c == 'T').collect();
    match field {
        "YEAR" => parts.first().and_then(|p| p.parse::<i64>().ok()).map(Value::Int64).unwrap_or(Value::Null),
        "MONTH" => parts.get(1).and_then(|p| p.parse::<i64>().ok()).map(Value::Int64).unwrap_or(Value::Null),
        "DAY" => parts.get(2).and_then(|p| p.parse::<i64>().ok()).map(Value::Int64).unwrap_or(Value::Null),
        "HOUR" => parts.get(3).and_then(|p| p.parse::<i64>().ok()).map(Value::Int64).unwrap_or(Value::Null),
        "MINUTE" => parts.get(4).and_then(|p| p.parse::<i64>().ok()).map(Value::Int64).unwrap_or(Value::Null),
        "SECOND" => parts.get(5).and_then(|p| p.parse::<i64>().ok()).map(Value::Int64).unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn like_match(s: &str, pattern: &str) -> bool {
    if pattern.is_ascii() && s.is_ascii() {
        return like_match_ascii(s.as_bytes(), pattern.as_bytes(), false);
    }
    let s_chars: Vec<char> = s.chars().collect();
    let p_chars: Vec<char> = pattern.chars().collect();
    like_match_inner(&s_chars, &p_chars, false)
}

fn ilike_match(s: &str, pattern: &str) -> bool {
    if pattern.is_ascii() && s.is_ascii() {
        return like_match_ascii(s.as_bytes(), pattern.as_bytes(), true);
    }
    let s_chars: Vec<char> = s.chars().flat_map(|c| c.to_lowercase()).collect();
    let p_chars: Vec<char> = pattern.chars().flat_map(|c| c.to_lowercase()).collect();
    like_match_inner(&s_chars, &p_chars, false)
}

fn ascii_eq(a: u8, b: u8, case_insensitive: bool) -> bool {
    if case_insensitive {
        a.eq_ignore_ascii_case(&b)
    } else {
        a == b
    }
}

fn like_match_ascii(s: &[u8], p: &[u8], case_insensitive: bool) -> bool {
    let mut si = 0usize;
    let mut pi = 0usize;
    let mut star: Option<(usize, usize)> = None;
    while si < s.len() {
        if pi < p.len() && (p[pi] == b'_' || ascii_eq(p[pi], s[si], case_insensitive)) {
            si += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == b'%' {
            star = Some((pi, si));
            pi += 1;
        } else if let Some((sp, ss)) = star {
            pi = sp + 1;
            si = ss + 1;
            star = Some((sp, si));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == b'%' {
        pi += 1;
    }
    pi == p.len()
}

fn like_match_inner(s: &[char], p: &[char], _case_insensitive: bool) -> bool {
    let mut si = 0usize;
    let mut pi = 0usize;
    let mut star: Option<(usize, usize)> = None;
    while si < s.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == s[si]) {
            si += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '%' {
            star = Some((pi, si));
            pi += 1;
        } else if let Some((sp, ss)) = star {
            pi = sp + 1;
            si = ss + 1;
            star = Some((sp, si));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

fn ast_fk_action_to_core(a: crate::parser::ast::FkAction) -> bytedb_core::tuple::schema::FkAction {
    use crate::parser::ast::FkAction as A;
    use bytedb_core::tuple::schema::FkAction as C;
    match a {
        A::Restrict => C::Restrict,
        A::Cascade => C::Cascade,
        A::SetNull => C::SetNull,
    }
}
