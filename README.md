# ByteDB

A hybrid storage engine in Rust: relational SQL + key-value + document, with MVCC, WAL/ARIES recovery, B+Tree indexes, and a vectorized executor.

**v0.2** - full storage rewrite to 8KB slotted pages, LRU-K buffer pool, WAL group commit, background workers (WAL flusher, page flusher, checkpoint, vacuum), and a real cost-based optimizer driven by table statistics.

> 📖 Doc language: [English](#english) · [Русский](#русский)

---

## English

### What's new in v0.2

- **Slotted pages (8KB)** - every table file is a sequence of fixed-size pages with a 32-byte header and slot directory.
- **Page checksums (CRC32)** - every page is checksummed on write and verified on read. Silent corruption surfaces immediately as `ChecksumMismatch`, not weeks later.
- **WAL integrity** - strict LSN chaining (each record carries `prev_lsn`), per-record CRC32 covering header+payload, and torn-write detection on recovery. A flipped bit anywhere in the WAL aborts replay with `WalCorrupted` instead of producing wrong data.
- **Atomic file writes** - table files (`*.tbl`) and the catalog (`catalog.bin`) are written to `*.tmp`, fsynced, then atomically renamed. A crash mid-write leaves either the old file fully intact or the new file fully visible - never a half-written mix. Both formats now carry a CRC32 trailer over the payload.
- **Row-level locking** - shared/exclusive locks per `(table, row-key)`, FIFO waiter queue, wait-for graph deadlock detection that aborts the requester with `Deadlock` instead of hanging, configurable lock-wait timeout (`LockTimeout`), and live metrics (acquires, releases, waits, timeouts, deadlocks, total wait micros).
- **Per-transaction deadlines** - optional default timeout on every `begin`, plus `begin_with_timeout` for one-off overrides; `check_deadline` and `timed_out_txns` let the server abort runaway transactions with `TransactionTimeout`.
- **Query cancellation & resource governor** - every query can run under a `QueryContext` with a cooperative cancel flag, an absolute deadline, and per-query caps on memory bytes, temp-spill bytes, and scan rows. Hot loops (sequential scans, hash joins, hash aggregates, distinct, sort) poll the context and bail out with `Cancelled`, `QueryTimeout`, or `ResourceLimit` instead of OOM-ing the process. Use `engine.execute_sql_with_ctx(sql, txn, ctx)`.
- **Observability metrics** - `LatencyHistogram` (p50/p95/p99 + mean/max + ring-buffered samples + QPS), `GcMetrics` (vacuum runs, versions/keys removed, total/last pause micros), `DeadTupleMetrics` (live/dead version counts + ratio). Buffer pool already exposes hits/misses; WAL exposes `fsync_count` and `commits_served`; lock manager exposes acquires/waits/timeouts/deadlocks. `engine.query_latency()` returns the per-query latency histogram.
- **Slow query log** - `engine.set_slow_query_threshold_ms(Some(ms))` enables auto-capture of any query whose wall time crosses the threshold. `engine.slow_query_log()` returns the captured entries (ring-buffered, capacity 256). Each entry records the SQL text, duration in micros, txn id, and wall-clock timestamp.
- **`EXPLAIN ANALYZE`** - prints the chosen plan with `estimated_rows` + `estimated_cost`, then runs the statement and prints `actual rows`, wall time in ms, and the est/actual factor so you can spot bad cardinality estimates.
- **WAL durability mode** - `LogManager::set_durability_mode(DurabilityMode::Strict | Relaxed)`. Strict (default): `commit(lsn)` blocks until the WAL is fsynced and chained — no ack before durability. Relaxed: `commit(lsn)` returns immediately and lets group commit batch the fsync — faster but the last few transactions can be lost on crash. Counters: `fsync_count`, `commits_served`, `relaxed_acks`.
- **Throttled vacuum** - `MvccVacuum::with_throttle(...)` runs vacuum in batches of N stores with a configurable sleep between batches, recording each run into `GcMetrics` (pause time) and refreshing `DeadTupleMetrics`. Non-blocking on writers — uses the existing per-key write lock for the brief retain pass.
- **Free-space map** - `storage::fsm::FreeSpaceMap` tracks per-page free byte counts in 16 buckets so `find_with_at_least(n)` returns a reusable page in O(1). Combined with the existing `Page::compact()` reclaiming dead-tuple space, this stops linear bloat under heavy update/delete workloads.
- **Buffer pool with LRU-K (K=2)** - bounded-memory page cache replacing the previous read-everything-at-startup model.
- **WAL group commit** - leader/follower fsync batching cuts disk syncs under concurrent writers.
- **Background workers** - dedicated threads for WAL flushing, dirty-page writeback, periodic checkpoints, and MVCC vacuum/GC.
- **MVCC garbage collector** - old versions invisible to all active transactions are reclaimed automatically.
- **`ANALYZE` statistics** - per-column NDV, MCV (top-K most common values), and equi-depth histograms; query with `SHOW STATS [FOR <table>]`.
- **Cost-based optimizer** - selectivity estimation from MCV/histograms, join cardinality from NDV, greedy reordering of left-deep INNER join chains by smallest cardinality. Outer joins are conservatively kept in source order.
- **Clean break** - v0.2 storage format is incompatible with v0.1 (new BSDB magic stamp).

### Status

- ✅ Storage: 8KB slotted pages, LRU-K buffer pool, WAL with group commit, ARIES recovery
- ✅ Concurrency: MVCC snapshot isolation, background vacuum/GC, periodic checkpoints
- ✅ Optimizer: cost-based join reordering driven by `ANALYZE` statistics (NDV, MCV, histograms)
- ✅ DDL: `CREATE/DROP TABLE`, `CREATE INDEX`, `ALTER TABLE` (ADD/DROP/RENAME COLUMN)
- ✅ DML: `INSERT` (incl. `INSERT...SELECT`, `RETURNING`, `ON CONFLICT`), `UPDATE`, `DELETE`, `SELECT`
- ✅ Constraints: `PRIMARY KEY`, `NOT NULL`, `UNIQUE`, `DEFAULT`, `CHECK`, `FOREIGN KEY`, `SERIAL`
- ✅ Queries: `JOIN` (INNER/LEFT/RIGHT), `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT/OFFSET`, `DISTINCT`, `UNION/INTERSECT/EXCEPT`, CTEs (`WITH`), subqueries (in `FROM`, scalar, `IN`, `EXISTS`), window functions (`ROW_NUMBER`, `SUM OVER`, ...)
- ✅ Expressions: `CASE WHEN`, `CAST` / `::`, `COALESCE`, `NULLIF`, `BETWEEN`, `LIKE`/`ILIKE`, `IS NULL`
- ✅ Built-ins: `UPPER`/`LOWER`/`LENGTH`/`SUBSTRING`/`TRIM`/`CONCAT`/`CONCAT_WS`/`SPLIT_PART`/`STARTS_WITH`/`ENDS_WITH`, `ABS`/`CEIL`/`FLOOR`/`ROUND`/`POWER`/`SQRT`, `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`/`COUNT(DISTINCT)`
- ✅ Transactions: `BEGIN [SERIALIZABLE]`, `COMMIT`, `ROLLBACK`, MVCC snapshot isolation
- ✅ KV mode: `KV GET/SET/DELETE/SCAN`
- ✅ Document mode: `DOC INSERT/FIND/UPDATE/DELETE` with JSON path
- ✅ Utility: `SHOW TABLES/COLUMNS/CREATE TABLE/STATS`, `ANALYZE [TABLE]`, `TRUNCATE`, `EXPLAIN [ANALYZE]`, `information_schema.tables`/`columns`
- ✅ Persistence: snapshot (binary + JSON) + WAL with ARIES-style recovery
- ✅ Performance: zero-copy filter, fast top-N, fast aggregate, prepared-statement cache
- ✅ Observability: latency histogram (p50/p95/p99/QPS), GC pause metrics, dead-tuple ratio, slow query log, `EXPLAIN ANALYZE`
- ✅ Durability: strict / relaxed WAL modes, group commit, fsync metrics
- ✅ Storage hygiene: throttled MVCC vacuum, free-space map, page compaction

### Architecture

```
SQL → Parser → AST → Logical Plan → Optimizer → Physical Plan → Executor → Result
                                                                  │
                                  TableData ←─── MVCC Version Store
                                       │
                                       ▼
                                 B+Tree index ──── WAL ──── Snapshot
```

- **bytedb-core** - storage primitives (tuples, schema, B+Tree, MVCC, WAL, snapshot, sequences)
- **bytedb-query** - parser, planner, optimizer, executor, KV / Document engines
- **bytedb-server** - server entrypoint
- **bytedb-bench** - benchmarks

### Quick start

```rust
use std::sync::Arc;
use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_query::executor::engine::{QueryEngine, ExecutionResult};

let db  = Arc::new(Database::new("mydb"));
let txn = Arc::new(TransactionManager::new());
let engine = QueryEngine::new(db, txn);

engine.execute_sql("CREATE TABLE users (id SERIAL, email TEXT UNIQUE, age INT CHECK (age >= 0))", None)?;
engine.execute_sql("INSERT INTO users (email, age) VALUES ('a@x.com', 30)", None)?;

let r = engine.execute_sql("SELECT id, email FROM users WHERE age > 18", None)?;
if let ExecutionResult::Rows { columns, rows } = r {
    println!("{:?}", columns);
    for row in rows { println!("{:?}", row); }
}
```

### SQL reference

#### CREATE TABLE

```sql
CREATE TABLE [IF NOT EXISTS] name (
    col TYPE [NOT NULL] [PRIMARY KEY] [UNIQUE] [DEFAULT expr]
        [CHECK (expr)] [REFERENCES other_table(col)],
    ...,
    CHECK (table_expr),
    FOREIGN KEY (a, b) REFERENCES other(x, y)
);
```

Supported types - all stored natively (no string round-tripping):
- **Numeric:** `INT`/`INTEGER`/`BIGINT`/`SMALLINT`, `SERIAL` (auto-increment), `FLOAT`/`REAL`/`DOUBLE PRECISION`, `NUMERIC(p,s)`/`DECIMAL(p,s)` (fixed-point i128 mantissa)
- **String/binary:** `TEXT`, `VARCHAR(n)`, `BYTES`
- **Boolean:** `BOOL`
- **Temporal:** `TIMESTAMP` (μs since epoch), `DATE` (days since epoch - accepts `'YYYY-MM-DD'`)
- **Identifier:** `UUID` (16 raw bytes - accepts `'550e8400-e29b-41d4-a716-446655440000'`)
- **Document:** `JSON`

String literals are auto-coerced to the column type on INSERT (e.g. `'2026-05-20'` → `Date`, `'550e8400-...'` → `Uuid`).

#### Foreign key actions

```sql
CREATE TABLE child (
    id INT PRIMARY KEY,
    parent_id INT REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT
);
```

Supported actions: `RESTRICT` (default), `CASCADE`, `SET NULL`, `NO ACTION` (alias for `RESTRICT`). For table-level constraints: `FOREIGN KEY (cols) REFERENCES other(cols) ON DELETE CASCADE`.

#### Multi-database

Each database lives in its own directory under `<data-dir>/databases/<name>/` with its own catalog and table files.

```sql
CREATE DATABASE analytics;
USE analytics;
SHOW DATABASES;
DROP DATABASE analytics;
```

`USE` swaps the active table set; `CREATE DATABASE` materializes the directory immediately; `DROP DATABASE` deletes it.

#### Built-in date/time

`NOW()`, `CURRENT_TIMESTAMP` → native `Timestamp` (microseconds since epoch). `CURRENT_DATE()` → native `Date`. Use them in `INSERT` / `WHERE` and the executor stores the typed value, not a formatted string.

#### Transactions

```sql
BEGIN [SERIALIZABLE];
-- ... statements ...
COMMIT;   -- or ROLLBACK;
```

#### UPSERT

```sql
INSERT INTO t (id, val) VALUES (1, 'x')
ON CONFLICT (id) DO UPDATE SET val = 'x';
```

#### Window functions

```sql
SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) AS rn FROM emp;
```

#### CTE

```sql
WITH big AS (SELECT * FROM orders WHERE amount > 100)
SELECT * FROM big;
```

#### ANALYZE & statistics

```sql
ANALYZE TABLE users;       -- collect NDV, MCV, histograms for one table
ANALYZE;                   -- analyze every user table
SHOW STATS FOR users;      -- per-column row count / null fraction / NDV / MCVs
SHOW STATS;                -- summary across all tables
```

The optimizer consults this catalog for selectivity and join cardinality. Without stats, plans fall back to source order - same behaviour as v0.1.

### Storage modes

- **Relational** - `CREATE TABLE`, `SELECT`, joins, etc.
- **Key-value** - `KV SET "k" "v"`, `KV GET "k"`, `KV SCAN "a" "z"`
- **Document** - `DOC INSERT INTO logs {"level":"error"}`, `DOC FIND IN logs WHERE level = 'error'`

### Build & test

```bash
cargo build --release
cargo test
cargo bench -p bytedb-bench
```

### Physical storage layout

ByteDB persists a database as a directory on disk. Pass `--data-dir <path>` to the server (default `./bytedb_data`):

```
<data-dir>/
├── server.meta              # registry of all databases
├── databases/
│   └── <db-name>/
│       ├── catalog.bin      # schemas, sequences, FK metadata
│       └── tables/
│           ├── users.tbl    # raw key/value pairs (binary)
│           └── orders.tbl
├── bytedb.wal               # Write-Ahead Log (ARIES-style)
└── snapshots/               # optional periodic full-state snapshots
    └── snapshot_*.bin
```

- **Per-table data files** (`databases/<db>/tables/<table>.tbl`) - every committed `INSERT/UPDATE/DELETE` rewrites the affected table file via atomic rename. **Schema and data are stored separately**: `catalog.bin` holds metadata, `*.tbl` holds rows. No periodic flush is needed - the file on disk is always up to date after each successful mutation.
- **WAL** (`bytedb.wal`) - durability for in-flight transactions; redo committed / undo uncommitted on restart.
- **Snapshots** (`snapshots/`) - optional full-state archives. Disabled by default (`--snapshot-write-threshold 0`); time-based snapshots default to every 30 min. To turn them off entirely use `--no-snapshot`. Set `--no-shutdown-snapshot` to skip the final snapshot on Ctrl+C.

#### Tuning snapshots

| Flag | Default | Meaning |
|------|---------|---------|
| `--snapshot-interval-secs N` | 1800 | Take a snapshot every N seconds. `0` disables. |
| `--snapshot-write-threshold N` | 0 | Take a snapshot after N writes. `0` disables. |
| `--no-snapshot` | off | Disable all background snapshots. |
| `--no-shutdown-snapshot` | off | Skip final snapshot on Ctrl+C. |
| `--snapshot-format binary\|json` | binary | Snapshot encoding. |

#### Creating a physical database

1. Pick a directory. The server creates it if missing:
   ```bash
   bytedb-server --data-dir /var/lib/bytedb --host 0.0.0.0 --port 7654
   ```
   On first start the directory is empty → server initializes a fresh empty DB.
2. Execute DDL via your client:
   ```sql
   CREATE TABLE users (id SERIAL, email TEXT UNIQUE NOT NULL);
   ```
   The change is durable as soon as `COMMIT` (or autocommit) returns - it lives in WAL.
3. To force flushing in-memory state to a snapshot file, send `SIGINT` (Ctrl+C) or wait for the snapshot interval.

#### Updating / mutating the database

All DML goes through the same pipeline:

```
client → SQL parse → plan → executor
  ↓
WAL append  (durable)        ──┐
B+Tree insert/update/delete    │
MVCC version write             │── one logical mutation
returning rows (if any)        │
  ↓                          ──┘
ack to client
```

So an `UPDATE users SET email = 'b@x' WHERE id = 1`:
1. parser → AST → plan,
2. point-lookup the row by PK,
3. write an `Update { old_data, new_data }` WAL record,
4. replace the value in the B+Tree (and MVCC version chain if inside a transaction),
5. return modified-row count or `RETURNING` rows.

#### Crash recovery

If the process dies mid-write, on next start:

1. `SnapshotManager::load_latest()` restores the last full snapshot.
2. `RecoveryManager::recover(&wal)` walks the WAL forward from the snapshot LSN:
   - **REDO** every record whose txn committed,
   - **UNDO** every record whose txn never committed (rolled back or aborted).
3. The server is ready to accept connections.

You will see in the log:

```
WAL recovery: 42 redo, 3 undo records processed
Snapshot restored successfully
ByteDB server listening on 0.0.0.0:7654
```

#### Backups

A consistent snapshot is a single binary file. To back up:

```bash
# while server is running, force a snapshot:
kill -INT $(pidof bytedb-server)   # graceful shutdown writes final snapshot
cp -r /var/lib/bytedb /backup/bytedb-$(date +%F)
```

Restore = copy the directory back and start the server pointing at it.

#### Snapshot format

Use `--snapshot-format binary` (default, compact) or `--snapshot-format json` (human-readable, larger). Both round-trip the same data.

### Known limitations

- No PostgreSQL wire protocol (use the Rust client API).
- No replication - single-node only. (Roadmap.)

---

## Русский

### Что нового в v0.2

- **Slotted pages (8KB)** - файл каждой таблицы - последовательность страниц фиксированного размера с 32-байтным заголовком и слот-директорией.
- **Контрольные суммы страниц (CRC32)** - каждая страница вычисляет checksum при записи и сверяет его при чтении. Тихая порча данных всплывает сразу как `ChecksumMismatch`, а не через недели.
- **Целостность WAL** - строгая цепочка LSN (каждая запись хранит `prev_lsn`), CRC32 на каждую запись (заголовок + payload), детекция torn-write при восстановлении. Любой битый бит в WAL прерывает replay с `WalCorrupted` вместо тихой выдачи неверных данных.
- **Атомарная запись файлов** - файлы таблиц (`*.tbl`) и каталог (`catalog.bin`) пишутся через `*.tmp` → `fsync` → `rename`. Падение посреди записи оставляет либо старый файл целиком, либо новый файл целиком - никаких полузаписанных смесей. Оба формата теперь хранят CRC32-trailer по payload.
- **Row-level блокировки** - shared/exclusive локи на `(table, row-key)`, FIFO-очередь ожидающих, детекция дедлоков по wait-for графу с прерыванием запросившего ошибкой `Deadlock` вместо вечного ожидания, настраиваемый таймаут ожидания (`LockTimeout`) и живые метрики (acquires, releases, waits, timeouts, deadlocks, total wait micros).
- **Дедлайны транзакций** - опциональный дефолтный таймаут на каждый `begin`, плюс `begin_with_timeout` для разовых переопределений; `check_deadline` и `timed_out_txns` позволяют серверу прерывать зависшие транзакции ошибкой `TransactionTimeout`.
- **Отмена запросов и resource governor** - каждый запрос может выполняться под `QueryContext` с cooperative-флагом отмены, абсолютным дедлайном и лимитами на память, временный spill и количество просканированных строк. Горячие циклы (sequential scan, hash join, hash aggregate, distinct, sort) опрашивают контекст и завершаются с `Cancelled`, `QueryTimeout` или `ResourceLimit` вместо OOM. Использовать через `engine.execute_sql_with_ctx(sql, txn, ctx)`.
- **Метрики наблюдаемости** - `LatencyHistogram` (p50/p95/p99 + mean/max + ring-buffered семплы + QPS), `GcMetrics` (запуски vacuum, удалённые версии/ключи, общая/последняя длительность пауз), `DeadTupleMetrics` (количество живых/мёртвых версий + ratio). Buffer pool уже отдаёт hits/misses; WAL — `fsync_count` и `commits_served`; lock manager — acquires/waits/timeouts/deadlocks. `engine.query_latency()` возвращает гистограмму латентности по запросам.
- **Slow query log** - `engine.set_slow_query_threshold_ms(Some(ms))` включает автозахват каждого запроса, чьё время превышает порог. `engine.slow_query_log()` возвращает накопленные записи (ring-buffer, ёмкость 256). Каждая запись содержит SQL, длительность в микросекундах, txn id и wall-clock метку времени.
- **`EXPLAIN ANALYZE`** - печатает выбранный план с `estimated_rows` + `estimated_cost`, затем выполняет запрос и печатает `actual rows`, время в мс и фактор est/actual — чтобы видеть плохие оценки кардинальности.
- **Режимы durability WAL** - `LogManager::set_durability_mode(DurabilityMode::Strict | Relaxed)`. Strict (по умолчанию): `commit(lsn)` блокируется до тех пор, пока WAL не fsync-нут — никакого ack до durability. Relaxed: `commit(lsn)` возвращается мгновенно и group commit досинхронизует пачку — быстрее, но последние транзакции могут потеряться при крэше. Счётчики: `fsync_count`, `commits_served`, `relaxed_acks`.
- **Throttled vacuum** - `MvccVacuum::with_throttle(...)` запускает vacuum пачками по N стораджей с настраиваемой паузой между пачками, записывая каждый запуск в `GcMetrics` (длительность паузы) и обновляя `DeadTupleMetrics`. Не блокирует писателей — использует существующий per-key write lock на короткий retain-проход.
- **Free-space map** - `storage::fsm::FreeSpaceMap` хранит free-bytes по страницам в 16 бакетах, так что `find_with_at_least(n)` возвращает переиспользуемую страницу за O(1). В сочетании с существующим `Page::compact()`, освобождающим место от мёртвых tuple, это останавливает линейный bloat при тяжёлых update/delete-нагрузках.
- **Buffer pool с LRU-K (K=2)** - кеш страниц с ограниченной памятью вместо «прочитать всё при старте».
- **WAL group commit** - fsync-батчинг по схеме лидер/последователь снижает число синхронизаций при параллельной записи.
- **Фоновые воркеры** - отдельные потоки для flush WAL, записи грязных страниц, периодических чекпоинтов и vacuum/GC MVCC.
- **Сборщик мусора MVCC** - старые версии, невидимые ни одной активной транзакции, освобождаются автоматически.
- **Статистика `ANALYZE`** - NDV, MCV (top-K самых частых значений) и equi-depth гистограммы по колонкам; смотреть через `SHOW STATS [FOR <table>]`.
- **Cost-based оптимизатор** - оценка селективности по MCV/гистограммам, кардинальность join по NDV, жадная переупорядочка left-deep INNER-join цепочек по наименьшей кардинальности. Внешние джойны консервативно остаются в исходном порядке.
- **Чистый break** - формат хранения v0.2 несовместим с v0.1 (новый magic stamp BSDB).

### Состояние

- ✅ Хранение: 8KB slotted pages, LRU-K buffer pool, WAL с group commit, ARIES recovery
- ✅ Конкурентность: MVCC snapshot isolation, фоновый vacuum/GC, периодические чекпоинты
- ✅ Оптимизатор: cost-based перестановка джойнов на основе `ANALYZE` статистики (NDV, MCV, гистограммы)
- ✅ DDL: `CREATE/DROP TABLE`, `CREATE INDEX`, `ALTER TABLE` (ADD/DROP/RENAME COLUMN)
- ✅ DML: `INSERT` (включая `INSERT...SELECT`, `RETURNING`, `ON CONFLICT`), `UPDATE`, `DELETE`, `SELECT`
- ✅ Ограничения: `PRIMARY KEY`, `NOT NULL`, `UNIQUE`, `DEFAULT`, `CHECK`, `FOREIGN KEY`, `SERIAL`
- ✅ Запросы: `JOIN` (INNER/LEFT/RIGHT), `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT/OFFSET`, `DISTINCT`, `UNION/INTERSECT/EXCEPT`, CTE (`WITH`), подзапросы (в `FROM`, скалярные, `IN`, `EXISTS`), оконные функции (`ROW_NUMBER`, `SUM OVER`, ...)
- ✅ Выражения: `CASE WHEN`, `CAST` / `::`, `COALESCE`, `NULLIF`, `BETWEEN`, `LIKE`/`ILIKE`, `IS NULL`
- ✅ Встроенные функции: `UPPER`/`LOWER`/`LENGTH`/`SUBSTRING`/`TRIM`/`CONCAT`/`CONCAT_WS`/`SPLIT_PART`/`STARTS_WITH`/`ENDS_WITH`, `ABS`/`CEIL`/`FLOOR`/`ROUND`/`POWER`/`SQRT`, `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`/`COUNT(DISTINCT)`
- ✅ Транзакции: `BEGIN [SERIALIZABLE]`, `COMMIT`, `ROLLBACK`, MVCC snapshot isolation
- ✅ KV-режим: `KV GET/SET/DELETE/SCAN`
- ✅ Документный режим: `DOC INSERT/FIND/UPDATE/DELETE` с JSON-путями
- ✅ Utility: `SHOW TABLES/COLUMNS/CREATE TABLE/STATS`, `ANALYZE [TABLE]`, `TRUNCATE`, `EXPLAIN [ANALYZE]`, `information_schema.tables`/`columns`
- ✅ Персистентность: snapshot (binary + JSON) + WAL с восстановлением в стиле ARIES
- ✅ Производительность: zero-copy фильтр, fast top-N, fast aggregate, кеш подготовленных стейтментов
- ✅ Наблюдаемость: гистограмма латентности (p50/p95/p99/QPS), метрики GC-пауз, dead-tuple ratio, slow query log, `EXPLAIN ANALYZE`
- ✅ Durability: режимы strict / relaxed WAL, group commit, метрики fsync
- ✅ Гигиена хранения: throttled MVCC vacuum, free-space map, page compaction

### Архитектура

```
SQL → Парсер → AST → Логический план → Оптимизатор → Физический план → Executor → Результат
                                                                          │
                                  TableData ←─── MVCC Version Store
                                       │
                                       ▼
                                 B+Tree-индекс ──── WAL ──── Snapshot
```

- **bytedb-core** - примитивы хранения (tuples, schema, B+Tree, MVCC, WAL, snapshot, sequences)
- **bytedb-query** - парсер, планировщик, оптимизатор, executor, KV / Document движки
- **bytedb-server** - точка входа сервера
- **bytedb-bench** - бенчмарки

### Быстрый старт

```rust
use std::sync::Arc;
use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_query::executor::engine::{QueryEngine, ExecutionResult};

let db  = Arc::new(Database::new("mydb"));
let txn = Arc::new(TransactionManager::new());
let engine = QueryEngine::new(db, txn);

engine.execute_sql("CREATE TABLE users (id SERIAL, email TEXT UNIQUE, age INT CHECK (age >= 0))", None)?;
engine.execute_sql("INSERT INTO users (email, age) VALUES ('a@x.com', 30)", None)?;

let r = engine.execute_sql("SELECT id, email FROM users WHERE age > 18", None)?;
if let ExecutionResult::Rows { columns, rows } = r {
    println!("{:?}", columns);
    for row in rows { println!("{:?}", row); }
}
```

### SQL-справочник

#### CREATE TABLE

```sql
CREATE TABLE [IF NOT EXISTS] name (
    col ТИП [NOT NULL] [PRIMARY KEY] [UNIQUE] [DEFAULT выражение]
        [CHECK (выражение)] [REFERENCES другая_таблица(col)],
    ...,
    CHECK (выражение_таблицы),
    FOREIGN KEY (a, b) REFERENCES other(x, y)
);
```

Поддерживаемые типы - все хранятся нативно (без round-trip через строки):
- **Числовые:** `INT`/`INTEGER`/`BIGINT`/`SMALLINT`, `SERIAL` (auto-increment), `FLOAT`/`REAL`/`DOUBLE PRECISION`, `NUMERIC(p,s)`/`DECIMAL(p,s)` (fixed-point, мантисса i128)
- **Строки/бинарные:** `TEXT`, `VARCHAR(n)`, `BYTES`
- **Логический:** `BOOL`
- **Временные:** `TIMESTAMP` (мкс от эпохи), `DATE` (дни от эпохи - принимает `'YYYY-MM-DD'`)
- **Идентификатор:** `UUID` (16 raw bytes - принимает `'550e8400-e29b-41d4-a716-446655440000'`)
- **Документный:** `JSON`

Строковые литералы автоматически приводятся к типу колонки на INSERT (например `'2026-05-20'` → `Date`, `'550e8400-...'` → `Uuid`).

#### Действия внешних ключей

```sql
CREATE TABLE child (
    id INT PRIMARY KEY,
    parent_id INT REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT
);
```

Поддерживаются: `RESTRICT` (по умолчанию - блокирует операцию), `CASCADE` (каскадно удаляет/обновляет дочерние строки), `SET NULL` (обнуляет ссылку), `NO ACTION` (синоним `RESTRICT`). Для constraint на уровне таблицы: `FOREIGN KEY (cols) REFERENCES other(cols) ON DELETE CASCADE`.

#### Несколько баз данных

Каждая база живёт в своей директории `<data-dir>/databases/<имя>/` со своим каталогом и файлами таблиц.

```sql
CREATE DATABASE analytics;       -- создать новую БД (создаётся директория на диске)
USE analytics;                   -- переключиться на неё (меняется активный набор таблиц)
SHOW DATABASES;                  -- список всех БД
DROP DATABASE analytics;         -- удалить БД (нельзя удалить текущую или базу по умолчанию)
```

#### Встроенные функции даты/времени

`NOW()`, `CURRENT_TIMESTAMP` → нативный `Timestamp` (микросекунды от эпохи). `CURRENT_DATE()` → нативный `Date`. Используйте в `INSERT` / `WHERE` - executor сохранит типизированное значение, а не строку.

#### Транзакции

```sql
BEGIN [SERIALIZABLE];
-- ... запросы ...
COMMIT;   -- или ROLLBACK;
```

#### UPSERT

```sql
INSERT INTO t (id, val) VALUES (1, 'x')
ON CONFLICT (id) DO UPDATE SET val = 'x';
```

#### Оконные функции

```sql
SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) AS rn FROM emp;
```

#### CTE

```sql
WITH big AS (SELECT * FROM orders WHERE amount > 100)
SELECT * FROM big;
```

#### ANALYZE и статистика

```sql
ANALYZE TABLE users;       -- собрать NDV, MCV, гистограммы для одной таблицы
ANALYZE;                   -- проанализировать все пользовательские таблицы
SHOW STATS FOR users;      -- по колонкам: row count / null fraction / NDV / MCV
SHOW STATS;                -- сводка по всем таблицам
```

Оптимизатор использует этот каталог для оценки селективности и кардинальности джойнов. Без статистики планы возвращаются к исходному порядку - поведение совпадает с v0.1.

### Режимы хранения

- **Реляционный** - `CREATE TABLE`, `SELECT`, joins, и т.п.
- **Key-value** - `KV SET "k" "v"`, `KV GET "k"`, `KV SCAN "a" "z"`
- **Документный** - `DOC INSERT INTO logs {"level":"error"}`, `DOC FIND IN logs WHERE level = 'error'`

### Сборка и тесты

```bash
cargo build --release
cargo test
cargo bench -p bytedb-bench
```

### Физическая структура хранения

ByteDB сохраняет базу как директорию на диске. Путь задаётся флагом `--data-dir <path>` (по умолчанию `./bytedb_data`):

```
<data-dir>/
├── server.meta              # реестр всех БД
├── databases/
│   └── <имя-бд>/
│       ├── catalog.bin      # схемы, sequences, FK-метаданные
│       └── tables/
│           ├── users.tbl    # пары ключ/значение в бинарном формате
│           └── orders.tbl
├── bytedb.wal               # Write-Ahead Log (в стиле ARIES)
└── snapshots/               # опциональные периодические снапшоты
    └── snapshot_*.bin
```

- **Файлы таблиц** (`databases/<db>/tables/<table>.tbl`) - каждый зафиксированный `INSERT/UPDATE/DELETE` атомарно перезаписывает файл соответствующей таблицы (через `rename`). **Схема и данные хранятся раздельно**: `catalog.bin` для метаданных, `*.tbl` для строк. Никаких периодических флашей не нужно - файл на диске всегда актуален после успешной мутации.
- **WAL** (`bytedb.wal`) - durability для in-flight транзакций; на рестарте REDO/UNDO.
- **Snapshots** (`snapshots/`) - опциональные архивы полного состояния. По умолчанию write-threshold отключён (`--snapshot-write-threshold 0`); по времени снапшоты пишутся раз в 30 минут. Чтобы выключить полностью - `--no-snapshot`. Чтобы пропустить финальный снапшот при Ctrl+C - `--no-shutdown-snapshot`.

#### Настройка снапшотов

| Флаг | По умолчанию | Что делает |
|------|--------------|-----------|
| `--snapshot-interval-secs N` | 1800 | Снапшот каждые N секунд. `0` отключает. |
| `--snapshot-write-threshold N` | 0 | Снапшот после N записей. `0` отключает. |
| `--no-snapshot` | off | Полностью отключить фоновые снапшоты. |
| `--no-shutdown-snapshot` | off | Не делать финальный снапшот на Ctrl+C. |
| `--snapshot-format binary\|json` | binary | Формат снапшота. |

#### Создание физической базы

1. Выберите директорию. Сервер создаст её, если её нет:
   ```bash
   bytedb-server --data-dir /var/lib/bytedb --host 0.0.0.0 --port 7654
   ```
   При первом старте директория пустая → сервер инициализирует свежую пустую БД.
2. Выполните DDL через клиент:
   ```sql
   CREATE TABLE users (id SERIAL, email TEXT UNIQUE NOT NULL);
   ```
   Изменение становится durable как только вернулся `COMMIT` (или автокоммит) - оно уже в WAL.
3. Чтобы принудительно сбросить состояние в snapshot-файл, пошлите `SIGINT` (Ctrl+C) или дождитесь интервала.

#### Обновление / мутации базы

Весь DML идёт через один конвейер:

```
клиент → SQL parse → план → executor
  ↓
WAL append  (durable)         ──┐
B+Tree insert/update/delete     │
запись MVCC-версии              │── одна логическая мутация
returning rows (если есть)      │
  ↓                           ──┘
ack клиенту
```

Например, `UPDATE users SET email = 'b@x' WHERE id = 1`:
1. парсер → AST → план,
2. point-lookup строки по PK,
3. запись WAL-записи `Update { old_data, new_data }`,
4. замена значения в B+Tree (и в цепочке версий MVCC, если внутри транзакции),
5. возврат числа изменённых строк или `RETURNING`.

#### Восстановление после сбоя

Если процесс упал посреди записи, при следующем старте:

1. `SnapshotManager::load_latest()` восстанавливает последний полный снапшот.
2. `RecoveryManager::recover(&wal)` идёт по WAL вперёд от LSN снапшота:
   - **REDO** для каждой записи зафиксированной транзакции,
   - **UNDO** для каждой записи незафиксированной транзакции (rollback/abort).
3. Сервер готов принимать соединения.

В логе вы увидите:

```
WAL recovery: 42 redo, 3 undo records processed
Snapshot restored successfully
ByteDB server listening on 0.0.0.0:7654
```

#### Бэкапы

Консистентный снапшот - это один бинарный файл. Чтобы сделать бэкап:

```bash
# при работающем сервере - принудительно записать снапшот:
kill -INT $(pidof bytedb-server)   # graceful shutdown пишет финальный снапшот
cp -r /var/lib/bytedb /backup/bytedb-$(date +%F)
```

Восстановление = скопировать директорию обратно и запустить сервер с указанием на неё.

#### Формат снапшота

`--snapshot-format binary` (по умолчанию, компактный) либо `--snapshot-format json` (читаемый, больше по размеру). Оба формата хранят одни и те же данные.

### Известные ограничения

- Нет PostgreSQL wire protocol - пользуйтесь Rust-клиентом.
- Нет репликации - только single-node. (Roadmap.)
