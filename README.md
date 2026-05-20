# ByteDB

A hybrid storage engine in Rust: relational SQL + key-value + document, with MVCC, WAL/ARIES recovery, B+Tree indexes, and a vectorized executor.

> 📖 Doc language: [English](#english) · [Русский](#русский)

---

## English

### Status

- ✅ DDL: `CREATE/DROP TABLE`, `CREATE INDEX`, `ALTER TABLE` (ADD/DROP/RENAME COLUMN)
- ✅ DML: `INSERT` (incl. `INSERT...SELECT`, `RETURNING`, `ON CONFLICT`), `UPDATE`, `DELETE`, `SELECT`
- ✅ Constraints: `PRIMARY KEY`, `NOT NULL`, `UNIQUE`, `DEFAULT`, `CHECK`, `FOREIGN KEY`, `SERIAL`
- ✅ Queries: `JOIN` (INNER/LEFT/RIGHT), `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT/OFFSET`, `DISTINCT`, `UNION/INTERSECT/EXCEPT`, CTEs (`WITH`), subqueries (in `FROM`, scalar, `IN`, `EXISTS`), window functions (`ROW_NUMBER`, `SUM OVER`, ...)
- ✅ Expressions: `CASE WHEN`, `CAST` / `::`, `COALESCE`, `NULLIF`, `BETWEEN`, `LIKE`/`ILIKE`, `IS NULL`
- ✅ Built-ins: `UPPER`/`LOWER`/`LENGTH`/`SUBSTRING`/`TRIM`/`CONCAT`/`CONCAT_WS`/`SPLIT_PART`/`STARTS_WITH`/`ENDS_WITH`, `ABS`/`CEIL`/`FLOOR`/`ROUND`/`POWER`/`SQRT`, `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`/`COUNT(DISTINCT)`
- ✅ Transactions: `BEGIN [SERIALIZABLE]`, `COMMIT`, `ROLLBACK`, MVCC snapshot isolation
- ✅ KV mode: `KV GET/SET/DELETE/SCAN`
- ✅ Document mode: `DOC INSERT/FIND/UPDATE/DELETE` with JSON path
- ✅ Utility: `SHOW TABLES/COLUMNS/CREATE TABLE`, `TRUNCATE`, `EXPLAIN [ANALYZE]`, `information_schema.tables`/`columns`
- ✅ Persistence: snapshot (binary + JSON) + WAL with ARIES-style recovery
- ✅ Performance: zero-copy filter, fast top-N, fast aggregate, prepared-statement cache

### Architecture

```
SQL → Parser → AST → Logical Plan → Optimizer → Physical Plan → Executor → Result
                                                                  │
                                  TableData ←─── MVCC Version Store
                                       │
                                       ▼
                                 B+Tree index ──── WAL ──── Snapshot
```

- **bytedb-core** — storage primitives (tuples, schema, B+Tree, MVCC, WAL, snapshot, sequences)
- **bytedb-query** — parser, planner, optimizer, executor, KV / Document engines
- **bytedb-server** — server entrypoint
- **bytedb-bench** — benchmarks

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

Supported types: `INT`/`INTEGER`/`BIGINT`/`SMALLINT`, `SERIAL`, `FLOAT`/`REAL`/`DOUBLE PRECISION`/`NUMERIC`, `TEXT`/`VARCHAR(n)`, `BOOL`, `BYTES`, `JSON`, `TIMESTAMP`, `DATE`, `UUID`.

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

### Storage modes

- **Relational** — `CREATE TABLE`, `SELECT`, joins, etc.
- **Key-value** — `KV SET "k" "v"`, `KV GET "k"`, `KV SCAN "a" "z"`
- **Document** — `DOC INSERT INTO logs {"level":"error"}`, `DOC FIND IN logs WHERE level = 'error'`

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
├── bytedb.wal               # Write-Ahead Log (ARIES-style)
└── snapshots/
    ├── snapshot_0001.bin    # binary snapshot, monotonic id
    ├── snapshot_0002.bin
    └── ...
```

- **WAL** (`bytedb.wal`) — every `INSERT/UPDATE/DELETE/COMMIT/ROLLBACK` is appended here first. Used to redo committed and undo uncommitted work on restart.
- **Snapshots** (`snapshots/`) — full materialized state of all tables, written periodically (`--snapshot-interval-secs`, default 300s) and after `--snapshot-write-threshold` writes (default 100000), and on graceful shutdown (Ctrl+C). On boot the latest snapshot is loaded, then WAL replay is applied on top.

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
   The change is durable as soon as `COMMIT` (or autocommit) returns — it lives in WAL.
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
- `Date`, `Timestamp`, `Decimal`, `Uuid` are parsed but stored as Int64/Text/Float64 internally.
- `RESTRICT`/`CASCADE` on foreign keys not yet honored — FK is enforced on INSERT only.
- No multi-database; one `Database` instance per server.
- No replication.

---

## Русский

### Состояние

- ✅ DDL: `CREATE/DROP TABLE`, `CREATE INDEX`, `ALTER TABLE` (ADD/DROP/RENAME COLUMN)
- ✅ DML: `INSERT` (включая `INSERT...SELECT`, `RETURNING`, `ON CONFLICT`), `UPDATE`, `DELETE`, `SELECT`
- ✅ Ограничения: `PRIMARY KEY`, `NOT NULL`, `UNIQUE`, `DEFAULT`, `CHECK`, `FOREIGN KEY`, `SERIAL`
- ✅ Запросы: `JOIN` (INNER/LEFT/RIGHT), `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT/OFFSET`, `DISTINCT`, `UNION/INTERSECT/EXCEPT`, CTE (`WITH`), подзапросы (в `FROM`, скалярные, `IN`, `EXISTS`), оконные функции (`ROW_NUMBER`, `SUM OVER`, ...)
- ✅ Выражения: `CASE WHEN`, `CAST` / `::`, `COALESCE`, `NULLIF`, `BETWEEN`, `LIKE`/`ILIKE`, `IS NULL`
- ✅ Встроенные функции: `UPPER`/`LOWER`/`LENGTH`/`SUBSTRING`/`TRIM`/`CONCAT`/`CONCAT_WS`/`SPLIT_PART`/`STARTS_WITH`/`ENDS_WITH`, `ABS`/`CEIL`/`FLOOR`/`ROUND`/`POWER`/`SQRT`, `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`/`COUNT(DISTINCT)`
- ✅ Транзакции: `BEGIN [SERIALIZABLE]`, `COMMIT`, `ROLLBACK`, MVCC snapshot isolation
- ✅ KV-режим: `KV GET/SET/DELETE/SCAN`
- ✅ Документный режим: `DOC INSERT/FIND/UPDATE/DELETE` с JSON-путями
- ✅ Utility: `SHOW TABLES/COLUMNS/CREATE TABLE`, `TRUNCATE`, `EXPLAIN [ANALYZE]`, `information_schema.tables`/`columns`
- ✅ Персистентность: snapshot (binary + JSON) + WAL с восстановлением в стиле ARIES
- ✅ Производительность: zero-copy фильтр, fast top-N, fast aggregate, кеш подготовленных стейтментов

### Архитектура

```
SQL → Парсер → AST → Логический план → Оптимизатор → Физический план → Executor → Результат
                                                                          │
                                  TableData ←─── MVCC Version Store
                                       │
                                       ▼
                                 B+Tree-индекс ──── WAL ──── Snapshot
```

- **bytedb-core** — примитивы хранения (tuples, schema, B+Tree, MVCC, WAL, snapshot, sequences)
- **bytedb-query** — парсер, планировщик, оптимизатор, executor, KV / Document движки
- **bytedb-server** — точка входа сервера
- **bytedb-bench** — бенчмарки

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

Поддерживаемые типы: `INT`/`INTEGER`/`BIGINT`/`SMALLINT`, `SERIAL`, `FLOAT`/`REAL`/`DOUBLE PRECISION`/`NUMERIC`, `TEXT`/`VARCHAR(n)`, `BOOL`, `BYTES`, `JSON`, `TIMESTAMP`, `DATE`, `UUID`.

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

### Режимы хранения

- **Реляционный** — `CREATE TABLE`, `SELECT`, joins, и т.п.
- **Key-value** — `KV SET "k" "v"`, `KV GET "k"`, `KV SCAN "a" "z"`
- **Документный** — `DOC INSERT INTO logs {"level":"error"}`, `DOC FIND IN logs WHERE level = 'error'`

### Сборка и тесты

```bash
cargo build --release
cargo test
cargo bench -p bytedb-bench
```

### Известные ограничения

- Нет PostgreSQL wire protocol — пользуйтесь Rust-клиентом.
- `Date`, `Timestamp`, `Decimal`, `Uuid` парсятся, но хранятся как Int64/Text/Float64.
- `RESTRICT`/`CASCADE` для внешних ключей пока не учитываются — FK проверяется только на INSERT.
- Нет multi-database; один экземпляр `Database` на сервер.
- Нет репликации.
