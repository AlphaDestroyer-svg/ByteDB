use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytedb_core::catalog::database::Database;
use bytedb_core::mvcc::transaction::TransactionManager;
use bytedb_core::tuple::value::Value;
use bytedb_query::executor::diskstore::DiskStore;
use bytedb_query::executor::engine::{ExecutionResult, QueryEngine};

fn temp_dir(tag: &str) -> PathBuf {
    let mut d = std::env::temp_dir();
    d.push(format!("bytedb_blob_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn open(dir: &Path, threshold: usize) -> QueryEngine {
    let ds = DiskStore::open(dir.to_path_buf(), "test").unwrap();
    let db = Arc::new(Database::new("test"));
    let txn = Arc::new(TransactionManager::new());
    let mut e = QueryEngine::new(db, txn);
    e.attach_disk_store(ds);
    e.set_blob_config(threshold, 8 * 1024 * 1024);
    e
}

fn big_text(n: usize, seed: u8) -> String {
    let mut s = String::with_capacity(n);
    let bytes = b"abcdefghijklmnopqrstuvwxyz0123456789";
    for i in 0..n {
        s.push(bytes[(i + seed as usize) % bytes.len()] as char);
    }
    s
}

fn one_text(res: ExecutionResult) -> Option<String> {
    match res {
        ExecutionResult::Rows { rows, .. } => match rows.into_iter().next()?.into_iter().next()? {
            Value::Text(v) => Some(v),
            _ => None,
        },
        _ => None,
    }
}

fn rows_len(res: &ExecutionResult) -> usize {
    match res {
        ExecutionResult::Rows { rows, .. } => rows.len(),
        _ => 0,
    }
}

fn blob_count(dir: &Path) -> usize {
    let blobs = dir.join("databases").join("test").join("blobs");
    std::fs::read_dir(&blobs)
        .map(|rd| rd.filter_map(|e| e.ok()).filter(|e| {
            e.path().extension().map(|x| x == "blob").unwrap_or(false)
        }).count())
        .unwrap_or(0)
}

const THRESHOLD: usize = 65537;
const BIG: usize = 100_000;

#[test]
fn large_value_round_trips_through_blob() {
    let dir = temp_dir("roundtrip");
    let e = open(&dir, THRESHOLD);
    let body = big_text(BIG, 1);

    e.execute_sql("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)", None).unwrap();
    e.execute_sql(&format!("INSERT INTO docs VALUES (1, '{}')", body), None).unwrap();

    assert_eq!(blob_count(&dir), 1, "one blob file must be written for the spilled value");

    let got = one_text(e.execute_sql("SELECT body FROM docs WHERE id = 1", None).unwrap());
    assert_eq!(got.as_deref(), Some(body.as_str()), "spilled value must round-trip byte-for-byte");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn read_paths_return_spilled_rows() {
    let dir = temp_dir("readpaths");
    let e = open(&dir, THRESHOLD);
    let a = big_text(BIG, 2);
    let b = big_text(BIG, 3);

    e.execute_sql("CREATE TABLE docs (id INT PRIMARY KEY, tag INT, body TEXT)", None).unwrap();
    e.execute_sql(&format!("INSERT INTO docs VALUES (1, 10, '{}')", a), None).unwrap();
    e.execute_sql(&format!("INSERT INTO docs VALUES (2, 20, '{}')", b), None).unwrap();

    // Full scan projecting the large column.
    let full = e.execute_sql("SELECT id, body FROM docs", None).unwrap();
    assert_eq!(rows_len(&full), 2);

    // Filter on a small column (fast-path predicate) still yields the spilled body.
    let filtered = e.execute_sql("SELECT body FROM docs WHERE tag = 20", None).unwrap();
    assert_eq!(one_text(filtered).as_deref(), Some(b.as_str()));

    // Column pruning that excludes the large column must not error and must return both rows.
    let pruned = e.execute_sql("SELECT id FROM docs", None).unwrap();
    assert_eq!(rows_len(&pruned), 2);

    // PK point lookup.
    let point = e.execute_sql("SELECT body FROM docs WHERE id = 1", None).unwrap();
    assert_eq!(one_text(point).as_deref(), Some(a.as_str()));

    // ORDER BY a small column with a large column present.
    let ordered = e.execute_sql("SELECT id FROM docs ORDER BY tag DESC LIMIT 1", None).unwrap();
    match ordered {
        ExecutionResult::Rows { rows, .. } => {
            assert_eq!(rows[0][0], Value::Int64(2));
        }
        _ => panic!("expected rows"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn update_small_column_preserves_large_value() {
    let dir = temp_dir("update_small");
    let e = open(&dir, THRESHOLD);
    let body = big_text(BIG, 4);

    e.execute_sql("CREATE TABLE docs (id INT PRIMARY KEY, tag INT, body TEXT)", None).unwrap();
    e.execute_sql(&format!("INSERT INTO docs VALUES (1, 10, '{}')", body), None).unwrap();
    e.execute_sql("UPDATE docs SET tag = 99 WHERE id = 1", None).unwrap();

    let got = one_text(e.execute_sql("SELECT body FROM docs WHERE id = 1", None).unwrap());
    assert_eq!(got.as_deref(), Some(body.as_str()), "large value must survive update of another column");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn update_large_value_round_trips_new_content() {
    let dir = temp_dir("update_large");
    let e = open(&dir, THRESHOLD);
    let v1 = big_text(BIG, 5);
    let v2 = big_text(BIG + 5000, 6);

    e.execute_sql("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)", None).unwrap();
    e.execute_sql(&format!("INSERT INTO docs VALUES (1, '{}')", v1), None).unwrap();
    e.execute_sql(&format!("UPDATE docs SET body = '{}' WHERE id = 1", v2), None).unwrap();

    let got = one_text(e.execute_sql("SELECT body FROM docs WHERE id = 1", None).unwrap());
    assert_eq!(got.as_deref(), Some(v2.as_str()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn spilled_value_survives_eviction_and_reload() {
    let dir = temp_dir("evict_reload");
    let e = open(&dir, THRESHOLD);
    let body = big_text(BIG, 7);

    e.execute_sql("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)", None).unwrap();
    e.execute_sql(&format!("INSERT INTO docs VALUES (1, '{}')", body), None).unwrap();
    e.execute_sql("CREATE TABLE other (id INT PRIMARY KEY)", None).unwrap();
    e.execute_sql("INSERT INTO other VALUES (1)", None).unwrap();

    // Make `docs` the coldest and evict it; its in-RAM index (with the blob ref) is dropped.
    e.execute_sql("SELECT id FROM other", None).unwrap();
    let evicted = e.evict_cold_tables(1);
    assert!(evicted >= 1, "expected at least one table evicted");
    assert!(!e.tables().read().contains_key("docs"), "docs should have been evicted");

    // Reload from the on-disk log rebuilds the tag-22 ref; resolving reads the persisted blob.
    let got = one_text(e.execute_sql("SELECT body FROM docs WHERE id = 1", None).unwrap());
    assert_eq!(got.as_deref(), Some(body.as_str()), "spilled value must survive eviction + reload");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn transaction_reads_spilled_value() {
    let dir = temp_dir("txn");
    let e = open(&dir, THRESHOLD);
    let body = big_text(BIG, 8);

    e.execute_sql("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)", None).unwrap();
    e.execute_sql(&format!("INSERT INTO docs VALUES (1, '{}')", body), None).unwrap();

    let begin = e.execute_sql("BEGIN", None).unwrap();
    let tid: u64 = match begin {
        ExecutionResult::Ok(m) => m.split_whitespace().nth(1).unwrap().parse().unwrap(),
        _ => panic!("BEGIN did not return a transaction id"),
    };
    let got = one_text(e.execute_sql("SELECT body FROM docs WHERE id = 1", Some(tid)).unwrap());
    assert_eq!(got.as_deref(), Some(body.as_str()), "committed spilled row must be visible in a transaction");
    e.execute_sql("COMMIT", Some(tid)).unwrap();

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn secondary_index_lookup_returns_spilled_row() {
    let dir = temp_dir("secidx");
    let e = open(&dir, THRESHOLD);
    let body = big_text(BIG, 9);

    e.execute_sql("CREATE TABLE docs (id INT PRIMARY KEY, tag INT, body TEXT)", None).unwrap();
    e.execute_sql(&format!("INSERT INTO docs VALUES (1, 42, '{}')", body), None).unwrap();
    e.execute_sql("CREATE INDEX idx_tag ON docs (tag)", None).unwrap();

    let got = one_text(e.execute_sql("SELECT body FROM docs WHERE tag = 42", None).unwrap());
    assert_eq!(got.as_deref(), Some(body.as_str()), "secondary index scan must return the spilled row intact");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn old_inline_data_reads_after_enabling_spill() {
    let dir = temp_dir("compat");
    let body = big_text(BIG, 15);

    {
        let e = open(&dir, 0);
        e.execute_sql("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)", None).unwrap();
        e.execute_sql(&format!("INSERT INTO docs VALUES (1, '{}')", body), None).unwrap();
        assert_eq!(blob_count(&dir), 0, "spill disabled: the value must stay inline on disk");
    }

    {
        let e = open(&dir, THRESHOLD);
        let got = one_text(e.execute_sql("SELECT body FROM docs WHERE id = 1", None).unwrap());
        assert_eq!(
            got.as_deref(),
            Some(body.as_str()),
            "inline data written before spill was enabled must still read correctly"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn gc_reclaims_orphaned_blob_after_update() {
    let dir = temp_dir("gc_orphan");
    let e = open(&dir, THRESHOLD);
    let v1 = big_text(BIG, 11);
    let v2 = big_text(BIG + 3000, 12);

    e.execute_sql("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)", None).unwrap();
    e.execute_sql(&format!("INSERT INTO docs VALUES (1, '{}')", v1), None).unwrap();
    e.execute_sql(&format!("UPDATE docs SET body = '{}' WHERE id = 1", v2), None).unwrap();

    // Two blobs exist: the superseded v1 blob and the live v2 blob.
    assert_eq!(blob_count(&dir), 2, "update should create a second blob, orphaning the first");

    // Grace = 0 in this single-threaded test: reclaim the orphan, keep the live blob.
    let removed = e.gc_blobs(0);
    assert_eq!(removed, 1, "exactly the orphaned blob must be reclaimed");
    assert_eq!(blob_count(&dir), 1, "the live blob must remain");

    let got = one_text(e.execute_sql("SELECT body FROM docs WHERE id = 1", None).unwrap());
    assert_eq!(got.as_deref(), Some(v2.as_str()), "the live value must still resolve after GC");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn gc_keeps_blob_referenced_by_evicted_table() {
    let dir = temp_dir("gc_evicted");
    let e = open(&dir, THRESHOLD);
    let body = big_text(BIG, 13);

    e.execute_sql("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)", None).unwrap();
    e.execute_sql(&format!("INSERT INTO docs VALUES (1, '{}')", body), None).unwrap();
    e.execute_sql("CREATE TABLE other (id INT PRIMARY KEY)", None).unwrap();
    e.execute_sql("INSERT INTO other VALUES (1)", None).unwrap();

    e.execute_sql("SELECT id FROM other", None).unwrap();
    let evicted = e.evict_cold_tables(1);
    assert!(evicted >= 1);
    assert!(!e.tables().read().contains_key("docs"), "docs should be evicted");

    // The blob is only referenced by the evicted table's on-disk rows: GC must NOT delete it.
    let removed = e.gc_blobs(0);
    assert_eq!(removed, 0, "a blob referenced by an evicted table must survive GC");
    assert_eq!(blob_count(&dir), 1);

    let got = one_text(e.execute_sql("SELECT body FROM docs WHERE id = 1", None).unwrap());
    assert_eq!(got.as_deref(), Some(body.as_str()), "evicted table's spilled value must still resolve");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn gc_keeps_live_blob() {
    let dir = temp_dir("gc_live");
    let e = open(&dir, THRESHOLD);
    let body = big_text(BIG, 14);

    e.execute_sql("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)", None).unwrap();
    e.execute_sql(&format!("INSERT INTO docs VALUES (1, '{}')", body), None).unwrap();

    let removed = e.gc_blobs(0);
    assert_eq!(removed, 0, "a referenced blob must never be reclaimed");
    assert_eq!(blob_count(&dir), 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn small_values_are_not_spilled() {
    let dir = temp_dir("small");
    let e = open(&dir, THRESHOLD);

    e.execute_sql("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)", None).unwrap();
    e.execute_sql("INSERT INTO docs VALUES (1, 'short value')", None).unwrap();

    assert_eq!(blob_count(&dir), 0, "values under the threshold must stay inline");
    let got = one_text(e.execute_sql("SELECT body FROM docs WHERE id = 1", None).unwrap());
    assert_eq!(got.as_deref(), Some("short value"));

    let _ = std::fs::remove_dir_all(&dir);
}
