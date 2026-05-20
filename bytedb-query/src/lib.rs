pub mod parser;
pub mod planner;
pub mod executor;
pub mod kv;
pub mod document;
pub mod error;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use bytedb_core::catalog::database::Database;
    use bytedb_core::mvcc::transaction::{TransactionManager, IsolationLevel};
    use bytedb_core::tuple::value::Value;

    use crate::parser::parser::Parser;
    use crate::parser::ast::*;
    use crate::executor::engine::{QueryEngine, ExecutionResult};
    use crate::kv::kv_engine::KvEngine;
    use crate::document::doc_engine::DocEngine;

    fn setup_engine() -> QueryEngine {
        let db = Arc::new(Database::new("test"));
        let txn = Arc::new(TransactionManager::new());
        QueryEngine::new(db, txn)
    }

    #[test]
    fn test_parse_create_table() {
        let mut parser = Parser::new("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT)").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.name, "users");
                assert_eq!(ct.columns.len(), 3);
                assert!(ct.columns[0].primary_key);
                assert!(!ct.columns[1].nullable);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_select() {
        let mut parser = Parser::new("SELECT name, email FROM users WHERE id > 10 ORDER BY name LIMIT 5").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.columns.len(), 2);
                assert!(matches!(sel.from, FromClause::Table(ref t) if t == "users"));
                assert!(sel.where_clause.is_some());
                assert_eq!(sel.order_by.len(), 1);
                assert_eq!(sel.limit, Some(5));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let mut parser = Parser::new("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.table, "users");
                assert_eq!(ins.columns, Some(vec!["id".into(), "name".into()]));
                match &ins.source {
                    crate::parser::ast::InsertSource::Values(values) => assert_eq!(values.len(), 2),
                    _ => panic!("Expected Values source"),
                }
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_kv() {
        let mut parser = Parser::new("KV SET \"mykey\" \"myvalue\"").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::KvSet(k, v) => {
                assert_eq!(k, "mykey");
                assert_eq!(v, "myvalue");
            }
            _ => panic!("Expected KvSet"),
        }

        let mut parser = Parser::new("KV GET \"mykey\"").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::KvGet(_)));
    }

    #[test]
    fn test_parse_doc_insert() {
        let mut parser = Parser::new("DOC INSERT INTO logs {\"level\": \"error\", \"msg\": \"disk full\"}").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::DocInsert(di) => {
                assert_eq!(di.collection, "logs");
                assert!(di.document.contains("error"));
            }
            _ => panic!("Expected DocInsert"),
        }
    }

    #[test]
    fn test_parse_begin_commit() {
        let mut parser = Parser::new("BEGIN SERIALIZABLE").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::Begin(Some(iso)) => {
                assert_eq!(iso, IsolationLevel::Serializable);
            }
            _ => panic!("Expected Begin Serializable"),
        }

        let mut parser = Parser::new("COMMIT").unwrap();
        assert!(matches!(parser.parse().unwrap(), Statement::Commit));
    }

    #[test]
    fn test_engine_create_and_insert() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        assert!(matches!(result, ExecutionResult::Ok(_)));

        let mut p = Parser::new("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        assert!(matches!(result, ExecutionResult::Modified { count: 1 }));

        let mut p = Parser::new("INSERT INTO users VALUES (2, 'Bob')").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        assert!(matches!(result, ExecutionResult::Modified { count: 1 }));
    }

    #[test]
    fn test_engine_select() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE items (id INT PRIMARY KEY, name TEXT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("INSERT INTO items VALUES (1, 'Apple')").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
        let mut p = Parser::new("INSERT INTO items VALUES (2, 'Banana')").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("SELECT * FROM items").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["id", "name"]);
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_engine_select_where() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE nums (id INT PRIMARY KEY, val INT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        for i in 1..=5 {
            let sql = format!("INSERT INTO nums VALUES ({}, {})", i, i * 10);
            let mut p = Parser::new(&sql).unwrap();
            engine.execute(p.parse().unwrap(), None).unwrap();
        }

        let mut p = Parser::new("SELECT * FROM nums WHERE val > 30").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_engine_update() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("INSERT INTO t VALUES (1, 'old')").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("UPDATE t SET name = 'new' WHERE id = 1").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        assert!(matches!(result, ExecutionResult::Modified { count: 1 }));

        let mut p = Parser::new("SELECT * FROM t").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[0][1], Value::Text("new".into()));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_engine_delete() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE d (id INT PRIMARY KEY)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("INSERT INTO d VALUES (1)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
        let mut p = Parser::new("INSERT INTO d VALUES (2)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("DELETE FROM d WHERE id = 1").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        assert!(matches!(result, ExecutionResult::Modified { count: 1 }));

        let mut p = Parser::new("SELECT * FROM d").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_kv_engine() {
        let kv = KvEngine::new();
        kv.set("hello", "world").unwrap();
        assert_eq!(kv.get("hello").unwrap(), Some("world".into()));
        assert_eq!(kv.get("missing").unwrap(), None);

        kv.set("a", "1").unwrap();
        kv.set("b", "2").unwrap();
        kv.set("c", "3").unwrap();

        let results = kv.scan("a", "c").unwrap();
        assert_eq!(results.len(), 3);

        assert!(kv.delete("hello").unwrap());
        assert_eq!(kv.get("hello").unwrap(), None);
    }

    #[test]
    fn test_doc_engine() {
        let doc = DocEngine::new();
        let id = doc.insert("logs", r#"{"level": "error", "msg": "disk full"}"#).unwrap();
        assert!(id.starts_with("doc_"));

        doc.insert("logs", r#"{"level": "info", "msg": "started"}"#).unwrap();

        let all = doc.find_all("logs").unwrap();
        assert_eq!(all.len(), 2);

        let errors = doc.find_by_path("logs", "level", &serde_json::json!("error")).unwrap();
        assert_eq!(errors.len(), 1);

        assert_eq!(doc.count("logs"), 2);
    }

    #[test]
    fn test_engine_join() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
        let mut p = Parser::new("INSERT INTO users VALUES (2, 'Bob')").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("INSERT INTO orders VALUES (1, 1, 100)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
        let mut p = Parser::new("INSERT INTO orders VALUES (2, 1, 200)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
        let mut p = Parser::new("INSERT INTO orders VALUES (3, 2, 50)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("SELECT * FROM orders JOIN users ON user_id = id").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { columns, rows } => {
                assert_eq!(columns.len(), 5);
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_engine_group_by() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE sales (id INT PRIMARY KEY, category TEXT, amount INT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("INSERT INTO sales VALUES (1, 'food', 10)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
        let mut p = Parser::new("INSERT INTO sales VALUES (2, 'food', 20)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
        let mut p = Parser::new("INSERT INTO sales VALUES (3, 'drink', 5)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
        let mut p = Parser::new("INSERT INTO sales VALUES (4, 'drink', 15)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("SELECT category, COUNT(id), SUM(amount) FROM sales GROUP BY category").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { columns, rows } => {
                assert_eq!(columns.len(), 3);
                assert_eq!(rows.len(), 2);
                for row in &rows {
                    match &row[0] {
                        Value::Text(cat) if cat == "food" => {
                            assert_eq!(row[1], Value::Int64(2));
                            assert_eq!(row[2], Value::Int64(30));
                        }
                        Value::Text(cat) if cat == "drink" => {
                            assert_eq!(row[1], Value::Int64(2));
                            assert_eq!(row[2], Value::Int64(20));
                        }
                        _ => panic!("Unexpected category"),
                    }
                }
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_mvcc_isolation() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        // Insert with txn1
        let mut p = Parser::new("BEGIN").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        let txn1_id = match result {
            ExecutionResult::Ok(msg) => {
                let parts: Vec<&str> = msg.split_whitespace().collect();
                parts[1].parse::<u64>().unwrap()
            }
            _ => panic!("Expected Ok"),
        };

        let mut p = Parser::new("INSERT INTO accounts VALUES (1, 1000)").unwrap();
        engine.execute(p.parse().unwrap(), Some(txn1_id)).unwrap();

        // Start txn2 - should NOT see txn1's uncommitted data
        let mut p = Parser::new("BEGIN").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        let txn2_id = match result {
            ExecutionResult::Ok(msg) => {
                let parts: Vec<&str> = msg.split_whitespace().collect();
                parts[1].parse::<u64>().unwrap()
            }
            _ => panic!("Expected Ok"),
        };

        let mut p = Parser::new("SELECT * FROM accounts").unwrap();
        let result = engine.execute(p.parse().unwrap(), Some(txn2_id)).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 0, "txn2 should not see uncommitted data from txn1");
            }
            _ => panic!("Expected Rows"),
        }

        // Commit txn1
        let mut p = Parser::new("COMMIT").unwrap();
        engine.execute(p.parse().unwrap(), Some(txn1_id)).unwrap();
    }

    #[test]
    fn test_engine_order_by_limit() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE scores (id INT PRIMARY KEY, score INT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        for i in 1..=10 {
            let sql = format!("INSERT INTO scores VALUES ({}, {})", i, (11 - i) * 10);
            let mut p = Parser::new(&sql).unwrap();
            engine.execute(p.parse().unwrap(), None).unwrap();
        }

        let mut p = Parser::new("SELECT * FROM scores ORDER BY score LIMIT 3").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0][1], Value::Int64(10));
                assert_eq!(rows[1][1], Value::Int64(20));
                assert_eq!(rows[2][1], Value::Int64(30));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_select_distinct() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE colors (id INT PRIMARY KEY, color TEXT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        for (i, color) in [(1, "red"), (2, "blue"), (3, "red"), (4, "blue"), (5, "green")] {
            let sql = format!("INSERT INTO colors VALUES ({}, '{}')", i, color);
            let mut p = Parser::new(&sql).unwrap();
            engine.execute(p.parse().unwrap(), None).unwrap();
        }

        let mut p = Parser::new("SELECT DISTINCT color FROM colors").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_case_when() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE ages (id INT PRIMARY KEY, age INT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("INSERT INTO ages VALUES (1, 25)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
        let mut p = Parser::new("INSERT INTO ages VALUES (2, 10)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("SELECT id, CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM ages").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][1], Value::Text("adult".into()));
                assert_eq!(rows[1][1], Value::Text("minor".into()));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_cast() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE nums (id INT PRIMARY KEY, val INT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("INSERT INTO nums VALUES (1, 42)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("SELECT CAST(val AS TEXT) FROM nums").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Text("42".into()));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_coalesce_nullif() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("INSERT INTO t VALUES (1, NULL)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
        let mut p = Parser::new("INSERT INTO t VALUES (2, 'hello')").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("SELECT COALESCE(name, 'default') FROM t").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Text("default".into()));
                assert_eq!(rows[1][0], Value::Text("hello".into()));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_builtin_functions() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE s (id INT PRIMARY KEY, name TEXT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("INSERT INTO s VALUES (1, 'Hello World')").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("SELECT UPPER(name), LOWER(name), LENGTH(name) FROM s").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Text("HELLO WORLD".into()));
                assert_eq!(rows[0][1], Value::Text("hello world".into()));
                assert_eq!(rows[0][2], Value::Int64(11));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_cast_double_colon() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE n (id INT PRIMARY KEY, val INT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("INSERT INTO n VALUES (1, 99)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("SELECT val::TEXT FROM n").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Text("99".into()));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_aggregate_without_group_by() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE counts (id INT PRIMARY KEY, val INT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        for i in 1..=5 {
            let sql = format!("INSERT INTO counts VALUES ({}, {})", i, i * 10);
            let mut p = Parser::new(&sql).unwrap();
            engine.execute(p.parse().unwrap(), None).unwrap();
        }

        let mut p = Parser::new("SELECT COUNT(*), SUM(val), AVG(val) FROM counts").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Int64(5));
                assert_eq!(rows[0][1], Value::Int64(150));
                assert_eq!(rows[0][2], Value::Float64(30.0));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_insert_select() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE src (id INT PRIMARY KEY, name TEXT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
        let mut p = Parser::new("CREATE TABLE dst (id INT PRIMARY KEY, name TEXT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("INSERT INTO src VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("INSERT INTO dst SELECT * FROM src").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        assert!(matches!(result, ExecutionResult::Modified { count: 3 }));

        let mut p = Parser::new("SELECT * FROM dst").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_union() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
        let mut p = Parser::new("CREATE TABLE t2 (id INT PRIMARY KEY, name TEXT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();
        let mut p = Parser::new("INSERT INTO t2 VALUES (2, 'b'), (3, 'c')").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("SELECT * FROM t1 UNION ALL SELECT * FROM t2").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 4);
            }
            _ => panic!("Expected Rows"),
        }

        let mut p = Parser::new("SELECT * FROM t1 UNION SELECT * FROM t2").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_alter_table() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE alt (id INT PRIMARY KEY, name TEXT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("ALTER TABLE alt ADD COLUMN age INT").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        assert!(matches!(result, ExecutionResult::Ok(_)));

        let mut p = Parser::new("ALTER TABLE alt RENAME COLUMN name TO full_name").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        assert!(matches!(result, ExecutionResult::Ok(_)));

        let mut p = Parser::new("ALTER TABLE alt DROP COLUMN age").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        assert!(matches!(result, ExecutionResult::Ok(_)));
    }

    #[test]
    fn test_not_null_constraint() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE strict (id INT PRIMARY KEY, name TEXT NOT NULL)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("INSERT INTO strict VALUES (1, NULL)").unwrap();
        let result = engine.execute(p.parse().unwrap(), None);
        assert!(result.is_err());

        let mut p = Parser::new("INSERT INTO strict VALUES (1, 'valid')").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        assert!(matches!(result, ExecutionResult::Modified { count: 1 }));
    }

    #[test]
    fn test_like() {
        let engine = setup_engine();

        let mut p = Parser::new("CREATE TABLE words (id INT PRIMARY KEY, word TEXT)").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("INSERT INTO words VALUES (1, 'hello'), (2, 'help'), (3, 'world')").unwrap();
        engine.execute(p.parse().unwrap(), None).unwrap();

        let mut p = Parser::new("SELECT * FROM words WHERE word LIKE 'hel%'").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_returning() {
        let engine = setup_engine();

        engine.execute_sql("CREATE TABLE ret_test (id INT PRIMARY KEY, name TEXT)", None).unwrap();

        let result = engine.execute_sql("INSERT INTO ret_test VALUES (1, 'alice'), (2, 'bob') RETURNING *", None).unwrap();
        match result {
            ExecutionResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["id", "name"]);
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][1], Value::Text("alice".into()));
            }
            _ => panic!("Expected Rows"),
        }

        let result = engine.execute_sql("UPDATE ret_test SET name = 'ALICE' WHERE id = 1 RETURNING id, name", None).unwrap();
        match result {
            ExecutionResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["id", "name"]);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], Value::Text("ALICE".into()));
            }
            _ => panic!("Expected Rows"),
        }

        let result = engine.execute_sql("DELETE FROM ret_test WHERE id = 2 RETURNING *", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], Value::Text("bob".into()));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_on_conflict() {
        let engine = setup_engine();

        engine.execute_sql("CREATE TABLE upsert_test (id INT PRIMARY KEY, val TEXT)", None).unwrap();
        engine.execute_sql("INSERT INTO upsert_test VALUES (1, 'first')", None).unwrap();

        engine.execute_sql("INSERT INTO upsert_test VALUES (1, 'second') ON CONFLICT (id) DO NOTHING", None).unwrap();
        let result = engine.execute_sql("SELECT val FROM upsert_test WHERE id = 1", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Text("first".into()));
            }
            _ => panic!("Expected Rows"),
        }

        engine.execute_sql("INSERT INTO upsert_test VALUES (1, 'updated') ON CONFLICT (id) DO UPDATE SET val = 'updated'", None).unwrap();
        let result = engine.execute_sql("SELECT val FROM upsert_test WHERE id = 1", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Text("updated".into()));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_ilike() {
        let engine = setup_engine();

        engine.execute_sql("CREATE TABLE ilike_test (id INT PRIMARY KEY, name TEXT)", None).unwrap();
        engine.execute_sql("INSERT INTO ilike_test VALUES (1, 'Hello'), (2, 'WORLD'), (3, 'help')", None).unwrap();

        let result = engine.execute_sql("SELECT * FROM ilike_test WHERE name ILIKE 'hel%'", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }

        let result = engine.execute_sql("SELECT * FROM ilike_test WHERE name LIKE 'hel%'", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_cte() {
        let engine = setup_engine();

        engine.execute_sql("CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT, amount INT)", None).unwrap();
        engine.execute_sql("INSERT INTO orders VALUES (1, 'alice', 100), (2, 'bob', 200), (3, 'alice', 150)", None).unwrap();

        let result = engine.execute_sql(
            "WITH high_orders AS (SELECT * FROM orders WHERE amount > 120) SELECT * FROM high_orders",
            None
        ).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_subquery_in_from() {
        let engine = setup_engine();

        engine.execute_sql("CREATE TABLE sales (id INT PRIMARY KEY, product TEXT, amount INT)", None).unwrap();
        engine.execute_sql("INSERT INTO sales VALUES (1, 'a', 100), (2, 'b', 200), (3, 'a', 300)", None).unwrap();

        let result = engine.execute_sql(
            "SELECT * FROM (SELECT * FROM sales WHERE amount > 150) AS big_sales",
            None
        ).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_window_functions() {
        let engine = setup_engine();

        engine.execute_sql("CREATE TABLE emp (id INT PRIMARY KEY, dept TEXT, salary INT)", None).unwrap();
        engine.execute_sql("INSERT INTO emp VALUES (1, 'eng', 100), (2, 'eng', 200), (3, 'sales', 150), (4, 'sales', 250), (5, 'eng', 300)", None).unwrap();

        let result = engine.execute_sql(
            "SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary ASC) AS rn FROM emp",
            None
        ).unwrap();
        match result {
            ExecutionResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["id", "dept", "rn"]);
                assert_eq!(rows.len(), 5);
                for row in &rows {
                    if row[0] == Value::Int64(1) {
                        assert_eq!(row[2], Value::Int64(1));
                    }
                    if row[0] == Value::Int64(5) {
                        assert_eq!(row[2], Value::Int64(3));
                    }
                }
            }
            _ => panic!("Expected Rows"),
        }

        let result = engine.execute_sql(
            "SELECT id, SUM(salary) OVER (PARTITION BY dept ORDER BY id ASC) AS running_sum FROM emp",
            None
        ).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 5);
                for row in &rows {
                    if row[0] == Value::Int64(2) {
                        assert_eq!(row[1], Value::Float64(300.0));
                    }
                }
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_explain() {
        let engine = setup_engine();

        engine.execute_sql("CREATE TABLE ex_test (id INT PRIMARY KEY, name TEXT)", None).unwrap();

        let result = engine.execute_sql("EXPLAIN SELECT * FROM ex_test WHERE id = 1", None).unwrap();
        match result {
            ExecutionResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["QUERY PLAN"]);
                assert!(!rows.is_empty());
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_nulls_first_last() {
        let engine = setup_engine();

        engine.execute_sql("CREATE TABLE nulls_test (id INT PRIMARY KEY, val INT)", None).unwrap();
        engine.execute_sql("INSERT INTO nulls_test VALUES (1, 10), (2, NULL), (3, 30)", None).unwrap();

        let result = engine.execute_sql("SELECT * FROM nulls_test ORDER BY val ASC NULLS FIRST", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[0][1], Value::Null);
            }
            _ => panic!("Expected Rows"),
        }

        let result = engine.execute_sql("SELECT * FROM nulls_test ORDER BY val ASC NULLS LAST", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[2][1], Value::Null);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_information_schema() {
        let engine = setup_engine();

        engine.execute_sql("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)", None).unwrap();
        engine.execute_sql("CREATE TABLE t2 (id INT PRIMARY KEY, val INT)", None).unwrap();

        let result = engine.execute_sql("SELECT * FROM information_schema.tables", None).unwrap();
        match result {
            ExecutionResult::Rows { columns, rows } => {
                assert!(columns.contains(&"table_name".to_string()));
                assert!(rows.len() >= 2);
            }
            _ => panic!("Expected Rows"),
        }

        let result = engine.execute_sql("SELECT * FROM information_schema.columns WHERE table_name = 't1'", None).unwrap();
        match result {
            ExecutionResult::Rows { columns, rows } => {
                assert!(columns.contains(&"column_name".to_string()));
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_intersect_except() {
        let engine = setup_engine();

        engine.execute_sql("CREATE TABLE set_a (id INT PRIMARY KEY, val INT)", None).unwrap();
        engine.execute_sql("CREATE TABLE set_b (id INT PRIMARY KEY, val INT)", None).unwrap();
        engine.execute_sql("INSERT INTO set_a VALUES (1, 10), (2, 20), (3, 30)", None).unwrap();
        engine.execute_sql("INSERT INTO set_b VALUES (2, 20), (3, 30), (4, 40)", None).unwrap();

        let result = engine.execute_sql("SELECT val FROM set_a INTERSECT SELECT val FROM set_b", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }

        let result = engine.execute_sql("SELECT val FROM set_a EXCEPT SELECT val FROM set_b", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Int64(10));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_table_alias() {
        let engine = setup_engine();

        engine.execute_sql("CREATE TABLE employees (id INT PRIMARY KEY, name TEXT NOT NULL, dept_id INT)", None).unwrap();
        engine.execute_sql("CREATE TABLE departments (id INT PRIMARY KEY, dept_name TEXT NOT NULL)", None).unwrap();
        engine.execute_sql("INSERT INTO employees VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Carol', 10)", None).unwrap();
        engine.execute_sql("INSERT INTO departments VALUES (10, 'Engineering'), (20, 'Sales')", None).unwrap();

        // Single table alias
        let result = engine.execute_sql("SELECT e.name FROM employees AS e WHERE e.id = 1", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Text("Alice".into()));
            }
            _ => panic!("Expected Rows"),
        }

        // JOIN with aliases
        let result = engine.execute_sql(
            "SELECT e.name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.id WHERE d.dept_name = 'Engineering'",
            None
        ).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                let names: Vec<&Value> = rows.iter().map(|r| &r[0]).collect();
                assert!(names.contains(&&Value::Text("Alice".into())));
                assert!(names.contains(&&Value::Text("Carol".into())));
            }
            _ => panic!("Expected Rows"),
        }

        // Qualified column without alias (using table name directly)
        let result = engine.execute_sql(
            "SELECT employees.name FROM employees WHERE employees.id = 2",
            None
        ).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Text("Bob".into()));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_having() {
        let engine = setup_engine();

        engine.execute_sql("CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT NOT NULL, amount INT)", None).unwrap();
        engine.execute_sql("INSERT INTO orders VALUES (1, 'Alice', 100), (2, 'Alice', 200), (3, 'Bob', 150), (4, 'Carol', 50), (5, 'Carol', 75), (6, 'Carol', 25)", None).unwrap();

        // HAVING with COUNT
        let result = engine.execute_sql(
            "SELECT customer, COUNT(*) FROM orders GROUP BY customer HAVING COUNT(*) > 1",
            None
        ).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                let customers: Vec<&Value> = rows.iter().map(|r| &r[0]).collect();
                assert!(customers.contains(&&Value::Text("Alice".into())));
                assert!(customers.contains(&&Value::Text("Carol".into())));
            }
            _ => panic!("Expected Rows"),
        }

        // HAVING with SUM
        let result = engine.execute_sql(
            "SELECT customer, SUM(amount) FROM orders GROUP BY customer HAVING SUM(amount) > 150",
            None
        ).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Text("Alice".into()));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_default_values() {
        let engine = setup_engine();

        engine.execute_sql(
            "CREATE TABLE products (id INT PRIMARY KEY, name TEXT NOT NULL, status TEXT DEFAULT 'active', quantity INT DEFAULT 0)",
            None
        ).unwrap();

        // Insert with all columns
        engine.execute_sql("INSERT INTO products VALUES (1, 'Widget', 'sold', 5)", None).unwrap();

        // Insert with partial columns - defaults should fill in
        engine.execute_sql("INSERT INTO products (id, name) VALUES (2, 'Gadget')", None).unwrap();

        let result = engine.execute_sql("SELECT id, name, status, quantity FROM products WHERE id = 1", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[0][2], Value::Text("sold".into()));
                assert_eq!(rows[0][3], Value::Int64(5));
            }
            _ => panic!("Expected Rows"),
        }

        let result = engine.execute_sql("SELECT id, name, status, quantity FROM products WHERE id = 2", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[0][2], Value::Text("active".into()));
                assert_eq!(rows[0][3], Value::Int64(0));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_count_distinct() {
        let engine = setup_engine();

        engine.execute_sql("CREATE TABLE visits (id INT PRIMARY KEY, user_name TEXT NOT NULL, page TEXT NOT NULL)", None).unwrap();
        engine.execute_sql("INSERT INTO visits VALUES (1, 'Alice', '/home'), (2, 'Alice', '/about'), (3, 'Bob', '/home'), (4, 'Alice', '/home')", None).unwrap();

        let result = engine.execute_sql("SELECT COUNT(DISTINCT user_name) FROM visits", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Int64(2));
            }
            _ => panic!("Expected Rows"),
        }

        let result = engine.execute_sql("SELECT page, COUNT(DISTINCT user_name) FROM visits GROUP BY page", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                for row in &rows {
                    match &row[0] {
                        Value::Text(p) if p == "/home" => assert_eq!(row[1], Value::Int64(2)),
                        Value::Text(p) if p == "/about" => assert_eq!(row[1], Value::Int64(1)),
                        _ => {}
                    }
                }
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_string_functions() {
        let engine = setup_engine();

        engine.execute_sql("CREATE TABLE strs (id INT PRIMARY KEY, s TEXT NOT NULL)", None).unwrap();
        engine.execute_sql("INSERT INTO strs VALUES (1, 'hello,world,foo'), (2, 'apple-banana-cherry')", None).unwrap();

        let result = engine.execute_sql("SELECT CONCAT_WS('-', 'a', 'b', 'c'), STARTS_WITH('hello', 'he'), ENDS_WITH('world', 'ld'), SPLIT_PART(s, ',', 2) FROM strs WHERE id = 1", None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Text("a-b-c".into()));
                assert_eq!(rows[0][1], Value::Bool(true));
                assert_eq!(rows[0][2], Value::Bool(true));
                assert_eq!(rows[0][3], Value::Text("world".into()));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_unique_constraint() {
        let engine = setup_engine();
        engine.execute_sql("CREATE TABLE u (id INT PRIMARY KEY, email TEXT UNIQUE)", None).unwrap();
        engine.execute_sql("INSERT INTO u VALUES (1, 'a@x.com')", None).unwrap();
        let r = engine.execute_sql("INSERT INTO u VALUES (2, 'a@x.com')", None);
        assert!(r.is_err(), "duplicate UNIQUE value should fail");
        engine.execute_sql("INSERT INTO u VALUES (3, 'b@x.com')", None).unwrap();
    }

    #[test]
    fn test_check_constraint() {
        let engine = setup_engine();
        engine.execute_sql("CREATE TABLE c (id INT PRIMARY KEY, age INT, CHECK (age >= 0))", None).unwrap();
        engine.execute_sql("INSERT INTO c VALUES (1, 25)", None).unwrap();
        let r = engine.execute_sql("INSERT INTO c VALUES (2, -5)", None);
        assert!(r.is_err(), "CHECK violation should fail");
    }

    #[test]
    fn test_foreign_key() {
        let engine = setup_engine();
        engine.execute_sql("CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)", None).unwrap();
        engine.execute_sql("CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id))", None).unwrap();
        engine.execute_sql("INSERT INTO parent VALUES (1, 'p')", None).unwrap();
        engine.execute_sql("INSERT INTO child VALUES (10, 1)", None).unwrap();
        let r = engine.execute_sql("INSERT INTO child VALUES (11, 99)", None);
        assert!(r.is_err(), "FK pointing to missing parent should fail");
    }

    #[test]
    fn test_native_date_type() {
        let engine = setup_engine();
        engine.execute_sql("CREATE TABLE events (id INT PRIMARY KEY, dt DATE)", None).unwrap();
        engine.execute_sql("INSERT INTO events VALUES (1, '2026-05-20')", None).unwrap();
        let r = engine.execute_sql("SELECT dt FROM events", None).unwrap();
        match r {
            ExecutionResult::Rows { rows, .. } => {
                assert!(matches!(rows[0][0], Value::Date(_)));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_native_uuid_type() {
        let engine = setup_engine();
        engine.execute_sql("CREATE TABLE u (id INT PRIMARY KEY, uid UUID)", None).unwrap();
        engine.execute_sql("INSERT INTO u VALUES (1, '550e8400-e29b-41d4-a716-446655440000')", None).unwrap();
        let r = engine.execute_sql("SELECT uid FROM u", None).unwrap();
        match r {
            ExecutionResult::Rows { rows, .. } => {
                assert!(matches!(rows[0][0], Value::Uuid(_)));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_native_decimal_type() {
        let engine = setup_engine();
        engine.execute_sql("CREATE TABLE prices (id INT PRIMARY KEY, p NUMERIC(10,2))", None).unwrap();
        engine.execute_sql("INSERT INTO prices VALUES (1, '123.45')", None).unwrap();
        engine.execute_sql("INSERT INTO prices VALUES (2, 99)", None).unwrap();
        let r = engine.execute_sql("SELECT p FROM prices ORDER BY id", None).unwrap();
        match r {
            ExecutionResult::Rows { rows, .. } => {
                assert!(matches!(rows[0][0], Value::Decimal(_, _)));
                assert!(matches!(rows[1][0], Value::Decimal(_, _)));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_fk_cascade_delete() {
        let engine = setup_engine();
        engine.execute_sql("CREATE TABLE p (id INT PRIMARY KEY)", None).unwrap();
        engine.execute_sql("CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id) ON DELETE CASCADE)", None).unwrap();
        engine.execute_sql("INSERT INTO p VALUES (1), (2)", None).unwrap();
        engine.execute_sql("INSERT INTO c VALUES (10, 1), (11, 1), (12, 2)", None).unwrap();
        engine.execute_sql("DELETE FROM p WHERE id = 1", None).unwrap();
        let r = engine.execute_sql("SELECT id FROM c", None).unwrap();
        match r {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Int64(12));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_fk_set_null_delete() {
        let engine = setup_engine();
        engine.execute_sql("CREATE TABLE p (id INT PRIMARY KEY)", None).unwrap();
        engine.execute_sql("CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id) ON DELETE SET NULL)", None).unwrap();
        engine.execute_sql("INSERT INTO p VALUES (1)", None).unwrap();
        engine.execute_sql("INSERT INTO c VALUES (10, 1)", None).unwrap();
        engine.execute_sql("DELETE FROM p WHERE id = 1", None).unwrap();
        let r = engine.execute_sql("SELECT pid FROM c WHERE id = 10", None).unwrap();
        match r {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Null);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_multi_database() {
        let engine = setup_engine();
        engine.execute_sql("CREATE DATABASE analytics", None).unwrap();
        engine.execute_sql("CREATE DATABASE IF NOT EXISTS analytics", None).unwrap();
        let r = engine.execute_sql("SHOW DATABASES", None).unwrap();
        match r {
            ExecutionResult::Rows { rows, .. } => {
                assert!(rows.iter().any(|r| matches!(&r[0], Value::Text(s) if s == "analytics")));
                assert!(rows.iter().any(|r| matches!(&r[0], Value::Text(s) if s == "test")));
            }
            _ => panic!("Expected Rows"),
        }
        engine.execute_sql("USE analytics", None).unwrap();
        let drop_current = engine.execute_sql("DROP DATABASE analytics", None);
        assert!(drop_current.is_err(), "cannot drop current db");
        engine.execute_sql("USE test", None).unwrap();
        engine.execute_sql("DROP DATABASE analytics", None).unwrap();
    }

    #[test]
    fn test_unique_violation_on_update() {
        let engine = setup_engine();
        engine.execute_sql("CREATE TABLE u (id INT PRIMARY KEY, email TEXT UNIQUE)", None).unwrap();
        engine.execute_sql("INSERT INTO u VALUES (1, 'a@x.com')", None).unwrap();
        engine.execute_sql("INSERT INTO u VALUES (2, 'b@x.com')", None).unwrap();
        let r = engine.execute_sql("UPDATE u SET email = 'a@x.com' WHERE id = 2", None);
        assert!(r.is_err(), "updating to a duplicate UNIQUE value should fail");
        // Updating row to its own value should work
        engine.execute_sql("UPDATE u SET email = 'b@x.com' WHERE id = 2", None).unwrap();
    }

    #[test]
    fn test_check_violation_on_update() {
        let engine = setup_engine();
        engine.execute_sql("CREATE TABLE c (id INT PRIMARY KEY, age INT, CHECK (age >= 0))", None).unwrap();
        engine.execute_sql("INSERT INTO c VALUES (1, 10)", None).unwrap();
        let r = engine.execute_sql("UPDATE c SET age = -1 WHERE id = 1", None);
        assert!(r.is_err(), "UPDATE that violates CHECK should fail");
    }

    #[test]
    fn test_fk_violation_on_update() {
        let engine = setup_engine();
        engine.execute_sql("CREATE TABLE p (id INT PRIMARY KEY)", None).unwrap();
        engine.execute_sql("CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id))", None).unwrap();
        engine.execute_sql("INSERT INTO p VALUES (1), (2)", None).unwrap();
        engine.execute_sql("INSERT INTO c VALUES (10, 1)", None).unwrap();
        let r = engine.execute_sql("UPDATE c SET pid = 99 WHERE id = 10", None);
        assert!(r.is_err(), "UPDATE pointing to missing parent should fail");
        engine.execute_sql("UPDATE c SET pid = 2 WHERE id = 10", None).unwrap();
    }

    #[test]
    fn test_fk_restrict_on_delete() {
        let engine = setup_engine();
        engine.execute_sql("CREATE TABLE p (id INT PRIMARY KEY)", None).unwrap();
        engine.execute_sql("CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id))", None).unwrap();
        engine.execute_sql("INSERT INTO p VALUES (1)", None).unwrap();
        engine.execute_sql("INSERT INTO c VALUES (10, 1)", None).unwrap();
        let r = engine.execute_sql("DELETE FROM p WHERE id = 1", None);
        assert!(r.is_err(), "DELETE of referenced parent should fail (RESTRICT)");
        // Remove child first, then parent succeeds
        engine.execute_sql("DELETE FROM c WHERE id = 10", None).unwrap();
        engine.execute_sql("DELETE FROM p WHERE id = 1", None).unwrap();
    }

    #[test]
    fn test_serial_autoincrement() {
        let engine = setup_engine();
        engine.execute_sql("CREATE TABLE s (id SERIAL, name TEXT)", None).unwrap();
        engine.execute_sql("INSERT INTO s (name) VALUES ('a')", None).unwrap();
        engine.execute_sql("INSERT INTO s (name) VALUES ('b')", None).unwrap();
        let r = engine.execute_sql("SELECT id, name FROM s", None).unwrap();
        match r {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], Value::Int64(1));
                assert_eq!(rows[1][0], Value::Int64(2));
            }
            _ => panic!("Expected Rows"),
        }
    }
}
