#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use bytedb_core::catalog::database::Database;
    use bytedb_core::mvcc::transaction::TransactionManager;
    use bytedb_query::executor::engine::{QueryEngine, ExecutionResult};
    use bytedb_query::parser::parser::Parser;
    use bytedb_core::tuple::value::Value;

    fn setup_engine() -> QueryEngine {
        let db = Arc::new(Database::new("test"));
        let txn = Arc::new(TransactionManager::new());
        QueryEngine::new(db, txn)
    }

    #[test]
    fn test_create_table_and_insert() {
        let engine = setup_engine();

        let mut parser = Parser::new("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)").unwrap();
        let stmt = parser.parse().unwrap();
        let result = engine.execute(stmt, None).unwrap();
        assert!(matches!(result, ExecutionResult::Ok(_)));

        let mut parser = Parser::new("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        let stmt = parser.parse().unwrap();
        let result = engine.execute(stmt, None).unwrap();
        assert!(matches!(result, ExecutionResult::Modified { count: 1 }));

        let mut parser = Parser::new("INSERT INTO users VALUES (2, 'Bob')").unwrap();
        let stmt = parser.parse().unwrap();
        let result = engine.execute(stmt, None).unwrap();
        assert!(matches!(result, ExecutionResult::Modified { count: 1 }));
    }

    #[test]
    fn test_select_all() {
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
    fn test_select_with_where() {
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
    fn test_update() {
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
    fn test_delete() {
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
    fn test_transactions() {
        let engine = setup_engine();

        let mut p = Parser::new("BEGIN").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Ok(msg) => {
                assert!(msg.contains("Transaction"));
                assert!(msg.contains("started"));
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_scalar_subquery() {
        let engine = setup_engine();
        engine.execute(Parser::new("CREATE TABLE outer_t (id INT, val INT)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("CREATE TABLE inner_t (id INT, x INT)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO outer_t VALUES (1, 10)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO outer_t VALUES (2, 20)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO inner_t VALUES (1, 100)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO inner_t VALUES (2, 200)").unwrap().parse(), None).unwrap();

        let mut p = Parser::new("SELECT id, (SELECT x FROM inner_t WHERE id = outer_t.id) FROM outer_t").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, columns } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_exists() {
        let engine = setup_engine();
        engine.execute(Parser::new("CREATE TABLE t1 (id INT)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("CREATE TABLE t2 (id INT)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO t1 VALUES (1)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO t1 VALUES (2)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO t2 VALUES (1)").unwrap().parse(), None).unwrap();

        let mut p = Parser::new("SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_in_subquery() {
        let engine = setup_engine();
        engine.execute(Parser::new("CREATE TABLE orders (customer_id INT, amount INT)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("CREATE TABLE customers (id INT, name TEXT)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO customers VALUES (1, 'Alice')").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO customers VALUES (2, 'Bob')").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO orders VALUES (1, 100)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO orders VALUES (1, 200)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO orders VALUES (2, 50)").unwrap().parse(), None).unwrap();

        let mut p = Parser::new("SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE amount > 150)").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, columns } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(columns[0], "name");
                assert_eq!(rows[0][0], Value::Text("Alice".to_string()));
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_distinct() {
        let engine = setup_engine();
        engine.execute(Parser::new("CREATE TABLE items (id INT, tag TEXT)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO items VALUES (1, 'a')").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO items VALUES (2, 'b')").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO items VALUES (3, 'a')").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO items VALUES (4, 'b')").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO items VALUES (5, 'a')").unwrap().parse(), None).unwrap();

        let mut p = Parser::new("SELECT DISTINCT tag FROM items").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, columns } => {
                assert_eq!(columns, vec!["tag"]);
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }
    }

    #[test]
    fn test_case_when() {
        let engine = setup_engine();
        engine.execute(Parser::new("CREATE TABLE products (id INT, name TEXT, price INT)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO products VALUES (1, 'Apple', 50)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO products VALUES (2, 'Pear', 150)").unwrap().parse(), None).unwrap();
        engine.execute(Parser::new("INSERT INTO products VALUES (3, 'Banana', 250)").unwrap().parse(), None).unwrap();

        let mut p = Parser::new("SELECT name, CASE WHEN price < 100 THEN 'cheap' WHEN price < 200 THEN 'medium' ELSE 'expensive' END AS category FROM products").unwrap();
        let result = engine.execute(p.parse().unwrap(), None).unwrap();
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("Expected Rows"),
        }
    }
}
