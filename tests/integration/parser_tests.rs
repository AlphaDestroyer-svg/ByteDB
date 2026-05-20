#[cfg(test)]
mod tests {
    use bytedb_query::parser::parser::Parser;
    use bytedb_query::parser::ast::*;

    #[test]
    fn test_parse_create_table() {
        let mut parser = Parser::new("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT)").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.name, "users");
                assert_eq!(ct.columns.len(), 3);
                assert_eq!(ct.columns[0].name, "id");
                assert!(ct.columns[0].primary_key);
                assert_eq!(ct.columns[1].name, "name");
                assert!(!ct.columns[1].nullable);
                assert_eq!(ct.columns[2].name, "email");
                assert!(ct.columns[2].nullable);
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
                assert_eq!(sel.from, "users");
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
                assert_eq!(ins.values.len(), 2);
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_kv_operations() {
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
        match stmt {
            Statement::KvGet(k) => assert_eq!(k, "mykey"),
            _ => panic!("Expected KvGet"),
        }
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
                assert_eq!(iso, bytedb_core::mvcc::transaction::IsolationLevel::Serializable);
            }
            _ => panic!("Expected Begin Serializable"),
        }

        let mut parser = Parser::new("COMMIT").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Commit));
    }

    #[test]
    fn test_parse_update() {
        let mut parser = Parser::new("UPDATE users SET name = 'Charlie' WHERE id = 1").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Update(upd) => {
                assert_eq!(upd.table, "users");
                assert_eq!(upd.assignments.len(), 1);
                assert_eq!(upd.assignments[0].0, "name");
                assert!(upd.where_clause.is_some());
            }
            _ => panic!("Expected Update"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let mut parser = Parser::new("DELETE FROM users WHERE id = 5").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Delete(del) => {
                assert_eq!(del.table, "users");
                assert!(del.where_clause.is_some());
            }
            _ => panic!("Expected Delete"),
        }
    }
}
