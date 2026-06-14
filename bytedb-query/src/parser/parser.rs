use bytedb_core::mvcc::transaction::IsolationLevel;
use bytedb_core::tuple::value::DataType;

use crate::error::{QueryError, Result};
use super::ast::*;
use super::lexer::Lexer;
use super::token::Token;

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    last_data_type_serial: bool,
}

impl Parser {
    pub fn new(input: &str) -> Result<Self> {
        let mut lexer = Lexer::new(input);
        let tokens = lexer.tokenize()?;
        Ok(Parser { tokens, pos: 0, last_data_type_serial: false })
    }

    fn previous_data_type_serial(&self) -> bool {
        self.last_data_type_serial
    }

    pub fn parse(&mut self) -> Result<Statement> {
        let stmt = self.parse_statement()?;
        let stmt = self.maybe_parse_union(stmt)?;
        if self.current() == &Token::Semicolon {
            self.advance();
        }
        Ok(stmt)
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        match self.current().clone() {
            Token::Explain => self.parse_explain(),
            Token::With => self.parse_with(),
            Token::Select => self.parse_select(),
            Token::Insert => self.parse_insert(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Alter => self.parse_alter(),
            Token::Begin => self.parse_begin(),
            Token::Commit => { self.advance(); Ok(Statement::Commit) }
            Token::Rollback => { self.advance(); Ok(Statement::Rollback) }
            Token::Kv => self.parse_kv(),
            Token::Doc => self.parse_doc(),
            Token::Show => self.parse_show(),
            Token::Truncate => self.parse_truncate(),
            Token::Use => {
                self.advance();
                if self.current() == &Token::Database { self.advance(); }
                let name = self.expect_ident()?;
                Ok(Statement::UseDatabase(name))
            }
            Token::Analyze => {
                self.advance();

                if let Token::Ident(s) = self.current() {
                    if s.eq_ignore_ascii_case("TABLE") {
                        self.advance();
                    }
                }
                let table = match self.current() {
                    Token::Ident(_) => Some(self.expect_ident()?),
                    _ => None,
                };
                Ok(Statement::Analyze(table))
            }
            Token::Backup => self.parse_backup(),
            Token::Restore => self.parse_restore(),
            Token::Migrate => { self.advance(); Ok(Statement::Migrate) }
            _ => Err(QueryError::Parse(format!("Unexpected token: {:?}", self.current()))),
        }
    }

    fn parse_backup(&mut self) -> Result<Statement> {
        self.expect(Token::Backup)?;
        self.expect(Token::To)?;
        let path = self.expect_string()?;
        Ok(Statement::Backup { path })
    }

    fn parse_restore(&mut self) -> Result<Statement> {
        self.expect(Token::Restore)?;
        self.expect(Token::From)?;
        let path = self.expect_string()?;
        let to_lsn = if self.current() == &Token::To {
            self.advance();
            self.expect(Token::Lsn)?;
            match self.current().clone() {
                Token::IntLit(n) => { self.advance(); Some(n as u64) }
                t => return Err(QueryError::Parse(format!("expected LSN integer, got {:?}", t))),
            }
        } else {
            None
        };
        Ok(Statement::Restore { path, to_lsn })
    }

    fn parse_explain(&mut self) -> Result<Statement> {
        self.expect(Token::Explain)?;
        let analyze = if self.current() == &Token::Analyze {
            self.advance();
            true
        } else {
            false
        };
        let stmt = self.parse_statement()?;
        Ok(Statement::Explain(Box::new(stmt), analyze))
    }

    fn parse_with(&mut self) -> Result<Statement> {
        self.expect(Token::With)?;
        if self.current() == &Token::Recursive {
            self.advance();
        }

        let mut ctes = Vec::new();
        loop {
            let name = self.expect_ident()?;
            self.expect(Token::As)?;
            self.expect(Token::LParen)?;
            self.expect(Token::Select)?;
            let query = self.parse_select_body()?;
            self.expect(Token::RParen)?;
            ctes.push(Cte { name, query });
            if self.current() != &Token::Comma { break; }
            self.advance();
        }

        self.expect(Token::Select)?;
        let mut select = self.parse_select_body()?;
        select.ctes = ctes;
        Ok(Statement::Select(select))
    }

    fn parse_select(&mut self) -> Result<Statement> {
        self.expect(Token::Select)?;

        let distinct = if self.current() == &Token::Distinct {
            self.advance();
            true
        } else {
            false
        };

        let columns = self.parse_select_columns()?;
        self.expect(Token::From)?;
        let (from, from_alias) = self.parse_from_clause()?;

        let mut joins = Vec::new();
        while matches!(self.current(), Token::Join | Token::Inner | Token::Left | Token::Right) {
            joins.push(self.parse_join()?);
        }

        let where_clause = if self.current() == &Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.current() == &Token::Group {
            self.advance();
            self.expect(Token::By)?;
            self.parse_expr_list()?
        } else {
            Vec::new()
        };

        let having = if self.current() == &Token::Having {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.current() == &Token::Order {
            self.advance();
            self.expect(Token::By)?;
            self.parse_order_by()?
        } else {
            Vec::new()
        };

        let limit = if self.current() == &Token::Limit {
            self.advance();
            Some(self.expect_integer()? as usize)
        } else {
            None
        };

        let offset = if self.current() == &Token::Offset {
            self.advance();
            Some(self.expect_integer()? as usize)
        } else {
            None
        };

        Ok(Statement::Select(SelectStmt {
            columns,
            distinct,
            from,
            from_alias,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            ctes: Vec::new(),
        }))
    }

    fn parse_select_body(&mut self) -> Result<SelectStmt> {
        let distinct = if self.current() == &Token::Distinct {
            self.advance();
            true
        } else {
            false
        };

        let columns = self.parse_select_columns()?;
        self.expect(Token::From)?;
        let (from, from_alias) = self.parse_from_clause()?;

        let mut joins = Vec::new();
        while matches!(self.current(), Token::Join | Token::Inner | Token::Left | Token::Right) {
            joins.push(self.parse_join()?);
        }

        let where_clause = if self.current() == &Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.current() == &Token::Group {
            self.advance();
            self.expect(Token::By)?;
            self.parse_expr_list()?
        } else {
            Vec::new()
        };

        let having = if self.current() == &Token::Having {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.current() == &Token::Order {
            self.advance();
            self.expect(Token::By)?;
            self.parse_order_by()?
        } else {
            Vec::new()
        };

        let limit = if self.current() == &Token::Limit {
            self.advance();
            Some(self.expect_integer()? as usize)
        } else {
            None
        };

        let offset = if self.current() == &Token::Offset {
            self.advance();
            Some(self.expect_integer()? as usize)
        } else {
            None
        };

        Ok(SelectStmt {
            columns,
            distinct,
            from,
            from_alias,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            ctes: Vec::new(),
        })
    }

    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>> {
        let mut columns = Vec::new();

        if self.current() == &Token::Star {
            self.advance();
            columns.push(SelectColumn::Star);
            return Ok(columns);
        }

        loop {
            let expr = self.parse_expr()?;
            let alias = if self.current() == &Token::As {
                self.advance();
                Some(self.expect_ident()?)
            } else {
                None
            };
            columns.push(SelectColumn::Expr(expr, alias));

            if self.current() != &Token::Comma {
                break;
            }
            self.advance();
        }

        Ok(columns)
    }

    fn parse_from_clause(&mut self) -> Result<(FromClause, Option<String>)> {
        if self.current() == &Token::LParen {
            self.advance();
            self.expect(Token::Select)?;
            let subquery = self.parse_select_body()?;
            self.expect(Token::RParen)?;
            let alias = self.try_parse_alias();
            Ok((FromClause::Subquery(Box::new(subquery)), alias))
        } else {
            let mut table = self.expect_ident()?;
            if self.current() == &Token::Dot {
                self.advance();
                let sub = self.expect_ident()?;
                table = format!("{}.{}", table, sub);
            }
            let alias = self.try_parse_alias();
            Ok((FromClause::Table(table), alias))
        }
    }

    fn parse_join(&mut self) -> Result<JoinClause> {
        let join_type = match self.current().clone() {
            Token::Inner => { self.advance(); self.expect(Token::Join)?; JoinType::Inner }
            Token::Left => { self.advance(); self.expect(Token::Join)?; JoinType::Left }
            Token::Right => { self.advance(); self.expect(Token::Join)?; JoinType::Right }
            Token::Join => { self.advance(); JoinType::Inner }
            _ => return Err(QueryError::Parse("Expected JOIN".into())),
        };

        let table = self.expect_ident()?;
        let alias = if self.current() != &Token::On {
            self.try_parse_alias()
        } else {
            None
        };
        self.expect(Token::On)?;
        let condition = self.parse_expr()?;

        Ok(JoinClause { join_type, table, alias, condition })
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderByExpr>> {
        let mut exprs = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let ascending = if self.current() == &Token::Desc {
                self.advance();
                false
            } else {
                if self.current() == &Token::Asc {
                    self.advance();
                }
                true
            };
            let nulls_first = if self.current() == &Token::Nulls {
                self.advance();
                if self.current() == &Token::First {
                    self.advance();
                    Some(true)
                } else {
                    self.expect(Token::Last)?;
                    Some(false)
                }
            } else {
                None
            };
            exprs.push(OrderByExpr { expr, ascending, nulls_first });

            if self.current() != &Token::Comma {
                break;
            }
            self.advance();
        }
        Ok(exprs)
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;
        let table = self.expect_ident()?;

        let columns = if self.current() == &Token::LParen {
            self.advance();
            let cols = self.parse_ident_list()?;
            self.expect(Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        let source = if self.current() == &Token::Select {
            self.advance();
            let select = self.parse_select_body()?;
            InsertSource::Select(select)
        } else {
            self.expect(Token::Values)?;
            let mut values = Vec::new();
            loop {
                self.expect(Token::LParen)?;
                let row = self.parse_expr_list()?;
                self.expect(Token::RParen)?;
                values.push(row);

                if self.current() != &Token::Comma {
                    break;
                }
                self.advance();
            }
            InsertSource::Values(values)
        };

        let on_conflict = if self.current() == &Token::On {
            self.advance();
            self.expect(Token::Conflict)?;
            self.expect(Token::LParen)?;
            let cols = self.parse_ident_list()?;
            self.expect(Token::RParen)?;
            self.expect(Token::Do)?;
            let action = if self.current() == &Token::Nothing {
                self.advance();
                ConflictAction::DoNothing
            } else {
                self.expect(Token::Update)?;
                self.expect(Token::Set)?;
                let mut assignments = Vec::new();
                loop {
                    let col = self.expect_ident()?;
                    self.expect(Token::Eq)?;
                    let expr = self.parse_expr()?;
                    assignments.push((col, expr));
                    if self.current() != &Token::Comma { break; }
                    self.advance();
                }
                ConflictAction::DoUpdate(assignments)
            };
            Some(OnConflict { columns: cols, action })
        } else {
            None
        };

        let returning = self.parse_returning()?;

        Ok(Statement::Insert(InsertStmt { table, columns, source, on_conflict, returning }))
    }

    fn parse_update(&mut self) -> Result<Statement> {
        self.expect(Token::Update)?;
        let table = self.expect_ident()?;
        self.expect(Token::Set)?;

        let mut assignments = Vec::new();
        loop {
            let col = self.expect_ident()?;
            self.expect(Token::Eq)?;
            let expr = self.parse_expr()?;
            assignments.push((col, expr));

            if self.current() != &Token::Comma {
                break;
            }
            self.advance();
        }

        let where_clause = if self.current() == &Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let returning = self.parse_returning()?;

        Ok(Statement::Update(UpdateStmt { table, assignments, where_clause, returning }))
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect(Token::Delete)?;
        self.expect(Token::From)?;
        let table = self.expect_ident()?;

        let where_clause = if self.current() == &Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let returning = self.parse_returning()?;

        Ok(Statement::Delete(DeleteStmt { table, where_clause, returning }))
    }

    fn parse_returning(&mut self) -> Result<Option<Vec<SelectColumn>>> {
        if self.current() == &Token::Returning {
            self.advance();
            let cols = self.parse_select_columns()?;
            Ok(Some(cols))
        } else {
            Ok(None)
        }
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.expect(Token::Create)?;

        if self.current() == &Token::Unique {
            self.advance();
            self.expect(Token::Index)?;
            return self.parse_create_index(true);
        }

        match self.current().clone() {
            Token::Table => self.parse_create_table(),
            Token::Index => { self.advance(); self.parse_create_index(false) }
            Token::Database => {
                self.advance();
                let if_not_exists = if self.current() == &Token::If {
                    self.advance();
                    self.expect(Token::Not)?;
                    self.expect(Token::Exists)?;
                    true
                } else { false };
                let name = self.expect_ident()?;
                Ok(Statement::CreateDatabase { name, if_not_exists })
            }
            _ => Err(QueryError::Parse("Expected TABLE, INDEX or DATABASE after CREATE".into())),
        }
    }

    fn parse_create_table(&mut self) -> Result<Statement> {
        self.expect(Token::Table)?;

        let if_not_exists = if self.current() == &Token::If {
            self.advance();
            self.expect(Token::Not)?;
            self.expect(Token::Exists)?;
            true
        } else {
            false
        };

        let name = self.expect_ident()?;
        self.expect(Token::LParen)?;

        let mut columns = Vec::new();
        let mut table_checks: Vec<Expr> = Vec::new();
        let mut table_fks: Vec<ForeignKeyDef> = Vec::new();
        loop {

            if self.current() == &Token::Unique || self.current() == &Token::Primary
                || matches!(self.current(), Token::Ident(s) if s.eq_ignore_ascii_case("CHECK") || s.eq_ignore_ascii_case("CONSTRAINT") || s.eq_ignore_ascii_case("FOREIGN"))
            {

                if matches!(self.current(), Token::Ident(s) if s.eq_ignore_ascii_case("CONSTRAINT")) {
                    self.advance();
                    let _ = self.expect_ident()?;
                }
                if matches!(self.current(), Token::Ident(s) if s.eq_ignore_ascii_case("CHECK")) {
                    self.advance();
                    self.expect(Token::LParen)?;
                    let e = self.parse_expr()?;
                    self.expect(Token::RParen)?;
                    table_checks.push(e);
                } else if matches!(self.current(), Token::Ident(s) if s.eq_ignore_ascii_case("FOREIGN")) {
                    self.advance();
                    self.expect(Token::Key)?;
                    self.expect(Token::LParen)?;
                    let cols = self.parse_ident_list()?;
                    self.expect(Token::RParen)?;

                    if !matches!(self.current(), Token::Ident(s) if s.eq_ignore_ascii_case("REFERENCES")) {
                        return Err(QueryError::Parse("Expected REFERENCES".into()));
                    }
                    self.advance();
                    let ref_table = self.expect_ident()?;
                    self.expect(Token::LParen)?;
                    let ref_cols = self.parse_ident_list()?;
                    self.expect(Token::RParen)?;
                    let (on_delete, on_update) = self.parse_fk_actions()?;
                    table_fks.push(ForeignKeyDef {
                        columns: cols,
                        ref_table,
                        ref_columns: ref_cols,
                        on_delete,
                        on_update,
                    });
                } else {
                    return Err(QueryError::Parse(format!("Unexpected constraint token: {:?}", self.current())));
                }
                if self.current() != &Token::Comma { break; }
                self.advance();
                continue;
            }

            let col_name = self.expect_ident()?;
            let (data_type, max_length) = self.parse_data_type()?;

            let mut nullable = true;
            let mut primary_key = false;
            let mut unique = false;
            let mut auto_increment = false;
            let mut default = None;
            let mut check = None;
            let mut references = None;
            let mut col_on_delete = FkAction::Restrict;
            let mut col_on_update = FkAction::Restrict;

            if matches!(data_type, bytedb_core::tuple::value::DataType::Int64)
                && false
            {}

            loop {
                match self.current().clone() {
                    Token::Not => {
                        self.advance();
                        self.expect(Token::NullLit)?;
                        nullable = false;
                    }
                    Token::Primary => {
                        self.advance();
                        self.expect(Token::Key)?;
                        primary_key = true;
                        nullable = false;
                    }
                    Token::Unique => {
                        self.advance();
                        unique = true;
                    }
                    Token::Default => {
                        self.advance();
                        default = Some(self.parse_primary()?);
                    }
                    Token::Ident(ref s) if s.eq_ignore_ascii_case("CHECK") => {
                        self.advance();
                        self.expect(Token::LParen)?;
                        check = Some(self.parse_expr()?);
                        self.expect(Token::RParen)?;
                    }
                    Token::Ident(ref s) if s.eq_ignore_ascii_case("REFERENCES") => {
                        self.advance();
                        let ref_table = self.expect_ident()?;
                        let ref_col = if self.current() == &Token::LParen {
                            self.advance();
                            let c = self.expect_ident()?;
                            self.expect(Token::RParen)?;
                            c
                        } else {
                            "id".to_string()
                        };
                        references = Some((ref_table, ref_col));
                        let (od, ou) = self.parse_fk_actions()?;
                        col_on_delete = od;
                        col_on_update = ou;
                    }
                    _ => break,
                }
            }

            if matches!(self.previous_data_type_serial(), true) {
                auto_increment = true;
                primary_key = true;
                nullable = false;
            }

            columns.push(ColumnDef {
                name: col_name,
                data_type,
                max_length,
                nullable,
                primary_key,
                unique,
                auto_increment,
                default,
                check,
                references,
                on_delete: col_on_delete,
                on_update: col_on_update,
            });

            if self.current() != &Token::Comma {
                break;
            }
            self.advance();
        }

        self.expect(Token::RParen)?;

        Ok(Statement::CreateTable(CreateTableStmt {
            name,
            columns,
            if_not_exists,
            check_constraints: table_checks,
            foreign_keys: table_fks,
        }))
    }

    fn parse_create_index(&mut self, unique: bool) -> Result<Statement> {
        let name = self.expect_ident()?;
        self.expect(Token::On)?;
        let table = self.expect_ident()?;
        self.expect(Token::LParen)?;
        let columns = self.parse_ident_list()?;
        self.expect(Token::RParen)?;

        Ok(Statement::CreateIndex(CreateIndexStmt { name, table, columns, unique }))
    }

    fn parse_drop(&mut self) -> Result<Statement> {
        self.expect(Token::Drop)?;
        match self.current().clone() {
            Token::Table => {
                self.advance();
                let if_exists = if self.current() == &Token::If {
                    self.advance();
                    self.expect(Token::Exists)?;
                    true
                } else {
                    false
                };
                let name = self.expect_ident()?;
                Ok(Statement::DropTable(DropTableStmt { name, if_exists }))
            }
            Token::Index => {
                self.advance();
                let name = self.expect_ident()?;
                Ok(Statement::DropIndex(name))
            }
            Token::Database => {
                self.advance();
                let if_exists = if self.current() == &Token::If {
                    self.advance();
                    self.expect(Token::Exists)?;
                    true
                } else { false };
                let name = self.expect_ident()?;
                Ok(Statement::DropDatabase { name, if_exists })
            }
            _ => Err(QueryError::Parse("Expected TABLE, INDEX or DATABASE after DROP".into())),
        }
    }

    fn parse_alter(&mut self) -> Result<Statement> {
        self.expect(Token::Alter)?;
        self.expect(Token::Table)?;
        let table = self.expect_ident()?;

        let action = match self.current().clone() {
            Token::Add => {
                self.advance();
                if self.current() == &Token::Column {
                    self.advance();
                }
                let col_name = self.expect_ident()?;
                let (data_type, max_length) = self.parse_data_type()?;
                let mut nullable = true;
                let mut primary_key = false;
                while matches!(self.current(), Token::Not | Token::Primary | Token::Unique) {
                    match self.current().clone() {
                        Token::Not => {
                            self.advance();
                            self.expect(Token::NullLit)?;
                            nullable = false;
                        }
                        Token::Primary => {
                            self.advance();
                            self.expect(Token::Key)?;
                            primary_key = true;
                            nullable = false;
                        }
                        _ => break,
                    }
                }
                AlterTableAction::AddColumn(ColumnDef {
                    name: col_name,
                    data_type,
                    max_length,
                    nullable,
                    primary_key,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    check: None,
                    references: None,
                    on_delete: FkAction::Restrict,
                    on_update: FkAction::Restrict,
                })
            }
            Token::Drop => {
                self.advance();
                if self.current() == &Token::Column {
                    self.advance();
                }
                let col_name = self.expect_ident()?;
                AlterTableAction::DropColumn(col_name)
            }
            Token::Rename => {
                self.advance();
                if self.current() == &Token::Column {
                    self.advance();
                }
                let old_name = self.expect_ident()?;

                match self.current().clone() {
                    Token::To => { self.advance(); }
                    Token::Ident(s) if s.to_uppercase() == "TO" => { self.advance(); }
                    _ => return Err(QueryError::Parse("Expected TO after column name in RENAME".into())),
                }
                let new_name = self.expect_ident()?;
                AlterTableAction::RenameColumn { old_name, new_name }
            }
            _ => return Err(QueryError::Parse("Expected ADD, DROP, or RENAME after ALTER TABLE <name>".into())),
        };

        Ok(Statement::AlterTable(AlterTableStmt { table, action }))
    }

    fn parse_begin(&mut self) -> Result<Statement> {
        self.expect(Token::Begin)?;
        let isolation = if self.current() == &Token::Serializable {
            self.advance();
            Some(IsolationLevel::Serializable)
        } else {
            None
        };
        Ok(Statement::Begin(isolation))
    }

    fn maybe_parse_union(&mut self, left: Statement) -> Result<Statement> {
        match self.current() {
            Token::Union => {
                self.advance();
                let all = if self.current() == &Token::All { self.advance(); true } else { false };
                let right = self.parse_statement()?;
                let right = self.maybe_parse_union(right)?;
                Ok(Statement::Union(Box::new(left), Box::new(right), all))
            }
            Token::Intersect => {
                self.advance();
                let all = if self.current() == &Token::All { self.advance(); true } else { false };
                let right = self.parse_statement()?;
                let right = self.maybe_parse_union(right)?;
                Ok(Statement::Intersect(Box::new(left), Box::new(right), all))
            }
            Token::Except => {
                self.advance();
                let all = if self.current() == &Token::All { self.advance(); true } else { false };
                let right = self.parse_statement()?;
                let right = self.maybe_parse_union(right)?;
                Ok(Statement::Except(Box::new(left), Box::new(right), all))
            }
            _ => Ok(left),
        }
    }

    fn parse_show(&mut self) -> Result<Statement> {
        self.expect(Token::Show)?;
        match self.current().clone() {
            Token::Tables => {
                self.advance();
                Ok(Statement::ShowTables)
            }
            Token::Columns => {
                self.advance();
                self.expect(Token::From)?;
                let table = self.expect_ident()?;
                Ok(Statement::ShowColumns(table))
            }
            Token::Create => {
                self.advance();
                self.expect(Token::Table)?;
                let table = self.expect_ident()?;
                Ok(Statement::ShowCreateTable(table))
            }
            Token::Ident(ref s) if s.eq_ignore_ascii_case("DATABASES") => {
                self.advance();
                Ok(Statement::ShowDatabases)
            }
            Token::Ident(ref s) if s.eq_ignore_ascii_case("STATS") => {
                self.advance();

                let table = if let Token::Ident(kw) = self.current() {
                    if kw.eq_ignore_ascii_case("FOR") {
                        self.advance();
                        Some(self.expect_ident()?)
                    } else {
                        None
                    }
                } else {
                    None
                };
                Ok(Statement::ShowStats(table))
            }
            _ => Err(QueryError::Parse("Expected TABLES, COLUMNS, CREATE, DATABASES, or STATS after SHOW".into())),
        }
    }

    fn parse_truncate(&mut self) -> Result<Statement> {
        self.expect(Token::Truncate)?;
        if self.current() == &Token::Table {
            self.advance();
        }
        let table = self.expect_ident()?;
        Ok(Statement::Truncate(table))
    }

    fn parse_kv(&mut self) -> Result<Statement> {
        self.expect(Token::Kv)?;
        match self.current().clone() {
            Token::Get => {
                self.advance();
                let key = self.expect_string()?;
                Ok(Statement::KvGet(key))
            }
            Token::Set => {
                self.advance();
                let key = self.expect_string()?;
                let value = self.expect_string()?;
                Ok(Statement::KvSet(key, value))
            }
            Token::Delete => {
                self.advance();
                let key = self.expect_string()?;
                Ok(Statement::KvDelete(key))
            }
            Token::Scan => {
                self.advance();
                let start = self.expect_string()?;
                let end = self.expect_string()?;
                Ok(Statement::KvScan(start, end))
            }
            _ => Err(QueryError::Parse("Expected GET, SET, DELETE, or SCAN after KV".into())),
        }
    }

    fn parse_doc(&mut self) -> Result<Statement> {
        self.expect(Token::Doc)?;
        match self.current().clone() {
            Token::Insert => {
                self.advance();
                self.expect(Token::Into)?;
                let collection = self.expect_ident()?;
                let document = self.parse_json_object()?;
                Ok(Statement::DocInsert(DocInsertStmt { collection, document }))
            }
            Token::Find => {
                self.advance();
                self.expect(Token::In)?;
                let collection = self.expect_ident()?;
                let filter = if self.current() == &Token::Where {
                    self.advance();
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                Ok(Statement::DocFind(DocFindStmt { collection, filter }))
            }
            Token::Update => {
                self.advance();
                self.expect(Token::In)?;
                let collection = self.expect_ident()?;
                let filter = if self.current() == &Token::Where {
                    self.advance();
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                self.expect(Token::Set)?;
                let mut updates = Vec::new();
                loop {
                    if self.current() == &Token::DollarDot {
                        self.advance();
                    }
                    let path = self.expect_ident()?;
                    self.expect(Token::Eq)?;
                    let expr = self.parse_expr()?;
                    updates.push((path, expr));
                    if self.current() != &Token::Comma {
                        break;
                    }
                    self.advance();
                }
                Ok(Statement::DocUpdate(DocUpdateStmt { collection, filter, updates }))
            }
            Token::Delete => {
                self.advance();
                self.expect(Token::From)?;
                let collection = self.expect_ident()?;
                let filter = if self.current() == &Token::Where {
                    self.advance();
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                Ok(Statement::DocDelete(DocDeleteStmt { collection, filter }))
            }
            _ => Err(QueryError::Parse("Expected INSERT, FIND, UPDATE, or DELETE after DOC".into())),
        }
    }

    fn parse_json_object(&mut self) -> Result<String> {
        let mut depth = 0;
        let _start = self.pos;
        let mut result = String::new();

        if self.current() != &Token::LBrace {
            return Err(QueryError::Parse("Expected '{'".into()));
        }

        loop {
            match self.current() {
                Token::LBrace => { depth += 1; result.push('{'); self.advance(); }
                Token::RBrace => {
                    depth -= 1;
                    result.push('}');
                    self.advance();
                    if depth == 0 { break; }
                }
                Token::StringLit(s) => { result.push('"'); result.push_str(s); result.push('"'); self.advance(); }
                Token::IntLit(n) => { result.push_str(&n.to_string()); self.advance(); }
                Token::FloatLit(f) => { result.push_str(&f.to_string()); self.advance(); }
                Token::BoolLit(b) => { result.push_str(&b.to_string()); self.advance(); }
                Token::NullLit => { result.push_str("null"); self.advance(); }
                Token::Colon => { result.push(':'); self.advance(); }
                Token::Comma => { result.push(','); self.advance(); }
                Token::LBracket => { result.push('['); self.advance(); }
                Token::RBracket => { result.push(']'); self.advance(); }
                Token::Eof => return Err(QueryError::Parse("Unterminated JSON object".into())),
                _ => { self.advance(); }
            }
        }

        Ok(result)
    }

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_and_expr()?;
        while self.current() == &Token::Or {
            self.advance();
            let right = self.parse_and_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinOp::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_comparison()?;
        while self.current() == &Token::And {
            self.advance();
            let right = self.parse_comparison()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let mut left = self.parse_addition()?;

        loop {
            let op = match self.current() {
                Token::Eq => BinOp::Eq,
                Token::Neq => BinOp::Neq,
                Token::Lt => BinOp::Lt,
                Token::Gt => BinOp::Gt,
                Token::Lte => BinOp::Lte,
                Token::Gte => BinOp::Gte,
                Token::Like => BinOp::Like,
                Token::Ilike => BinOp::Ilike,
                Token::Is => {
                    self.advance();
                    if self.current() == &Token::Not {
                        self.advance();
                        self.expect(Token::NullLit)?;
                        left = Expr::IsNotNull(Box::new(left));
                    } else {
                        self.expect(Token::NullLit)?;
                        left = Expr::IsNull(Box::new(left));
                    }
                    continue;
                }
                Token::Not => {
                    self.advance();
                    match self.current().clone() {
                        Token::In => {
                            self.advance();
                            self.expect(Token::LParen)?;
                            if self.current() == &Token::Select {
                                self.advance();
                                let subquery = self.parse_select_body()?;
                                self.expect(Token::RParen)?;
                                left = Expr::UnaryOp {
                                    op: UnaryOp::Not,
                                    expr: Box::new(Expr::InSubquery { expr: Box::new(left), subquery: Box::new(subquery) }),
                                };
                            } else {
                                let list = self.parse_expr_list()?;
                                self.expect(Token::RParen)?;
                                left = Expr::UnaryOp {
                                    op: UnaryOp::Not,
                                    expr: Box::new(Expr::InList { expr: Box::new(left), list }),
                                };
                            }
                        }
                        Token::Between => {
                            self.advance();
                            let low = self.parse_addition()?;
                            self.expect(Token::And)?;
                            let high = self.parse_addition()?;
                            left = Expr::UnaryOp {
                                op: UnaryOp::Not,
                                expr: Box::new(Expr::Between {
                                    expr: Box::new(left),
                                    low: Box::new(low),
                                    high: Box::new(high),
                                }),
                            };
                        }
                        Token::Like | Token::Ilike => {
                            let is_ilike = self.current() == &Token::Ilike;
                            self.advance();
                            let right = self.parse_addition()?;
                            left = Expr::UnaryOp {
                                op: UnaryOp::Not,
                                expr: Box::new(Expr::BinaryOp {
                                    left: Box::new(left),
                                    op: if is_ilike { BinOp::Ilike } else { BinOp::Like },
                                    right: Box::new(right),
                                }),
                            };
                        }
                        _ => {
                            return Err(QueryError::Parse("Expected IN, BETWEEN, or LIKE after NOT".into()));
                        }
                    }
                    continue;
                }
                Token::In => {
                    self.advance();
                    self.expect(Token::LParen)?;
                    if self.current() == &Token::Select {
                        self.advance();
                        let subquery = self.parse_select_body()?;
                        self.expect(Token::RParen)?;
                        left = Expr::InSubquery { expr: Box::new(left), subquery: Box::new(subquery) };
                    } else {
                        let list = self.parse_expr_list()?;
                        self.expect(Token::RParen)?;
                        left = Expr::InList { expr: Box::new(left), list };
                    }
                    continue;
                }
                Token::Between => {
                    self.advance();
                    let low = self.parse_addition()?;
                    self.expect(Token::And)?;
                    let high = self.parse_addition()?;
                    left = Expr::Between {
                        expr: Box::new(left),
                        low: Box::new(low),
                        high: Box::new(high),
                    };
                    continue;
                }
                _ => break,
            };
            self.advance();
            let right = self.parse_addition()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_addition(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplication()?;
        loop {
            let op = match self.current() {
                Token::Plus => BinOp::Plus,
                Token::Minus => BinOp::Minus,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplication()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_multiplication(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.current() {
                Token::Star => BinOp::Mul,
                Token::Slash => BinOp::Div,
                Token::Percent => BinOp::Mod,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        match self.current().clone() {
            Token::Not => {
                self.advance();
                let expr = self.parse_unary()?;
                Ok(Expr::UnaryOp { op: UnaryOp::Not, expr: Box::new(expr) })
            }
            Token::Minus => {
                self.advance();
                let expr = self.parse_unary()?;
                match expr {
                    Expr::Literal(LiteralValue::Integer(n)) => Ok(Expr::Literal(LiteralValue::Integer(-n))),
                    Expr::Literal(LiteralValue::Float(f)) => Ok(Expr::Literal(LiteralValue::Float(-f))),
                    other => Ok(Expr::UnaryOp { op: UnaryOp::Neg, expr: Box::new(other) }),
                }
            }
            _ => self.parse_primary(),
        }
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        let mut expr = match self.current().clone() {
            Token::IntLit(n) => {
                self.advance();
                Expr::Literal(LiteralValue::Integer(n))
            }
            Token::FloatLit(f) => {
                self.advance();
                Expr::Literal(LiteralValue::Float(f))
            }
            Token::StringLit(s) => {
                self.advance();
                Expr::Literal(LiteralValue::String(s))
            }
            Token::BoolLit(b) => {
                self.advance();
                Expr::Literal(LiteralValue::Bool(b))
            }
            Token::NullLit => {
                self.advance();
                Expr::Literal(LiteralValue::Null)
            }
            Token::HexBlob(bytes) => {
                self.advance();
                Expr::Literal(LiteralValue::HexBlob(bytes.clone()))
            }
            Token::Interval => {
                self.advance();
                if let Token::StringLit(s) = self.current().clone() {
                    self.advance();
                    Expr::Interval(s)
                } else {
                    return Err(QueryError::Parse("Expected string literal after INTERVAL".into()));
                }
            }
            Token::DollarDot => {
                self.advance();
                let path = self.expect_ident()?;
                Expr::JsonPath { path }
            }
            Token::Case => {
                self.advance();
                let operand = if !matches!(self.current(), Token::When) {
                    Some(Box::new(self.parse_expr()?))
                } else {
                    None
                };
                let mut when_clauses = Vec::new();
                while self.current() == &Token::When {
                    self.advance();
                    let condition = self.parse_expr()?;
                    self.expect(Token::Then)?;
                    let result = self.parse_expr()?;
                    when_clauses.push((condition, result));
                }
                let else_result = if self.current() == &Token::Else {
                    self.advance();
                    Some(Box::new(self.parse_expr()?))
                } else {
                    None
                };
                self.expect(Token::End)?;
                Expr::Case { operand, when_clauses, else_result }
            }
            Token::Cast => {
                self.advance();
                self.expect(Token::LParen)?;
                let expr = self.parse_expr()?;
                self.expect(Token::As)?;
                let (data_type, _) = self.parse_data_type()?;
                self.expect(Token::RParen)?;
                Expr::Cast { expr: Box::new(expr), data_type }
            }
            Token::Exists => {
                self.advance();
                self.expect(Token::LParen)?;
                self.expect(Token::Select)?;
                let select = self.parse_select_body()?;
                self.expect(Token::RParen)?;
                Expr::Exists(Box::new(select))
            }
            Token::Not => {
                self.advance();
                if self.current() == &Token::Exists {
                    self.advance();
                    self.expect(Token::LParen)?;
                    self.expect(Token::Select)?;
                    let select = self.parse_select_body()?;
                    self.expect(Token::RParen)?;
                    Expr::UnaryOp { op: UnaryOp::Not, expr: Box::new(Expr::Exists(Box::new(select))) }
                } else {
                    let e = self.parse_unary()?;
                    Expr::UnaryOp { op: UnaryOp::Not, expr: Box::new(e) }
                }
            }
            Token::Ident(name) => {
                self.advance();
                if self.current() == &Token::LParen {
                    self.advance();
                    let (func_name, args) = if self.current() == &Token::RParen {
                        (name, Vec::new())
                    } else if self.current() == &Token::Star {
                        self.advance();
                        (name, vec![Expr::Column("*".to_string())])
                    } else if self.current() == &Token::Distinct {
                        self.advance();
                        let distinct_args = self.parse_expr_list()?;
                        (format!("{}_DISTINCT", name.to_uppercase()), distinct_args)
                    } else {
                        (name, self.parse_expr_list()?)
                    };
                    self.expect(Token::RParen)?;
                    if self.current() == &Token::Over {
                        self.advance();
                        self.expect(Token::LParen)?;
                        let partition_by = if self.current() == &Token::Partition {
                            self.advance();
                            self.expect(Token::By)?;
                            self.parse_expr_list()?
                        } else {
                            Vec::new()
                        };
                        let order_by = if self.current() == &Token::Order {
                            self.advance();
                            self.expect(Token::By)?;
                            self.parse_order_by()?
                        } else {
                            Vec::new()
                        };
                        self.expect(Token::RParen)?;
                        Expr::WindowFunction { name: func_name, args, partition_by, order_by }
                    } else {
                        Expr::Function { name: func_name, args }
                    }
                } else if self.current() == &Token::Dot {
                    self.advance();
                    let col = self.expect_ident()?;
                    Expr::QualifiedColumn(name, col)
                } else {
                    Expr::Column(name)
                }
            }
            Token::LParen => {
                self.advance();
                if self.current() == &Token::Select {
                    self.expect(Token::Select)?;
                    let select = self.parse_select_body()?;
                    self.expect(Token::RParen)?;
                    Expr::Subquery(Box::new(select))
                } else {
                    let e = self.parse_expr()?;
                    self.expect(Token::RParen)?;
                    e
                }
            }
            Token::Default => {
                self.advance();
                Expr::Default
            }
            _ => return Err(QueryError::Parse(format!("Unexpected token in expression: {:?}", self.current()))),
        };

        while self.current() == &Token::DoubleColon {
            self.advance();
            let (data_type, _) = self.parse_data_type()?;
            expr = Expr::Cast { expr: Box::new(expr), data_type };
        }

        Ok(expr)
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>> {
        let mut exprs = Vec::new();
        loop {
            exprs.push(self.parse_expr()?);
            if self.current() != &Token::Comma {
                break;
            }
            self.advance();
        }
        Ok(exprs)
    }

    fn parse_ident_list(&mut self) -> Result<Vec<String>> {
        let mut idents = Vec::new();
        loop {
            idents.push(self.expect_ident()?);
            if self.current() != &Token::Comma {
                break;
            }
            self.advance();
        }
        Ok(idents)
    }

    fn parse_data_type(&mut self) -> Result<(DataType, Option<usize>)> {
        self.last_data_type_serial = matches!(self.current(), Token::Serial);
        let dt = match self.current() {
            Token::Int => DataType::Int64,
            Token::Integer => DataType::Int64,
            Token::Bigint => DataType::Int64,
            Token::Smallint => DataType::Int64,
            Token::Serial => DataType::Int64,
            Token::Float => DataType::Float64,
            Token::Real => DataType::Float64,
            Token::DoublePrecision => DataType::Float64,
            Token::Numeric => DataType::Decimal,
            Token::Text => DataType::Text,
            Token::Varchar => DataType::Text,
            Token::Bool => DataType::Bool,
            Token::Bytes => DataType::Bytes,
            Token::Blob => DataType::Bytes,
            Token::Json => DataType::Json,
            Token::Timestamp => DataType::Timestamp,
            Token::Date => DataType::Date,
            Token::Uuid => DataType::Uuid,
            Token::Interval => DataType::Interval,
            _ => return Err(QueryError::Parse(format!("Expected data type, got {:?}", self.current()))),
        };
        self.advance();

        if matches!(dt, DataType::Float64 | DataType::Decimal) {
            if let Token::Ident(s) = self.current() {
                if s.to_uppercase() == "PRECISION" {
                    self.advance();
                }
            }
        }

        let mut max_len: Option<usize> = None;
        if self.current() == &Token::LParen {
            self.advance();
            let n = self.expect_integer()? as usize;
            if self.current() == &Token::Comma {
                self.advance();
                let _ = self.expect_integer()?;
            }
            self.expect(Token::RParen)?;
            if matches!(dt, DataType::Text | DataType::Bytes) {
                max_len = Some(n);
            }
        }
        Ok((dt, max_len))
    }

    fn current(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) {
        self.pos += 1;
    }

    fn expect(&mut self, expected: Token) -> Result<()> {
        if std::mem::discriminant(self.current()) == std::mem::discriminant(&expected) {
            self.advance();
            Ok(())
        } else {
            Err(QueryError::Parse(format!("Expected {:?}, got {:?}", expected, self.current())))
        }
    }

    fn expect_ident(&mut self) -> Result<String> {
        match self.current().clone() {
            Token::Ident(s) => { self.advance(); Ok(s) }
            Token::Tables => { self.advance(); Ok("tables".into()) }
            Token::Columns => { self.advance(); Ok("columns".into()) }
            Token::Show => { self.advance(); Ok("show".into()) }
            Token::Index => { self.advance(); Ok("index".into()) }
            Token::Key => { self.advance(); Ok("key".into()) }
            Token::Column => { self.advance(); Ok("column".into()) }
            Token::Rows => { self.advance(); Ok("rows".into()) }
            Token::Row => { self.advance(); Ok("row".into()) }
            Token::Date => { self.advance(); Ok("date".into()) }
            Token::Timestamp => { self.advance(); Ok("timestamp".into()) }
            Token::Text => { self.advance(); Ok("text".into()) }
            Token::Int => { self.advance(); Ok("int".into()) }
            Token::Float => { self.advance(); Ok("float".into()) }
            Token::Bool => { self.advance(); Ok("bool".into()) }
            Token::Uuid => { self.advance(); Ok("uuid".into()) }
            Token::Json => { self.advance(); Ok("json".into()) }
            Token::First => { self.advance(); Ok("first".into()) }
            Token::Last => { self.advance(); Ok("last".into()) }
            Token::Partition => { self.advance(); Ok("partition".into()) }
            Token::Over => { self.advance(); Ok("over".into()) }
            _ => Err(QueryError::Parse(format!("Expected identifier, got {:?}", self.current()))),
        }
    }

    fn expect_string(&mut self) -> Result<String> {
        match self.current().clone() {
            Token::StringLit(s) => { self.advance(); Ok(s) }
            _ => Err(QueryError::Parse(format!("Expected string, got {:?}", self.current()))),
        }
    }

    fn expect_integer(&mut self) -> Result<i64> {
        match self.current().clone() {
            Token::IntLit(n) => { self.advance(); Ok(n) }
            _ => Err(QueryError::Parse(format!("Expected integer, got {:?}", self.current()))),
        }
    }

    fn parse_fk_actions(&mut self) -> Result<(FkAction, FkAction)> {
        let mut on_delete = FkAction::Restrict;
        let mut on_update = FkAction::Restrict;
        loop {
            if self.current() != &Token::On { break; }
            self.advance();
            let which = match self.current().clone() {
                Token::Delete => { self.advance(); 0 }
                Token::Update => { self.advance(); 1 }
                _ => return Err(QueryError::Parse("Expected DELETE or UPDATE after ON".into())),
            };
            let action = match self.current().clone() {
                Token::Cascade => { self.advance(); FkAction::Cascade }
                Token::Restrict => { self.advance(); FkAction::Restrict }
                Token::Set => {
                    self.advance();
                    if self.current() == &Token::NullLit { self.advance(); FkAction::SetNull }
                    else { return Err(QueryError::Parse("Expected NULL after SET".into())); }
                }
                Token::Ident(ref s) if s.eq_ignore_ascii_case("NO") => {
                    self.advance();
                    if matches!(self.current(), Token::Ident(s) if s.eq_ignore_ascii_case("ACTION")) {
                        self.advance();
                        FkAction::Restrict
                    } else {
                        return Err(QueryError::Parse("Expected ACTION after NO".into()));
                    }
                }
                _ => return Err(QueryError::Parse(format!("Expected CASCADE/RESTRICT/SET NULL, got {:?}", self.current()))),
            };
            if which == 0 { on_delete = action; } else { on_update = action; }
        }
        Ok((on_delete, on_update))
    }

    fn try_parse_alias(&mut self) -> Option<String> {
        if self.current() == &Token::As {
            self.advance();
            self.expect_ident().ok()
        } else if let Token::Ident(_) = self.current() {
            self.expect_ident().ok()
        } else {
            None
        }
    }
}
