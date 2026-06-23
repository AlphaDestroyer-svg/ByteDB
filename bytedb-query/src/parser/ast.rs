use bytedb_core::mvcc::transaction::IsolationLevel;
use bytedb_core::tuple::value::DataType;

#[derive(Debug, Clone)]
pub enum Statement {

    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    CreateIndex(CreateIndexStmt),
    DropIndex(String),
    AlterTable(AlterTableStmt),

    Union(Box<Statement>, Box<Statement>, bool),
    Intersect(Box<Statement>, Box<Statement>, bool),
    Except(Box<Statement>, Box<Statement>, bool),

    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),

    KvGet(String),
    KvSet(String, String),
    KvDelete(String),
    KvScan(String, String),

    DocInsert(DocInsertStmt),
    DocFind(DocFindStmt),
    DocUpdate(DocUpdateStmt),
    DocDelete(DocDeleteStmt),

    Begin(Option<IsolationLevel>),
    Commit,
    Rollback,

    ShowTables,
    ShowColumns(String),
    ShowCreateTable(String),
    Describe(String),
    Explain(Box<Statement>, bool),
    Truncate(String),
    CreateDatabase { name: String, if_not_exists: bool },
    DropDatabase { name: String, if_exists: bool },
    UseDatabase(String),
    ShowDatabases,

    Analyze(Option<String>),

    ShowStats(Option<String>),

    Backup { path: String },
    Restore { path: String, to_lsn: Option<u64> },
    Migrate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FkAction {
    Restrict,
    Cascade,
    SetNull,
}

#[derive(Debug, Clone)]
pub struct ForeignKeyDef {
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub on_delete: FkAction,
    pub on_update: FkAction,
}

#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub if_not_exists: bool,
    pub check_constraints: Vec<Expr>,
    pub foreign_keys: Vec<ForeignKeyDef>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub max_length: Option<usize>,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub auto_increment: bool,
    pub default: Option<Expr>,
    pub check: Option<Expr>,
    pub references: Option<(String, String)>,
    pub on_delete: FkAction,
    pub on_update: FkAction,
}

#[derive(Debug, Clone)]
pub struct DropTableStmt {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateIndexStmt {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone)]
pub struct AlterTableStmt {
    pub table: String,
    pub action: AlterTableAction,
}

#[derive(Debug, Clone)]
pub enum AlterTableAction {
    AddColumn(ColumnDef),
    DropColumn(String),
    RenameColumn { old_name: String, new_name: String },
}

#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub columns: Vec<SelectColumn>,
    pub distinct: bool,
    pub from: FromClause,
    pub from_alias: Option<String>,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub ctes: Vec<Cte>,
}

#[derive(Debug, Clone)]
pub enum FromClause {
    Table(String),
    Subquery(Box<SelectStmt>),
    None,
}

#[derive(Debug, Clone)]
pub struct Cte {
    pub name: String,
    pub query: SelectStmt,
}

#[derive(Debug, Clone)]
pub enum SelectColumn {
    Star,
    Expr(Expr, Option<String>),
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    pub alias: Option<String>,
    pub condition: Expr,
}

#[derive(Debug, Clone, Copy)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

#[derive(Debug, Clone)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub source: InsertSource,
    pub on_conflict: Option<OnConflict>,
    pub returning: Option<Vec<SelectColumn>>,
}

#[derive(Debug, Clone)]
pub enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Select(SelectStmt),
}

#[derive(Debug, Clone)]
pub struct OnConflict {
    pub columns: Vec<String>,
    pub action: ConflictAction,
}

#[derive(Debug, Clone)]
pub enum ConflictAction {
    DoNothing,
    DoUpdate(Vec<(String, Expr)>),
}

#[derive(Debug, Clone)]
pub struct UpdateStmt {
    pub table: String,
    pub assignments: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
    pub returning: Option<Vec<SelectColumn>>,
}

#[derive(Debug, Clone)]
pub struct DeleteStmt {
    pub table: String,
    pub where_clause: Option<Expr>,
    pub returning: Option<Vec<SelectColumn>>,
}

#[derive(Debug, Clone)]
pub struct DocInsertStmt {
    pub collection: String,
    pub document: String,
}

#[derive(Debug, Clone)]
pub struct DocFindStmt {
    pub collection: String,
    pub filter: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct DocUpdateStmt {
    pub collection: String,
    pub filter: Option<Expr>,
    pub updates: Vec<(String, Expr)>,
}

#[derive(Debug, Clone)]
pub struct DocDeleteStmt {
    pub collection: String,
    pub filter: Option<Expr>,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Literal(LiteralValue),
    Column(String),
    QualifiedColumn(String, String),
    BinaryOp {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Function {
        name: String,
        args: Vec<Expr>,
    },
    JsonPath {
        path: String,
    },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    Subquery(Box<SelectStmt>),
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<SelectStmt>,
    },
    Exists(Box<SelectStmt>),
    WindowFunction {
        name: String,
        args: Vec<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderByExpr>,
    },
    Default,
    Interval(String),
}

#[derive(Debug, Clone)]
pub enum LiteralValue {
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
    HexBlob(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    And,
    Or,
    Plus,
    Minus,
    Mul,
    Div,
    Mod,
    Like,
    Ilike,
}

#[derive(Debug, Clone, Copy)]
pub enum UnaryOp {
    Not,
    Neg,
}

pub fn expr_to_sql(expr: &Expr) -> String {
    let mut s = String::new();
    write_expr(expr, &mut s);
    s
}

fn binop_sql(op: BinOp) -> &'static str {
    match op {
        BinOp::Eq => "=",
        BinOp::Neq => "<>",
        BinOp::Lt => "<",
        BinOp::Gt => ">",
        BinOp::Lte => "<=",
        BinOp::Gte => ">=",
        BinOp::And => "AND",
        BinOp::Or => "OR",
        BinOp::Plus => "+",
        BinOp::Minus => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
        BinOp::Like => "LIKE",
        BinOp::Ilike => "ILIKE",
    }
}

fn quote_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn write_expr(expr: &Expr, out: &mut String) {
    match expr {
        Expr::Literal(LiteralValue::Integer(n)) => out.push_str(&n.to_string()),
        Expr::Literal(LiteralValue::Float(f)) => out.push_str(&format!("{:?}", f)),
        Expr::Literal(LiteralValue::String(s)) => out.push_str(&quote_string(s)),
        Expr::Literal(LiteralValue::Bool(b)) => out.push_str(if *b { "TRUE" } else { "FALSE" }),
        Expr::Literal(LiteralValue::Null) => out.push_str("NULL"),
        Expr::Literal(LiteralValue::HexBlob(bytes)) => {
            out.push_str("X'");
            for b in bytes {
                out.push_str(&format!("{:02X}", b));
            }
            out.push('\'');
        }
        Expr::Column(name) => out.push_str(name),
        Expr::QualifiedColumn(t, c) => {
            out.push_str(t);
            out.push('.');
            out.push_str(c);
        }
        Expr::BinaryOp { left, op, right } => {
            out.push('(');
            write_expr(left, out);
            out.push(' ');
            out.push_str(binop_sql(*op));
            out.push(' ');
            write_expr(right, out);
            out.push(')');
        }
        Expr::UnaryOp { op, expr } => {
            match op {
                UnaryOp::Not => {
                    out.push_str("(NOT ");
                    write_expr(expr, out);
                    out.push(')');
                }
                UnaryOp::Neg => {
                    out.push_str("(-");
                    write_expr(expr, out);
                    out.push(')');
                }
            }
        }
        Expr::Function { name, args } => {
            out.push_str(name);
            out.push('(');
            for (i, a) in args.iter().enumerate() {
                if i > 0 { out.push_str(", "); }
                write_expr(a, out);
            }
            out.push(')');
        }
        Expr::IsNull(inner) => {
            out.push('(');
            write_expr(inner, out);
            out.push_str(" IS NULL)");
        }
        Expr::IsNotNull(inner) => {
            out.push('(');
            write_expr(inner, out);
            out.push_str(" IS NOT NULL)");
        }
        Expr::InList { expr, list } => {
            out.push('(');
            write_expr(expr, out);
            out.push_str(" IN (");
            for (i, a) in list.iter().enumerate() {
                if i > 0 { out.push_str(", "); }
                write_expr(a, out);
            }
            out.push_str("))");
        }
        Expr::Between { expr, low, high } => {
            out.push('(');
            write_expr(expr, out);
            out.push_str(" BETWEEN ");
            write_expr(low, out);
            out.push_str(" AND ");
            write_expr(high, out);
            out.push(')');
        }
        Expr::Case { operand, when_clauses, else_result } => {
            out.push_str("CASE");
            if let Some(o) = operand {
                out.push(' ');
                write_expr(o, out);
            }
            for (w, t) in when_clauses {
                out.push_str(" WHEN ");
                write_expr(w, out);
                out.push_str(" THEN ");
                write_expr(t, out);
            }
            if let Some(e) = else_result {
                out.push_str(" ELSE ");
                write_expr(e, out);
            }
            out.push_str(" END");
        }
        Expr::Cast { expr, data_type } => {
            out.push_str("CAST(");
            write_expr(expr, out);
            out.push_str(" AS ");
            out.push_str(&data_type.to_string());
            out.push(')');
        }
        Expr::Interval(s) => {
            out.push_str("INTERVAL ");
            out.push_str(&quote_string(s));
        }
        Expr::JsonPath { path } => out.push_str(path),
        Expr::Subquery(_)
        | Expr::InSubquery { .. }
        | Expr::Exists(_)
        | Expr::WindowFunction { .. }
        | Expr::Default => out.push_str("NULL"),
    }
}
