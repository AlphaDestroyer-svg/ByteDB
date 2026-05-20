use bytedb_core::mvcc::transaction::IsolationLevel;
use bytedb_core::tuple::value::DataType;

#[derive(Debug, Clone)]
pub enum Statement {
    // DDL
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    CreateIndex(CreateIndexStmt),
    DropIndex(String),
    AlterTable(AlterTableStmt),
    // Set operations
    Union(Box<Statement>, Box<Statement>, bool), // left, right, all
    Intersect(Box<Statement>, Box<Statement>, bool),
    Except(Box<Statement>, Box<Statement>, bool),
    // DML
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    // KV
    KvGet(String),
    KvSet(String, String),
    KvDelete(String),
    KvScan(String, String),
    // Document
    DocInsert(DocInsertStmt),
    DocFind(DocFindStmt),
    DocUpdate(DocUpdateStmt),
    DocDelete(DocDeleteStmt),
    // Transaction
    Begin(Option<IsolationLevel>),
    Commit,
    Rollback,
    // Utility
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
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub auto_increment: bool,
    pub default: Option<Expr>,
    pub check: Option<Expr>,
    pub references: Option<(String, String)>, // (table, column)
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

// Document statements
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
}

#[derive(Debug, Clone)]
pub enum LiteralValue {
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
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
