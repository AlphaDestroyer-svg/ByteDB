use crate::parser::ast::*;

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    SeqScan {
        table: String,
        filter: Option<Expr>,
        limit: Option<usize>,
        needed_columns: Option<Vec<usize>>,
    },
    IndexScan {
        table: String,
        index_name: String,
        column: String,
        op: BinOp,
        value: Expr,
        filter: Option<Expr>,
        limit: Option<usize>,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<SelectColumn>,
    },
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        condition: Expr,
        join_type: JoinType,
    },
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        condition: Expr,
        join_type: JoinType,
    },
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        having: Option<Expr>,
    },
    Sort {
        input: Box<PhysicalPlan>,
        order_by: Vec<OrderByExpr>,
    },
    Limit {
        input: Box<PhysicalPlan>,
        count: usize,
        offset: usize,
    },
    Distinct {
        input: Box<PhysicalPlan>,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        source: InsertSource,
    },
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
    },
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    CreateIndex(CreateIndexStmt),
    DropIndex(String),
}
