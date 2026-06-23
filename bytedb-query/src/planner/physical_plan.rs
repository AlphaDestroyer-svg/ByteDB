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

impl PhysicalPlan {
    pub fn explain_tree(&self) -> Vec<String> {
        let mut out = Vec::new();
        self.write_tree(0, &mut out);
        out
    }

    fn write_tree(&self, depth: usize, out: &mut Vec<String>) {
        let indent = "  ".repeat(depth);
        let arrow = if depth == 0 { String::new() } else { format!("{}-> ", indent) };
        match self {
            PhysicalPlan::SeqScan { table, filter, limit, .. } => {
                let mut line = format!("{}Seq Scan on {}", arrow, table);
                if let Some(f) = filter { line.push_str(&format!("  (filter: {})", expr_to_sql(f))); }
                if let Some(l) = limit { line.push_str(&format!("  (limit {})", l)); }
                out.push(line);
            }
            PhysicalPlan::IndexScan { table, index_name, column, op, value, filter, .. } => {
                out.push(format!("{}Index Scan using {} on {}  ({} {} {})",
                    arrow, index_name, table, column, binop_text(*op), expr_to_sql(value)));
                if let Some(f) = filter {
                    out.push(format!("{}  filter: {}", indent, expr_to_sql(f)));
                }
            }
            PhysicalPlan::Filter { input, predicate } => {
                out.push(format!("{}Filter  ({})", arrow, expr_to_sql(predicate)));
                input.write_tree(depth + 1, out);
            }
            PhysicalPlan::Project { input, columns } => {
                out.push(format!("{}Project  ({} cols)", arrow, columns.len()));
                input.write_tree(depth + 1, out);
            }
            PhysicalPlan::HashJoin { left, right, condition, join_type } => {
                out.push(format!("{}Hash Join  [{}]  on {}", arrow, join_type_text(*join_type), expr_to_sql(condition)));
                left.write_tree(depth + 1, out);
                right.write_tree(depth + 1, out);
            }
            PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
                out.push(format!("{}Nested Loop Join  [{}]  on {}", arrow, join_type_text(*join_type), expr_to_sql(condition)));
                left.write_tree(depth + 1, out);
                right.write_tree(depth + 1, out);
            }
            PhysicalPlan::HashAggregate { input, group_by, aggregates, having } => {
                let mut line = format!("{}Aggregate", arrow);
                if !group_by.is_empty() {
                    let keys: Vec<String> = group_by.iter().map(expr_to_sql).collect();
                    line.push_str(&format!("  (group by: {})", keys.join(", ")));
                }
                line.push_str(&format!("  ({} aggs)", aggregates.len()));
                if having.is_some() { line.push_str("  (having)"); }
                out.push(line);
                input.write_tree(depth + 1, out);
            }
            PhysicalPlan::Sort { input, order_by } => {
                let keys: Vec<String> = order_by.iter()
                    .map(|o| format!("{} {}", expr_to_sql(&o.expr), if o.ascending { "ASC" } else { "DESC" }))
                    .collect();
                out.push(format!("{}Sort  ({})", arrow, keys.join(", ")));
                input.write_tree(depth + 1, out);
            }
            PhysicalPlan::Limit { input, count, offset } => {
                out.push(format!("{}Limit {} offset {}", arrow, count, offset));
                input.write_tree(depth + 1, out);
            }
            PhysicalPlan::Distinct { input } => {
                out.push(format!("{}Distinct", arrow));
                input.write_tree(depth + 1, out);
            }
            PhysicalPlan::Insert { table, .. } => out.push(format!("{}Insert into {}", arrow, table)),
            PhysicalPlan::Update { table, filter, .. } => {
                let mut line = format!("{}Update {}", arrow, table);
                if let Some(f) = filter { line.push_str(&format!("  (filter: {})", expr_to_sql(f))); }
                out.push(line);
            }
            PhysicalPlan::Delete { table, filter } => {
                let mut line = format!("{}Delete from {}", arrow, table);
                if let Some(f) = filter { line.push_str(&format!("  (filter: {})", expr_to_sql(f))); }
                out.push(line);
            }
            PhysicalPlan::CreateTable(ct) => out.push(format!("{}Create Table {}", arrow, ct.name)),
            PhysicalPlan::DropTable(dt) => out.push(format!("{}Drop Table {}", arrow, dt.name)),
            PhysicalPlan::CreateIndex(ci) => out.push(format!("{}Create Index {} on {}", arrow, ci.name, ci.table)),
            PhysicalPlan::DropIndex(name) => out.push(format!("{}Drop Index {}", arrow, name)),
        }
    }
}

fn binop_text(op: BinOp) -> &'static str {
    match op {
        BinOp::Eq => "=", BinOp::Neq => "<>", BinOp::Lt => "<", BinOp::Gt => ">",
        BinOp::Lte => "<=", BinOp::Gte => ">=", _ => "?",
    }
}

fn join_type_text(jt: JoinType) -> &'static str {
    match jt {
        JoinType::Inner => "inner",
        JoinType::Left => "left",
        JoinType::Right => "right",
    }
}
