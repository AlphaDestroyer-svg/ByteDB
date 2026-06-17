use crate::parser::ast::*;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table: String,
        filter: Option<Expr>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<SelectColumn>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        condition: Expr,
        join_type: JoinType,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        having: Option<Expr>,
    },
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<OrderByExpr>,
    },
    Limit {
        input: Box<LogicalPlan>,
        count: usize,
        offset: usize,
    },
    Distinct {
        input: Box<LogicalPlan>,
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

pub fn build_logical_plan(stmt: &Statement) -> crate::error::Result<LogicalPlan> {
    match stmt {
        Statement::Select(select) => build_select_plan(select),
        Statement::Insert(insert) => Ok(LogicalPlan::Insert {
            table: insert.table.clone(),
            columns: insert.columns.clone(),
            source: insert.source.clone(),
        }),
        Statement::Update(update) => Ok(LogicalPlan::Update {
            table: update.table.clone(),
            assignments: update.assignments.clone(),
            filter: update.where_clause.clone(),
        }),
        Statement::Delete(delete) => Ok(LogicalPlan::Delete {
            table: delete.table.clone(),
            filter: delete.where_clause.clone(),
        }),
        Statement::CreateTable(ct) => Ok(LogicalPlan::CreateTable(ct.clone())),
        Statement::DropTable(dt) => Ok(LogicalPlan::DropTable(dt.clone())),
        Statement::CreateIndex(ci) => Ok(LogicalPlan::CreateIndex(ci.clone())),
        Statement::DropIndex(name) => Ok(LogicalPlan::DropIndex(name.clone())),
        _ => Err(crate::error::QueryError::Plan("Statement does not produce a logical plan".into())),
    }
}

fn build_select_plan(select: &SelectStmt) -> crate::error::Result<LogicalPlan> {
    let table_name = match &select.from {
        FromClause::Table(name) => name.clone(),
        FromClause::Subquery(_) => select.from_alias.clone().unwrap_or_else(|| "__subquery__".to_string()),
        FromClause::None => {
            return Err(crate::error::QueryError::Plan("SELECT without FROM is handled by the executor".into()));
        }
    };

    let mut plan = LogicalPlan::Scan {
        table: table_name,
        filter: None,
    };

    for join in &select.joins {
        let right = LogicalPlan::Scan {
            table: join.table.clone(),
            filter: None,
        };
        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(right),
            condition: join.condition.clone(),
            join_type: join.join_type,
        };
    }

    if let Some(ref where_clause) = select.where_clause {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: where_clause.clone(),
        };
    }

    if !select.group_by.is_empty() || has_aggregate_functions(&select.columns) {
        let aggregates: Vec<Expr> = select.columns.iter().filter_map(|c| {
            match c {
                SelectColumn::Expr(expr, _) => Some(expr.clone()),
                _ => None,
            }
        }).collect();

        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_by: select.group_by.clone(),
            aggregates,
            having: select.having.clone(),
        };
    }

    if !select.order_by.is_empty() {
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            order_by: select.order_by.clone(),
        };
    }

    if select.limit.is_some() || select.offset.is_some() {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            count: select.limit.unwrap_or(usize::MAX),
            offset: select.offset.unwrap_or(0),
        };
    }

    plan = LogicalPlan::Project {
        input: Box::new(plan),
        columns: select.columns.clone(),
    };

    if select.distinct {
        plan = LogicalPlan::Distinct {
            input: Box::new(plan),
        };
    }

    Ok(plan)
}

fn has_aggregate_functions(columns: &[SelectColumn]) -> bool {
    columns.iter().any(|c| match c {
        SelectColumn::Expr(expr, _) => expr_has_aggregate(expr),
        _ => false,
    })
}

fn is_aggregate_name(name: &str) -> bool {
    const AGG_NAMES: &[&str] = &["COUNT", "SUM", "AVG", "MIN", "MAX", "COUNT_DISTINCT"];
    AGG_NAMES.contains(&name.to_uppercase().as_str())
}

fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, args } => {
            is_aggregate_name(name) || args.iter().any(expr_has_aggregate)
        }
        Expr::BinaryOp { left, right, .. } => expr_has_aggregate(left) || expr_has_aggregate(right),
        Expr::UnaryOp { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) | Expr::Cast { expr, .. } => {
            expr_has_aggregate(expr)
        }
        Expr::InList { expr, list } => expr_has_aggregate(expr) || list.iter().any(expr_has_aggregate),
        Expr::Between { expr, low, high } => {
            expr_has_aggregate(expr) || expr_has_aggregate(low) || expr_has_aggregate(high)
        }
        Expr::Case { operand, when_clauses, else_result } => {
            operand.as_ref().map_or(false, |o| expr_has_aggregate(o))
                || when_clauses.iter().any(|(w, t)| expr_has_aggregate(w) || expr_has_aggregate(t))
                || else_result.as_ref().map_or(false, |e| expr_has_aggregate(e))
        }
        _ => false,
    }
}
