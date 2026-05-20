use super::logical_plan::LogicalPlan;
use super::physical_plan::PhysicalPlan;
use crate::parser::ast::*;
use crate::error::Result;

pub fn optimize(logical: LogicalPlan) -> Result<PhysicalPlan> {
    let physical = convert_to_physical(logical)?;
    Ok(push_limit_down(physical))
}

fn convert_to_physical(plan: LogicalPlan) -> Result<PhysicalPlan> {
    match plan {
        LogicalPlan::Scan { table, filter } => {
            Ok(PhysicalPlan::SeqScan { table, filter, limit: None, needed_columns: None })
        }
        LogicalPlan::Filter { input, predicate } => {
            let input = convert_to_physical(*input)?;
            match input {
                PhysicalPlan::SeqScan { table, filter: existing, limit, needed_columns } => {
                    let merged = match existing {
                        Some(existing_pred) => Expr::BinaryOp {
                            left: Box::new(existing_pred),
                            op: BinOp::And,
                            right: Box::new(predicate),
                        },
                        None => predicate,
                    };
                    Ok(PhysicalPlan::SeqScan { table, filter: Some(merged), limit, needed_columns })
                }
                other => Ok(PhysicalPlan::Filter {
                    input: Box::new(other),
                    predicate,
                })
            }
        }
        LogicalPlan::Project { input, columns } => {
            let input = convert_to_physical(*input)?;
            Ok(PhysicalPlan::Project {
                input: Box::new(input),
                columns,
            })
        }
        LogicalPlan::Join { left, right, condition, join_type } => {
            let left = convert_to_physical(*left)?;
            let right = convert_to_physical(*right)?;
            Ok(PhysicalPlan::HashJoin {
                left: Box::new(left),
                right: Box::new(right),
                condition,
                join_type,
            })
        }
        LogicalPlan::Aggregate { input, group_by, aggregates, having } => {
            let input = convert_to_physical(*input)?;
            Ok(PhysicalPlan::HashAggregate {
                input: Box::new(input),
                group_by,
                aggregates,
                having,
            })
        }
        LogicalPlan::Sort { input, order_by } => {
            let input = convert_to_physical(*input)?;
            Ok(PhysicalPlan::Sort {
                input: Box::new(input),
                order_by,
            })
        }
        LogicalPlan::Limit { input, count, offset } => {
            let input = convert_to_physical(*input)?;
            Ok(PhysicalPlan::Limit {
                input: Box::new(input),
                count,
                offset,
            })
        }
        LogicalPlan::Distinct { input } => {
            let input = convert_to_physical(*input)?;
            Ok(PhysicalPlan::Distinct {
                input: Box::new(input),
            })
        }
        LogicalPlan::Insert { table, columns, source } => {
            Ok(PhysicalPlan::Insert { table, columns, source })
        }
        LogicalPlan::Update { table, assignments, filter } => {
            Ok(PhysicalPlan::Update { table, assignments, filter })
        }
        LogicalPlan::Delete { table, filter } => {
            Ok(PhysicalPlan::Delete { table, filter })
        }
        LogicalPlan::CreateTable(ct) => Ok(PhysicalPlan::CreateTable(ct)),
        LogicalPlan::DropTable(dt) => Ok(PhysicalPlan::DropTable(dt)),
        LogicalPlan::CreateIndex(ci) => Ok(PhysicalPlan::CreateIndex(ci)),
        LogicalPlan::DropIndex(name) => Ok(PhysicalPlan::DropIndex(name)),
    }
}

fn push_limit_down(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Project { input, columns } => {
            let pushed_input = push_limit_down(*input);
            PhysicalPlan::Project { input: Box::new(pushed_input), columns }
        }
        PhysicalPlan::Limit { input, count, offset } => {
            if offset == 0 {
                if matches!(*input, PhysicalPlan::Sort { .. }) {
                    PhysicalPlan::Limit { input, count, offset }
                } else {
                    let pushed = push_limit_into_scan(*input, count);
                    PhysicalPlan::Limit { input: Box::new(pushed), count, offset }
                }
            } else {
                PhysicalPlan::Limit { input, count, offset }
            }
        }
        other => other,
    }
}

fn push_limit_into_scan(plan: PhysicalPlan, limit: usize) -> PhysicalPlan {
    match plan {
        PhysicalPlan::SeqScan { table, filter, limit: _, needed_columns } => {
            PhysicalPlan::SeqScan { table, filter, limit: Some(limit), needed_columns }
        }
        PhysicalPlan::Filter { input, predicate } => {
            let pushed = push_limit_into_scan(*input, limit);
            PhysicalPlan::Filter { input: Box::new(pushed), predicate }
        }
        other => other,
    }
}
