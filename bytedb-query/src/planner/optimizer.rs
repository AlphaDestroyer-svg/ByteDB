use std::collections::HashMap;

use super::cost::{cost_plan, StatsCatalog};
use super::logical_plan::LogicalPlan;
use super::physical_plan::PhysicalPlan;
use crate::error::Result;
use crate::parser::ast::*;

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

pub type IndexCatalog = HashMap<String, Vec<IndexInfo>>;

pub fn optimize(logical: LogicalPlan) -> Result<PhysicalPlan> {
    optimize_with_stats(logical, &StatsCatalog::new())
}

pub fn optimize_with_stats(logical: LogicalPlan, stats: &StatsCatalog) -> Result<PhysicalPlan> {
    optimize_with_catalog(logical, stats, &IndexCatalog::new())
}

pub fn optimize_with_catalog(
    logical: LogicalPlan,
    stats: &StatsCatalog,
    indexes: &IndexCatalog,
) -> Result<PhysicalPlan> {
    let physical = convert_to_physical(logical, stats)?;
    let physical = push_limit_down(physical);
    Ok(select_indexes(physical, indexes))
}

fn select_indexes(plan: PhysicalPlan, indexes: &IndexCatalog) -> PhysicalPlan {
    match plan {
        PhysicalPlan::SeqScan { table, filter: Some(pred), limit, needed_columns } => {
            if let Some(idxs) = indexes.get(&table) {
                if let Some((index_name, column, op, value)) = find_index_predicate(&pred, idxs) {
                    return PhysicalPlan::IndexScan {
                        table,
                        index_name,
                        column,
                        op,
                        value,
                        filter: Some(pred),
                        limit,
                    };
                }
            }
            PhysicalPlan::SeqScan { table, filter: Some(pred), limit, needed_columns }
        }
        PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(select_indexes(*input, indexes)),
            predicate,
        },
        PhysicalPlan::Project { input, columns } => PhysicalPlan::Project {
            input: Box::new(select_indexes(*input, indexes)),
            columns,
        },
        PhysicalPlan::HashJoin { left, right, condition, join_type } => PhysicalPlan::HashJoin {
            left: Box::new(select_indexes(*left, indexes)),
            right: Box::new(select_indexes(*right, indexes)),
            condition,
            join_type,
        },
        PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => PhysicalPlan::NestedLoopJoin {
            left: Box::new(select_indexes(*left, indexes)),
            right: Box::new(select_indexes(*right, indexes)),
            condition,
            join_type,
        },
        PhysicalPlan::HashAggregate { input, group_by, aggregates, having } => PhysicalPlan::HashAggregate {
            input: Box::new(select_indexes(*input, indexes)),
            group_by,
            aggregates,
            having,
        },
        PhysicalPlan::Sort { input, order_by } => PhysicalPlan::Sort {
            input: Box::new(select_indexes(*input, indexes)),
            order_by,
        },
        PhysicalPlan::Limit { input, count, offset } => PhysicalPlan::Limit {
            input: Box::new(select_indexes(*input, indexes)),
            count,
            offset,
        },
        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(select_indexes(*input, indexes)),
        },
        other => other,
    }
}

fn find_index_predicate(
    pred: &Expr,
    idxs: &[IndexInfo],
) -> Option<(String, String, BinOp, Expr)> {
    let mut conjuncts = Vec::new();
    collect_conjuncts(pred, &mut conjuncts);

    let mut range_match: Option<(String, String, BinOp, Expr)> = None;
    for c in &conjuncts {
        if let Some((col, op, val)) = as_col_op_literal(c) {
            if let Some(ix) = idxs.iter().find(|i| i.columns.first().map(|s| s.as_str()) == Some(col.as_str())) {
                match op {
                    BinOp::Eq => return Some((ix.name.clone(), col, op, val)),
                    BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => {
                        if range_match.is_none() {
                            range_match = Some((ix.name.clone(), col, op, val));
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    range_match
}

fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp { left, op: BinOp::And, right } => {
            collect_conjuncts(left, out);
            collect_conjuncts(right, out);
        }
        other => out.push(other),
    }
}

fn as_col_op_literal(expr: &Expr) -> Option<(String, BinOp, Expr)> {
    let Expr::BinaryOp { left, op, right } = expr else { return None };
    if !matches!(op, BinOp::Eq | BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte) {
        return None;
    }
    let col_name = |e: &Expr| -> Option<String> {
        match e {
            Expr::Column(n) => Some(n.clone()),
            Expr::QualifiedColumn(_, n) => Some(n.clone()),
            _ => None,
        }
    };
    if let (Some(c), Expr::Literal(_)) = (col_name(left), right.as_ref()) {
        return Some((c, *op, (**right).clone()));
    }
    if let (Expr::Literal(_), Some(c)) = (left.as_ref(), col_name(right)) {
        let flipped = match op {
            BinOp::Lt => BinOp::Gt,
            BinOp::Gt => BinOp::Lt,
            BinOp::Lte => BinOp::Gte,
            BinOp::Gte => BinOp::Lte,
            other => *other,
        };
        return Some((c, flipped, (**left).clone()));
    }
    None
}

fn convert_to_physical(plan: LogicalPlan, stats: &StatsCatalog) -> Result<PhysicalPlan> {
    match plan {
        LogicalPlan::Scan { table, filter } => {
            Ok(PhysicalPlan::SeqScan { table, filter, limit: None, needed_columns: None })
        }
        LogicalPlan::Filter { input, predicate } => {
            let input = convert_to_physical(*input, stats)?;
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
            let input = convert_to_physical(*input, stats)?;
            Ok(PhysicalPlan::Project {
                input: Box::new(input),
                columns,
            })
        }
        LogicalPlan::Join { .. } => {

            let (rels, joins) = flatten_join_chain(plan);
            let (ordered_rels, ordered_joins) = reorder_join_chain(rels, joins, stats);
            build_physical_join_chain(ordered_rels, ordered_joins, stats)
        }
        LogicalPlan::Aggregate { input, group_by, aggregates, having } => {
            let input = convert_to_physical(*input, stats)?;
            Ok(PhysicalPlan::HashAggregate {
                input: Box::new(input),
                group_by,
                aggregates,
                having,
            })
        }
        LogicalPlan::Sort { input, order_by } => {
            let input = convert_to_physical(*input, stats)?;
            Ok(PhysicalPlan::Sort {
                input: Box::new(input),
                order_by,
            })
        }
        LogicalPlan::Limit { input, count, offset } => {
            let input = convert_to_physical(*input, stats)?;
            Ok(PhysicalPlan::Limit {
                input: Box::new(input),
                count,
                offset,
            })
        }
        LogicalPlan::Distinct { input } => {
            let input = convert_to_physical(*input, stats)?;
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

struct JoinEdge {
    condition: Expr,
    join_type: JoinType,
}

fn flatten_join_chain(plan: LogicalPlan) -> (Vec<LogicalPlan>, Vec<JoinEdge>) {
    let mut rels: Vec<LogicalPlan> = Vec::new();
    let mut joins: Vec<JoinEdge> = Vec::new();
    walk(plan, &mut rels, &mut joins);
    (rels, joins)
}

fn walk(plan: LogicalPlan, rels: &mut Vec<LogicalPlan>, joins: &mut Vec<JoinEdge>) {
    match plan {
        LogicalPlan::Join { left, right, condition, join_type } => {
            walk(*left, rels, joins);
            joins.push(JoinEdge { condition, join_type });

            rels.push(*right);
        }
        other => rels.push(other),
    }
}

fn build_physical_join_chain(
    rels: Vec<LogicalPlan>,
    joins: Vec<JoinEdge>,
    stats: &StatsCatalog,
) -> Result<PhysicalPlan> {
    let mut iter = rels.into_iter();
    let first = iter.next().expect("join chain must have at least one relation");
    let mut current = convert_to_physical(first, stats)?;
    for (right, edge) in iter.zip(joins.into_iter()) {
        let right_phys = convert_to_physical(right, stats)?;
        current = PhysicalPlan::HashJoin {
            left: Box::new(current),
            right: Box::new(right_phys),
            condition: edge.condition,
            join_type: edge.join_type,
        };
    }
    Ok(current)
}

fn reorder_join_chain(
    rels: Vec<LogicalPlan>,
    joins: Vec<JoinEdge>,
    stats: &StatsCatalog,
) -> (Vec<LogicalPlan>, Vec<JoinEdge>) {

    if rels.len() < 2 || joins.is_empty() {
        return (rels, joins);
    }
    if !chain_has_stats(&rels, stats) {
        return (rels, joins);
    }
    if joins.iter().any(|j| !matches!(j.join_type, JoinType::Inner)) {
        return (rels, joins);
    }

    let leaf_costs: Vec<f64> = rels.iter()
        .map(|r| cost_plan(&leaf_physical_for_cost(r), stats).rows)
        .collect();

    let mut remaining: Vec<usize> = (0..rels.len()).collect();
    let seed_pos = remaining.iter().enumerate()
        .min_by(|(_, a), (_, b)| {
            leaf_costs[**a]
                .partial_cmp(&leaf_costs[**b])
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(idx, _)| idx)
        .unwrap_or(0);
    let first_idx = remaining.remove(seed_pos);
    let mut order: Vec<usize> = vec![first_idx];
    let mut running_rows = leaf_costs[first_idx];

    while !remaining.is_empty() {
        let pick_pos = remaining.iter().enumerate()
            .min_by(|(_, a), (_, b)| {
                let est_a = (running_rows * leaf_costs[**a])
                    / running_rows.max(leaf_costs[**a]).max(1.0);
                let est_b = (running_rows * leaf_costs[**b])
                    / running_rows.max(leaf_costs[**b]).max(1.0);
                est_a.partial_cmp(&est_b).unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let next = remaining.remove(pick_pos);
        running_rows = (running_rows * leaf_costs[next])
            / running_rows.max(leaf_costs[next]).max(1.0);
        order.push(next);
    }

    if order.iter().enumerate().all(|(i, &idx)| i == idx) {
        return (rels, joins);
    }

    let mut by_index: Vec<Option<LogicalPlan>> = rels.into_iter().map(Some).collect();
    let new_rels: Vec<LogicalPlan> = order.iter()
        .map(|&i| by_index[i].take().expect("each rel taken at most once"))
        .collect();

    (new_rels, joins)
}

fn chain_has_stats(rels: &[LogicalPlan], stats: &StatsCatalog) -> bool {
    rels.iter().all(|r| match r {
        LogicalPlan::Scan { table, .. } => stats.contains_key(table),
        _ => false,
    })
}

fn leaf_physical_for_cost(plan: &LogicalPlan) -> PhysicalPlan {
    match plan {
        LogicalPlan::Scan { table, filter } => PhysicalPlan::SeqScan {
            table: table.clone(),
            filter: filter.clone(),
            limit: None,
            needed_columns: None,
        },

        _ => PhysicalPlan::SeqScan {
            table: String::new(),
            filter: None,
            limit: None,
            needed_columns: None,
        },
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytedb_core::stats::{compute_table_stats, TableStats, DEFAULT_HISTOGRAM_BUCKETS, DEFAULT_MCV_COUNT};
    use bytedb_core::tuple::value::Value;

    fn stats_for(name: &str, n: usize) -> TableStats {
        let rows: Vec<Vec<Value>> = (0..n as i64)
            .map(|i| vec![Value::Int64(i)])
            .collect();
        compute_table_stats(
            name,
            &["id".to_string()],
            rows,
            DEFAULT_MCV_COUNT,
            DEFAULT_HISTOGRAM_BUCKETS,
        )
    }

    fn scan(t: &str) -> LogicalPlan {
        LogicalPlan::Scan { table: t.to_string(), filter: None }
    }

    fn join(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            condition: Expr::Literal(LiteralValue::Bool(true)),
            join_type: JoinType::Inner,
        }
    }

    fn first_table(p: &PhysicalPlan) -> Option<&str> {
        match p {
            PhysicalPlan::SeqScan { table, .. } => Some(table),
            PhysicalPlan::HashJoin { left, .. } => first_table(left),
            PhysicalPlan::NestedLoopJoin { left, .. } => first_table(left),
            PhysicalPlan::Filter { input, .. } => first_table(input),
            PhysicalPlan::Project { input, .. } => first_table(input),
            _ => None,
        }
    }

    #[test]
    fn no_stats_preserves_source_order() {
        let plan = join(join(scan("a"), scan("b")), scan("c"));
        let phys = optimize(plan).unwrap();

        assert_eq!(first_table(&phys), Some("a"));
    }

    #[test]
    fn smallest_table_drives_when_stats_present() {
        let mut catalog = StatsCatalog::new();
        catalog.insert("big".into(), stats_for("big", 10_000));
        catalog.insert("small".into(), stats_for("small", 5));
        catalog.insert("medium".into(), stats_for("medium", 500));

        let plan = join(join(scan("big"), scan("medium")), scan("small"));
        let phys = optimize_with_stats(plan, &catalog).unwrap();
        assert_eq!(first_table(&phys), Some("small"));
    }

    #[test]
    fn outer_join_keeps_source_order() {
        let mut catalog = StatsCatalog::new();
        catalog.insert("big".into(), stats_for("big", 10_000));
        catalog.insert("small".into(), stats_for("small", 5));

        let plan = LogicalPlan::Join {
            left: Box::new(scan("big")),
            right: Box::new(scan("small")),
            condition: Expr::Literal(LiteralValue::Bool(true)),
            join_type: JoinType::Left,
        };
        let phys = optimize_with_stats(plan, &catalog).unwrap();
        assert_eq!(first_table(&phys), Some("big"));
    }

    #[test]
    fn missing_stats_for_one_relation_falls_back() {
        let mut catalog = StatsCatalog::new();
        catalog.insert("big".into(), stats_for("big", 10_000));

        let plan = join(scan("big"), scan("small"));
        let phys = optimize_with_stats(plan, &catalog).unwrap();
        assert_eq!(first_table(&phys), Some("big"));
    }
}
