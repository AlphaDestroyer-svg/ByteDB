use std::collections::HashMap;

use bytedb_core::stats::TableStats;
use bytedb_core::tuple::value::Value;

use crate::parser::ast::*;
use crate::planner::physical_plan::PhysicalPlan;

pub const CPU_TUPLE_COST: f64 = 0.01;

pub const CPU_OPERATOR_COST: f64 = 0.0025;

pub const IO_SEQ_PAGE_COST: f64 = 1.0;

pub const IO_RANDOM_PAGE_COST: f64 = 4.0;

pub const MIN_CARD: f64 = 1.0;

pub const DEFAULT_EQ_SELECTIVITY: f64 = 0.005;
pub const DEFAULT_RANGE_SELECTIVITY: f64 = 0.333;

#[derive(Debug, Clone, Copy)]
pub struct PlanCost {

    pub rows: f64,

    pub total_cost: f64,

    pub startup_cost: f64,
}

impl PlanCost {
    pub fn new(rows: f64, startup_cost: f64, total_cost: f64) -> Self {
        Self {
            rows: rows.max(MIN_CARD),
            startup_cost: startup_cost.max(0.0),
            total_cost: total_cost.max(startup_cost.max(0.0)),
        }
    }

    pub fn empty() -> Self {
        Self { rows: MIN_CARD, startup_cost: 0.0, total_cost: 0.0 }
    }
}

pub type StatsCatalog = HashMap<String, TableStats>;

pub fn estimate_selectivity(
    expr: &Expr,
    table_stats: Option<&TableStats>,
) -> f64 {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinOp::And => {
                let a = estimate_selectivity(left, table_stats);
                let b = estimate_selectivity(right, table_stats);
                (a * b).clamp(0.0, 1.0)
            }
            BinOp::Or => {
                let a = estimate_selectivity(left, table_stats);
                let b = estimate_selectivity(right, table_stats);
                (a + b - a * b).clamp(0.0, 1.0)
            }
            BinOp::Eq | BinOp::Neq => {
                let sel = column_eq_selectivity(left, right, table_stats)
                    .or_else(|| column_eq_selectivity(right, left, table_stats))
                    .unwrap_or(DEFAULT_EQ_SELECTIVITY);
                if matches!(op, BinOp::Neq) {
                    (1.0 - sel).max(MIN_CARD / f64::MAX)
                } else {
                    sel
                }
            }
            BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => {
                column_range_selectivity(left, *op, right, table_stats)
                    .or_else(|| column_range_selectivity(right, flip_op(*op), left, table_stats))
                    .unwrap_or(DEFAULT_RANGE_SELECTIVITY)
            }
            BinOp::Like | BinOp::Ilike => 0.10,
            BinOp::Plus | BinOp::Minus | BinOp::Mul | BinOp::Div | BinOp::Mod => 1.0,
        },
        Expr::IsNull(inner) => column_null_fraction(inner, table_stats)
            .unwrap_or(DEFAULT_EQ_SELECTIVITY),
        Expr::IsNotNull(inner) => 1.0
            - column_null_fraction(inner, table_stats)
                .unwrap_or(DEFAULT_EQ_SELECTIVITY),
        Expr::InList { list, .. } => {

            let n = list.len().max(1) as f64;
            let eq = DEFAULT_EQ_SELECTIVITY;
            (1.0 - (1.0 - eq).powf(n)).clamp(0.0, 1.0)
        }
        Expr::Between { .. } => DEFAULT_RANGE_SELECTIVITY,
        Expr::UnaryOp { op: UnaryOp::Not, expr } => {
            (1.0 - estimate_selectivity(expr, table_stats)).clamp(0.0, 1.0)
        }
        _ => 1.0,
    }
}

fn flip_op(op: BinOp) -> BinOp {
    match op {
        BinOp::Lt => BinOp::Gt,
        BinOp::Lte => BinOp::Gte,
        BinOp::Gt => BinOp::Lt,
        BinOp::Gte => BinOp::Lte,
        other => other,
    }
}

fn column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(c) => Some(c.as_str()),
        Expr::QualifiedColumn(_, c) => Some(c.as_str()),
        _ => None,
    }
}

fn literal_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Literal(LiteralValue::Integer(i)) => Some(Value::Int64(*i)),
        Expr::Literal(LiteralValue::Float(f)) => Some(Value::Float64(*f)),
        Expr::Literal(LiteralValue::String(s)) => Some(Value::Text(s.clone())),
        Expr::Literal(LiteralValue::Bool(b)) => Some(Value::Bool(*b)),
        Expr::Literal(LiteralValue::Null) => Some(Value::Null),
        _ => None,
    }
}

fn column_eq_selectivity(
    col: &Expr,
    lit: &Expr,
    ts: Option<&TableStats>,
) -> Option<f64> {
    let cname = column_name(col)?;
    let val = literal_value(lit)?;
    let ts = ts?;
    let cs = ts.column(cname)?;

    for (mcv_val, freq) in &cs.mcv {
        if mcv_val.compare(&val) == Some(std::cmp::Ordering::Equal) {
            return Some((*freq).clamp(MIN_CARD / f64::MAX, 1.0));
        }
    }

    let mcv_total: f64 = cs.mcv.iter().map(|(_, f)| f).sum();
    let non_null_remain = (1.0 - cs.null_fraction - mcv_total).max(0.0);
    let remaining_ndv = (cs.ndv as f64 - cs.mcv.len() as f64).max(1.0);
    Some((non_null_remain / remaining_ndv).max(MIN_CARD / f64::MAX))
}

fn column_range_selectivity(
    col: &Expr,
    op: BinOp,
    lit: &Expr,
    ts: Option<&TableStats>,
) -> Option<f64> {
    let cname = column_name(col)?;
    let val = literal_value(lit)?;
    let ts = ts?;
    let cs = ts.column(cname)?;
    if cs.bucket_bounds.len() < 2 {
        return Some(DEFAULT_RANGE_SELECTIVITY);
    }

    let n_buckets = (cs.bucket_bounds.len() - 1) as f64;
    let mut below_buckets = 0.0;
    for win in cs.bucket_bounds.windows(2) {
        let (lo, hi) = (&win[0], &win[1]);
        let cmp_hi = hi.compare(&val);
        match cmp_hi {
            Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal) => below_buckets += 1.0,
            _ => {
                if lo.compare(&val) == Some(std::cmp::Ordering::Less) {
                    below_buckets += 0.5;
                }
                break;
            }
        }
    }
    let mcv_total: f64 = cs.mcv.iter().map(|(_, f)| f).sum();
    let hist_mass = (1.0 - cs.null_fraction - mcv_total).max(0.0);
    let frac_below = (below_buckets / n_buckets) * hist_mass;
    let frac_above = hist_mass - frac_below;

    let sel = match op {
        BinOp::Lt => frac_below,
        BinOp::Lte => frac_below + 0.0,
        BinOp::Gt => frac_above,
        BinOp::Gte => frac_above + 0.0,
        _ => return None,
    };
    Some(sel.clamp(MIN_CARD / f64::MAX, 1.0))
}

fn column_null_fraction(expr: &Expr, ts: Option<&TableStats>) -> Option<f64> {
    let cname = column_name(expr)?;
    let cs = ts?.column(cname)?;
    Some(cs.null_fraction)
}

pub fn cost_plan(plan: &PhysicalPlan, stats: &StatsCatalog) -> PlanCost {
    match plan {
        PhysicalPlan::SeqScan { table, filter, limit, .. } => {
            let ts = stats.get(table);
            let row_count = ts.map(|s| s.row_count as f64).unwrap_or(1000.0);
            let sel = filter.as_ref()
                .map(|f| estimate_selectivity(f, ts))
                .unwrap_or(1.0);
            let mut out_rows = (row_count * sel).max(MIN_CARD);
            if let Some(l) = limit {
                out_rows = out_rows.min(*l as f64);
            }
            let scan_cost = row_count * IO_SEQ_PAGE_COST
                + row_count * CPU_TUPLE_COST;
            PlanCost::new(out_rows, 0.0, scan_cost)
        }
        PhysicalPlan::IndexScan { table, .. } => {
            let ts = stats.get(table);
            let row_count = ts.map(|s| s.row_count as f64).unwrap_or(1000.0);

            let probe_cost = IO_RANDOM_PAGE_COST + (row_count.max(2.0)).log2() * CPU_TUPLE_COST;
            PlanCost::new(MIN_CARD, probe_cost * 0.25, probe_cost)
        }
        PhysicalPlan::Filter { input, predicate } => {
            let inner = cost_plan(input, stats);
            let table = leftmost_table(input);
            let ts = table.and_then(|t| stats.get(t));
            let sel = estimate_selectivity(predicate, ts);
            let out = (inner.rows * sel).max(MIN_CARD);
            let cost = inner.total_cost + inner.rows * CPU_TUPLE_COST;
            PlanCost::new(out, inner.startup_cost, cost)
        }
        PhysicalPlan::Project { input, .. } => {
            let inner = cost_plan(input, stats);
            let cost = inner.total_cost + inner.rows * CPU_OPERATOR_COST;
            PlanCost::new(inner.rows, inner.startup_cost, cost)
        }
        PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::NestedLoopJoin { left, right, .. } => {
            let l = cost_plan(left, stats);
            let r = cost_plan(right, stats);

            let denom = l.rows.max(r.rows).max(MIN_CARD);
            let out = (l.rows * r.rows / denom).max(MIN_CARD);

            let (build, probe) = if l.rows <= r.rows { (&l, &r) } else { (&r, &l) };
            let cost = build.total_cost
                + probe.total_cost
                + build.rows * CPU_TUPLE_COST * 2.0
                + probe.rows * CPU_TUPLE_COST;
            PlanCost::new(out, build.total_cost, cost)
        }
        PhysicalPlan::HashAggregate { input, group_by, .. } => {
            let inner = cost_plan(input, stats);

            let groups = if group_by.is_empty() {
                MIN_CARD
            } else {
                inner.rows.sqrt().max(MIN_CARD)
            };
            let cost = inner.total_cost + inner.rows * CPU_TUPLE_COST * 2.0;
            PlanCost::new(groups, inner.total_cost, cost)
        }
        PhysicalPlan::Sort { input, .. } => {
            let inner = cost_plan(input, stats);
            let n = inner.rows.max(2.0);
            let cost = inner.total_cost + n * n.log2() * CPU_OPERATOR_COST;

            PlanCost::new(inner.rows, cost, cost)
        }
        PhysicalPlan::Limit { input, count, .. } => {
            let inner = cost_plan(input, stats);
            let out = inner.rows.min(*count as f64).max(MIN_CARD);

            let frac = (out / inner.rows.max(MIN_CARD)).clamp(0.0, 1.0);
            let total = inner.startup_cost + (inner.total_cost - inner.startup_cost) * frac;
            PlanCost::new(out, inner.startup_cost, total)
        }
        PhysicalPlan::Distinct { input } => {
            let inner = cost_plan(input, stats);
            let groups = inner.rows.sqrt().max(MIN_CARD);
            let cost = inner.total_cost + inner.rows * CPU_TUPLE_COST;
            PlanCost::new(groups, inner.total_cost, cost)
        }
        PhysicalPlan::Insert { .. }
        | PhysicalPlan::Update { .. }
        | PhysicalPlan::Delete { .. }
        | PhysicalPlan::CreateTable(_)
        | PhysicalPlan::DropTable(_)
        | PhysicalPlan::CreateIndex(_)
        | PhysicalPlan::DropIndex(_) => PlanCost::empty(),
    }
}

fn leftmost_table(plan: &PhysicalPlan) -> Option<&str> {
    match plan {
        PhysicalPlan::SeqScan { table, .. } => Some(table),
        PhysicalPlan::IndexScan { table, .. } => Some(table),
        PhysicalPlan::Filter { input, .. } => leftmost_table(input),
        PhysicalPlan::Project { input, .. } => leftmost_table(input),
        PhysicalPlan::Limit { input, .. } => leftmost_table(input),
        PhysicalPlan::Sort { input, .. } => leftmost_table(input),
        PhysicalPlan::HashAggregate { input, .. } => leftmost_table(input),
        PhysicalPlan::Distinct { input } => leftmost_table(input),
        PhysicalPlan::HashJoin { left, .. }
        | PhysicalPlan::NestedLoopJoin { left, .. } => leftmost_table(left),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytedb_core::stats::{compute_table_stats, DEFAULT_HISTOGRAM_BUCKETS, DEFAULT_MCV_COUNT};
    use bytedb_core::tuple::value::Value;

    fn rows_for(values: Vec<Value>) -> Vec<Vec<Value>> {
        values.into_iter().map(|v| vec![v]).collect()
    }

    fn mk_stats(name: &str, vals: Vec<Value>) -> TableStats {
        compute_table_stats(
            name,
            &["col".to_string()],
            rows_for(vals),
            DEFAULT_MCV_COUNT,
            DEFAULT_HISTOGRAM_BUCKETS,
        )
    }

    #[test]
    fn equality_on_mcv_returns_freq() {
        let mut vs = Vec::new();
        for _ in 0..100 { vs.push(Value::Int64(7)); }
        for i in 0..50 { vs.push(Value::Int64(i)); }
        let ts = mk_stats("t", vs);
        let pred = Expr::BinaryOp {
            left: Box::new(Expr::Column("col".to_string())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(LiteralValue::Integer(7))),
        };
        let s = estimate_selectivity(&pred, Some(&ts));

        assert!(s > 0.5 && s < 0.8, "sel was {}", s);
    }

    #[test]
    fn equality_off_mcv_uses_remaining_ndv() {
        let vs: Vec<Value> = (0..100).map(Value::Int64).collect();
        let ts = mk_stats("t", vs);
        let pred = Expr::BinaryOp {
            left: Box::new(Expr::Column("col".to_string())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(LiteralValue::Integer(99999))),
        };
        let s = estimate_selectivity(&pred, Some(&ts));

        assert!(s < 0.05, "sel was {}", s);
    }

    #[test]
    fn and_or_combine() {
        let ts = mk_stats("t", (0..10).map(Value::Int64).collect());
        let p1 = Expr::BinaryOp {
            left: Box::new(Expr::Column("col".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(LiteralValue::Integer(1))),
        };
        let p2 = p1.clone();
        let and_ = Expr::BinaryOp { left: Box::new(p1.clone()), op: BinOp::And, right: Box::new(p2.clone()) };
        let or_ = Expr::BinaryOp { left: Box::new(p1.clone()), op: BinOp::Or, right: Box::new(p2.clone()) };
        let single = estimate_selectivity(&p1, Some(&ts));
        let conj = estimate_selectivity(&and_, Some(&ts));
        let disj = estimate_selectivity(&or_, Some(&ts));
        assert!(conj <= single);
        assert!(disj >= single);
        assert!(disj <= 1.0);
    }

    #[test]
    fn cost_seqscan_with_filter_is_lower_rows() {
        let ts = mk_stats("t", (0..1000).map(Value::Int64).collect());
        let mut catalog = StatsCatalog::new();
        catalog.insert("t".to_string(), ts);

        let unfiltered = PhysicalPlan::SeqScan {
            table: "t".into(),
            filter: None,
            limit: None,
            needed_columns: None,
        };
        let filtered = PhysicalPlan::SeqScan {
            table: "t".into(),
            filter: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("col".into())),
                op: BinOp::Eq,
                right: Box::new(Expr::Literal(LiteralValue::Integer(42))),
            }),
            limit: None,
            needed_columns: None,
        };
        let c1 = cost_plan(&unfiltered, &catalog);
        let c2 = cost_plan(&filtered, &catalog);
        assert!(c2.rows < c1.rows);

        assert!((c1.total_cost - c2.total_cost).abs() < 1.0);
    }

    #[test]
    fn limit_caps_rows_and_lowers_cost() {
        let ts = mk_stats("t", (0..1000).map(Value::Int64).collect());
        let mut catalog = StatsCatalog::new();
        catalog.insert("t".to_string(), ts);
        let scan = PhysicalPlan::SeqScan {
            table: "t".into(),
            filter: None,
            limit: None,
            needed_columns: None,
        };
        let limited = PhysicalPlan::Limit {
            input: Box::new(scan.clone()),
            count: 5,
            offset: 0,
        };
        let c_full = cost_plan(&scan, &catalog);
        let c_lim = cost_plan(&limited, &catalog);
        assert!(c_lim.rows <= 5.0);
        assert!(c_lim.total_cost <= c_full.total_cost);
    }

    #[test]
    fn missing_stats_falls_back_to_defaults() {
        let catalog = StatsCatalog::new();
        let scan = PhysicalPlan::SeqScan {
            table: "unknown".into(),
            filter: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("x".into())),
                op: BinOp::Eq,
                right: Box::new(Expr::Literal(LiteralValue::Integer(1))),
            }),
            limit: None,
            needed_columns: None,
        };
        let c = cost_plan(&scan, &catalog);

        assert!(c.rows < 100.0);
        assert!(c.total_cost > 0.0);
    }
}
