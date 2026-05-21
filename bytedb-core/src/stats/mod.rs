use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::tuple::value::Value;

pub const DEFAULT_MCV_COUNT: usize = 10;
pub const DEFAULT_HISTOGRAM_BUCKETS: usize = 10;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnStats {
    pub column: String,

    pub null_fraction: f64,

    pub ndv: u64,

    pub mcv: Vec<(Value, f64)>,

    pub bucket_bounds: Vec<Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableStats {
    pub table: String,
    pub row_count: u64,
    pub columns: Vec<ColumnStats>,

    pub computed_at_secs: u64,
}

impl TableStats {
    pub fn column(&self, name: &str) -> Option<&ColumnStats> {
        self.columns.iter().find(|c| c.column == name)
    }
}

pub fn compute_table_stats(
    table: impl Into<String>,
    column_names: &[String],
    rows: impl IntoIterator<Item = Vec<Value>>,
    mcv_count: usize,
    histogram_buckets: usize,
) -> TableStats {
    let table = table.into();
    let mut row_count: u64 = 0;
    let mut per_col_values: Vec<Vec<Value>> =
        column_names.iter().map(|_| Vec::new()).collect();

    for row in rows {
        row_count += 1;
        for (i, col_values) in per_col_values.iter_mut().enumerate() {
            let v = row.get(i).cloned().unwrap_or(Value::Null);
            col_values.push(v);
        }
    }

    let columns: Vec<ColumnStats> = column_names
        .iter()
        .zip(per_col_values.into_iter())
        .map(|(name, vals)| {
            compute_column_stats(name.clone(), vals, mcv_count, histogram_buckets)
        })
        .collect();

    TableStats {
        table,
        row_count,
        columns,
        computed_at_secs: now_secs(),
    }
}

fn compute_column_stats(
    name: String,
    values: Vec<Value>,
    mcv_count: usize,
    histogram_buckets: usize,
) -> ColumnStats {
    let total = values.len() as f64;
    if total == 0.0 {
        return ColumnStats {
            column: name,
            ..Default::default()
        };
    }

    let mut null_count = 0u64;
    let mut non_null: Vec<Value> = Vec::with_capacity(values.len());
    for v in values {
        if matches!(v, Value::Null) {
            null_count += 1;
        } else {
            non_null.push(v);
        }
    }
    let null_fraction = null_count as f64 / total;

    let mut freq: HashMap<Vec<u8>, (Value, u64)> = HashMap::new();
    for v in &non_null {
        let key = canonical_key(v);
        freq.entry(key).or_insert_with(|| (v.clone(), 0)).1 += 1;
    }
    let ndv = freq.len() as u64;

    let mut entries: Vec<(Value, u64)> =
        freq.values().map(|(v, c)| (v.clone(), *c)).collect();
    entries.sort_by(|a, b| b.1.cmp(&a.1));
    let mcv: Vec<(Value, f64)> = entries
        .iter()
        .take(mcv_count)
        .map(|(v, c)| (v.clone(), *c as f64 / total))
        .collect();

    let mcv_keys: std::collections::HashSet<Vec<u8>> = mcv
        .iter()
        .map(|(v, _)| canonical_key(v))
        .collect();
    let mut hist_input: Vec<Value> = non_null
        .into_iter()
        .filter(|v| !mcv_keys.contains(&canonical_key(v)))
        .collect();
    hist_input.sort_by(|a, b| {
        a.compare(b)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let bucket_bounds = if hist_input.len() < 2 || histogram_buckets < 2 {
        Vec::new()
    } else {
        let mut bounds = Vec::with_capacity(histogram_buckets + 1);
        let n = hist_input.len();
        for b in 0..=histogram_buckets {
            let idx = ((b as u64 * (n.saturating_sub(1)) as u64)
                / histogram_buckets as u64) as usize;
            bounds.push(hist_input[idx.min(n - 1)].clone());
        }
        bounds
    };

    ColumnStats {
        column: name,
        null_fraction,
        ndv,
        mcv,
        bucket_bounds,
    }
}

fn canonical_key(v: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    match v {
        Value::Null => out.push(0x00),
        Value::Bool(b) => {
            out.push(0x01);
            out.push(if *b { 1 } else { 0 });
        }
        Value::Int64(i) => {
            out.push(0x02);
            out.extend_from_slice(&i.to_be_bytes());
        }
        Value::Float64(f) => {
            out.push(0x03);
            out.extend_from_slice(&f.to_be_bytes());
        }
        Value::Text(s) => {
            out.push(0x04);
            out.extend_from_slice(s.as_bytes());
        }
        Value::Bytes(b) => {
            out.push(0x05);
            out.extend_from_slice(b);
        }
        Value::Json(j) => {
            out.push(0x06);
            out.extend_from_slice(j.to_string().as_bytes());
        }
        Value::Date(d) => {
            out.push(0x07);
            out.extend_from_slice(&d.to_be_bytes());
        }
        Value::Timestamp(t) => {
            out.push(0x08);
            out.extend_from_slice(&t.to_be_bytes());
        }
        Value::Decimal(mantissa, scale) => {
            out.push(0x09);
            out.extend_from_slice(&mantissa.to_be_bytes());
            out.push(*scale);
        }
        Value::Uuid(u) => {
            out.push(0x0A);
            out.extend_from_slice(u);
        }
    }
    out
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuple::value::Value;

    fn rows(rs: Vec<Vec<Value>>) -> impl IntoIterator<Item = Vec<Value>> {
        rs.into_iter().collect::<Vec<_>>()
    }

    #[test]
    fn empty_table_yields_zero_row_count() {
        let s = compute_table_stats(
            "t",
            &["a".to_string()],
            Vec::<Vec<Value>>::new(),
            10,
            10,
        );
        assert_eq!(s.row_count, 0);
        assert_eq!(s.columns.len(), 1);
        assert_eq!(s.columns[0].ndv, 0);
    }

    #[test]
    fn null_fraction_is_correct() {
        let s = compute_table_stats(
            "t",
            &["a".to_string()],
            rows(vec![
                vec![Value::Int64(1)],
                vec![Value::Null],
                vec![Value::Null],
                vec![Value::Int64(2)],
            ]),
            10,
            10,
        );
        assert_eq!(s.row_count, 4);
        assert!((s.columns[0].null_fraction - 0.5).abs() < 1e-9);
        assert_eq!(s.columns[0].ndv, 2);
    }

    #[test]
    fn mcv_captures_top_values() {
        let mut rs = Vec::new();
        for _ in 0..100 {
            rs.push(vec![Value::Int64(7)]);
        }
        for i in 0..50 {
            rs.push(vec![Value::Int64(i)]);
        }
        let s = compute_table_stats("t", &["a".to_string()], rs, 5, 10);
        let mcv = &s.columns[0].mcv;
        assert!(!mcv.is_empty());

        let (top_val, top_freq) = &mcv[0];
        assert_eq!(top_val.compare(&Value::Int64(7)), Some(std::cmp::Ordering::Equal));
        assert!(*top_freq > 0.6 && *top_freq < 0.7);
    }

    #[test]
    fn histogram_has_n_plus_1_bounds() {
        let mut rs = Vec::new();
        for i in 0..100 {
            rs.push(vec![Value::Int64(i)]);
        }
        let s = compute_table_stats("t", &["a".to_string()], rs, 0, 10);

        assert_eq!(s.columns[0].bucket_bounds.len(), 11);
    }
}
