use bytedb_core::tuple::value::Value;
use crate::parser::ast::BinOp;

pub const BATCH_SIZE: usize = 1024;

#[derive(Debug, Clone)]
pub enum ColumnVector {
    Int64(Vec<i64>, Vec<bool>),
    Float64(Vec<f64>, Vec<bool>),
    Bool(Vec<bool>, Vec<bool>),
    Text(Vec<String>, Vec<bool>),
    Null(usize),
}

#[derive(Debug, Clone)]
pub struct RecordBatch {
    pub columns: Vec<ColumnVector>,
    pub num_rows: usize,
}

#[derive(Debug, Clone)]
pub struct SelectionVector {
    pub indices: Vec<u16>,
}

impl ColumnVector {
    pub fn len(&self) -> usize {
        match self {
            ColumnVector::Int64(v, _) => v.len(),
            ColumnVector::Float64(v, _) => v.len(),
            ColumnVector::Bool(v, _) => v.len(),
            ColumnVector::Text(v, _) => v.len(),
            ColumnVector::Null(n) => *n,
        }
    }
}

impl RecordBatch {
    pub fn new(columns: Vec<ColumnVector>, num_rows: usize) -> Self {
        RecordBatch { columns, num_rows }
    }

    pub fn filter_int64_column(&self, col_idx: usize, op: BinOp, literal: i64) -> SelectionVector {
        let mut indices = Vec::with_capacity(self.num_rows);
        if let Some(ColumnVector::Int64(values, nulls)) = self.columns.get(col_idx) {
            for i in 0..self.num_rows {
                if nulls[i] {
                    continue;
                }
                let v = values[i];
                let matches = match op {
                    BinOp::Eq => v == literal,
                    BinOp::Neq => v != literal,
                    BinOp::Lt => v < literal,
                    BinOp::Gt => v > literal,
                    BinOp::Lte => v <= literal,
                    BinOp::Gte => v >= literal,
                    _ => true,
                };
                if matches {
                    indices.push(i as u16);
                }
            }
        }
        SelectionVector { indices }
    }

    pub fn filter_float64_column(&self, col_idx: usize, op: BinOp, literal: f64) -> SelectionVector {
        let mut indices = Vec::with_capacity(self.num_rows);
        if let Some(ColumnVector::Float64(values, nulls)) = self.columns.get(col_idx) {
            for i in 0..self.num_rows {
                if nulls[i] {
                    continue;
                }
                let v = values[i];
                let matches = match op {
                    BinOp::Eq => v == literal,
                    BinOp::Neq => v != literal,
                    BinOp::Lt => v < literal,
                    BinOp::Gt => v > literal,
                    BinOp::Lte => v <= literal,
                    BinOp::Gte => v >= literal,
                    _ => true,
                };
                if matches {
                    indices.push(i as u16);
                }
            }
        }
        SelectionVector { indices }
    }

    pub fn filter_text_column(&self, col_idx: usize, op: BinOp, literal: &str) -> SelectionVector {
        let mut indices = Vec::with_capacity(self.num_rows);
        if let Some(ColumnVector::Text(values, nulls)) = self.columns.get(col_idx) {
            for i in 0..self.num_rows {
                if nulls[i] {
                    continue;
                }
                let v = values[i].as_str();
                let matches = match op {
                    BinOp::Eq => v == literal,
                    BinOp::Neq => v != literal,
                    BinOp::Lt => v < literal,
                    BinOp::Gt => v > literal,
                    BinOp::Lte => v <= literal,
                    BinOp::Gte => v >= literal,
                    _ => true,
                };
                if matches {
                    indices.push(i as u16);
                }
            }
        }
        SelectionVector { indices }
    }

    pub fn materialize_rows(&self, sel: &SelectionVector) -> Vec<Vec<Value>> {
        let mut rows = Vec::with_capacity(sel.indices.len());
        for &idx in &sel.indices {
            let i = idx as usize;
            let mut row = Vec::with_capacity(self.columns.len());
            for col in &self.columns {
                let val = match col {
                    ColumnVector::Int64(values, nulls) => {
                        if nulls[i] { Value::Null } else { Value::Int64(values[i]) }
                    }
                    ColumnVector::Float64(values, nulls) => {
                        if nulls[i] { Value::Null } else { Value::Float64(values[i]) }
                    }
                    ColumnVector::Bool(values, nulls) => {
                        if nulls[i] { Value::Null } else { Value::Bool(values[i]) }
                    }
                    ColumnVector::Text(values, nulls) => {
                        if nulls[i] { Value::Null } else { Value::Text(values[i].clone()) }
                    }
                    ColumnVector::Null(_) => Value::Null,
                };
                row.push(val);
            }
            rows.push(row);
        }
        rows
    }

    pub fn materialize_all_rows(&self) -> Vec<Vec<Value>> {
        let mut rows = Vec::with_capacity(self.num_rows);
        for i in 0..self.num_rows {
            let mut row = Vec::with_capacity(self.columns.len());
            for col in &self.columns {
                let val = match col {
                    ColumnVector::Int64(values, nulls) => {
                        if nulls[i] { Value::Null } else { Value::Int64(values[i]) }
                    }
                    ColumnVector::Float64(values, nulls) => {
                        if nulls[i] { Value::Null } else { Value::Float64(values[i]) }
                    }
                    ColumnVector::Bool(values, nulls) => {
                        if nulls[i] { Value::Null } else { Value::Bool(values[i]) }
                    }
                    ColumnVector::Text(values, nulls) => {
                        if nulls[i] { Value::Null } else { Value::Text(values[i].clone()) }
                    }
                    ColumnVector::Null(_) => Value::Null,
                };
                row.push(val);
            }
            rows.push(row);
        }
        rows
    }
}

pub fn deserialize_batch(data_slices: &[&[u8]], num_columns: usize) -> Option<RecordBatch> {
    if data_slices.is_empty() || num_columns == 0 {
        return None;
    }

    let batch_size = data_slices.len();
    let mut columns: Vec<ColumnVector> = Vec::with_capacity(num_columns);

    for _ in 0..num_columns {
        columns.push(ColumnVector::Null(0));
    }

    let mut int_cols: Vec<Vec<i64>> = vec![Vec::with_capacity(batch_size); num_columns];
    let mut float_cols: Vec<Vec<f64>> = vec![Vec::with_capacity(batch_size); num_columns];
    let mut text_cols: Vec<Vec<String>> = vec![Vec::with_capacity(batch_size); num_columns];
    let mut bool_cols: Vec<Vec<bool>> = vec![Vec::with_capacity(batch_size); num_columns];
    let mut null_flags: Vec<Vec<bool>> = vec![Vec::with_capacity(batch_size); num_columns];
    let mut col_types: Vec<u8> = vec![255; num_columns];

    for data in data_slices {
        if data.is_empty() {
            return None;
        }
        let count = data[0] as usize;
        if count != num_columns {
            return None;
        }
        let mut pos = 1;
        for col in 0..num_columns {
            if pos >= data.len() {
                return None;
            }
            let tag = data[pos];
            pos += 1;

            if col_types[col] == 255 && tag != 0 {
                col_types[col] = tag;
            }

            match tag {
                0 => {
                    null_flags[col].push(true);
                    match col_types[col] {
                        2 => int_cols[col].push(0),
                        3 => float_cols[col].push(0.0),
                        4 => text_cols[col].push(String::new()),
                        1 => bool_cols[col].push(false),
                        _ => int_cols[col].push(0),
                    }
                }
                1 => {
                    if pos >= data.len() { return None; }
                    let v = data[pos] != 0;
                    pos += 1;
                    null_flags[col].push(false);
                    bool_cols[col].push(v);
                }
                2 => {
                    if pos + 8 > data.len() { return None; }
                    let n = i64::from_le_bytes(data[pos..pos+8].try_into().ok()?);
                    pos += 8;
                    null_flags[col].push(false);
                    int_cols[col].push(n);
                }
                3 => {
                    if pos + 8 > data.len() { return None; }
                    let f = f64::from_le_bytes(data[pos..pos+8].try_into().ok()?);
                    pos += 8;
                    null_flags[col].push(false);
                    float_cols[col].push(f);
                }
                4 => {
                    if pos + 4 > data.len() { return None; }
                    let len = u32::from_le_bytes(data[pos..pos+4].try_into().ok()?) as usize;
                    pos += 4;
                    if pos + len > data.len() { return None; }
                    let s = unsafe { String::from_utf8_unchecked(data[pos..pos+len].to_vec()) };
                    pos += len;
                    null_flags[col].push(false);
                    text_cols[col].push(s);
                }
                5 | 6 => {
                    if pos + 4 > data.len() { return None; }
                    let len = u32::from_le_bytes(data[pos..pos+4].try_into().ok()?) as usize;
                    pos += 4 + len;
                    null_flags[col].push(true);
                    text_cols[col].push(String::new());
                }
                7 => {
                    if pos + 8 > data.len() { return None; }
                    let n = i64::from_le_bytes(data[pos..pos+8].try_into().ok()?);
                    pos += 8;
                    null_flags[col].push(false);
                    int_cols[col].push(n);
                }
                _ => return None,
            }
        }
    }

    let mut result_columns = Vec::with_capacity(num_columns);
    for col in 0..num_columns {
        let cv = match col_types[col] {
            1 => ColumnVector::Bool(std::mem::take(&mut bool_cols[col]), std::mem::take(&mut null_flags[col])),
            2 | 7 => ColumnVector::Int64(std::mem::take(&mut int_cols[col]), std::mem::take(&mut null_flags[col])),
            3 => ColumnVector::Float64(std::mem::take(&mut float_cols[col]), std::mem::take(&mut null_flags[col])),
            4 => ColumnVector::Text(std::mem::take(&mut text_cols[col]), std::mem::take(&mut null_flags[col])),
            _ => ColumnVector::Null(batch_size),
        };
        result_columns.push(cv);
    }

    Some(RecordBatch { columns: result_columns, num_rows: batch_size })
}
