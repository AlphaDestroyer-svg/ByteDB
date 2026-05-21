use serde::{Serialize, Deserialize};
use super::value::Value;
use super::schema::Schema;
use crate::error::{CoreError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tuple {
    pub values: Vec<Value>,
}

impl Tuple {
    pub fn new(values: Vec<Value>) -> Self {
        Tuple { values }
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn set(&mut self, index: usize, value: Value) {
        if index < self.values.len() {
            self.values[index] = value;
        }
    }

    pub fn num_fields(&self) -> usize {
        self.values.len()
    }

    pub fn validate(&self, schema: &Schema) -> Result<()> {
        if self.values.len() != schema.columns.len() {
            return Err(CoreError::TypeMismatch {
                expected: format!("{} columns", schema.columns.len()),
                got: format!("{} values", self.values.len()),
            });
        }

        for (value, col) in self.values.iter().zip(schema.columns.iter()) {
            if value.is_null() {
                if !col.nullable {
                    return Err(CoreError::TypeMismatch {
                        expected: format!("non-null {}", col.data_type),
                        got: "NULL".into(),
                    });
                }
                continue;
            }

            if let Some(vtype) = value.data_type() {
                if vtype != col.data_type {
                    return Err(CoreError::TypeMismatch {
                        expected: col.data_type.to_string(),
                        got: vtype.to_string(),
                    });
                }
            }
        }

        Ok(())
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        buf.push(self.values.len() as u8);
        for val in &self.values {
            encode_value(val, &mut buf);
        }
        buf
    }

    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let mut pos = 0;
        let count = data[pos] as usize;
        pos += 1;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let (val, new_pos) = decode_value(data, pos)?;
            values.push(val);
            pos = new_pos;
        }
        Some(Tuple { values })
    }

    pub fn deserialize_columns(data: &[u8], needed: &[usize]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let mut pos = 0;
        let count = data[pos] as usize;
        pos += 1;
        let mut values = vec![Value::Null; count];
        let mut needed_ptr = 0;

        for col in 0..count {
            if needed_ptr < needed.len() && needed[needed_ptr] == col {
                let (val, new_pos) = decode_value(data, pos)?;
                values[col] = val;
                pos = new_pos;
                needed_ptr += 1;
            } else {
                pos = skip_value(data, pos)?;
            }
        }
        Some(Tuple { values })
    }

    pub fn key_bytes(&self, key_columns: &[usize]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        for &i in key_columns {
            if let Some(val) = self.values.get(i) {
                encode_value(val, &mut buf);
            }
        }
        buf
    }
}

const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT64: u8 = 2;
const TAG_FLOAT64: u8 = 3;
const TAG_TEXT: u8 = 4;
const TAG_BYTES: u8 = 5;
const TAG_JSON: u8 = 6;
const TAG_TIMESTAMP: u8 = 7;
const TAG_DATE: u8 = 8;
const TAG_DECIMAL: u8 = 9;
const TAG_UUID: u8 = 10;

fn encode_value(val: &Value, buf: &mut Vec<u8>) {
    match val {
        Value::Null => buf.push(TAG_NULL),
        Value::Bool(b) => {
            buf.push(TAG_BOOL);
            buf.push(*b as u8);
        }
        Value::Int64(n) => {
            buf.push(TAG_INT64);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Float64(f) => {
            buf.push(TAG_FLOAT64);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(TAG_TEXT);
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        Value::Bytes(b) => {
            buf.push(TAG_BYTES);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Json(j) => {
            buf.push(TAG_JSON);
            let s = j.to_string();
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        Value::Timestamp(t) => {
            buf.push(TAG_TIMESTAMP);
            buf.extend_from_slice(&t.to_le_bytes());
        }
        Value::Date(d) => {
            buf.push(TAG_DATE);
            buf.extend_from_slice(&d.to_le_bytes());
        }
        Value::Decimal(m, s) => {
            buf.push(TAG_DECIMAL);
            buf.extend_from_slice(&m.to_le_bytes());
            buf.push(*s);
        }
        Value::Uuid(b) => {
            buf.push(TAG_UUID);
            buf.extend_from_slice(b);
        }
    }
}

fn decode_value(data: &[u8], pos: usize) -> Option<(Value, usize)> {
    if pos >= data.len() {
        return None;
    }
    let tag = data[pos];
    let mut p = pos + 1;
    match tag {
        TAG_NULL => Some((Value::Null, p)),
        TAG_BOOL => {
            if p >= data.len() { return None; }
            let v = data[p] != 0;
            Some((Value::Bool(v), p + 1))
        }
        TAG_INT64 => {
            if p + 8 > data.len() { return None; }
            let n = i64::from_le_bytes(data[p..p+8].try_into().ok()?);
            Some((Value::Int64(n), p + 8))
        }
        TAG_FLOAT64 => {
            if p + 8 > data.len() { return None; }
            let f = f64::from_le_bytes(data[p..p+8].try_into().ok()?);
            Some((Value::Float64(f), p + 8))
        }
        TAG_TEXT => {
            if p + 4 > data.len() { return None; }
            let len = u32::from_le_bytes(data[p..p+4].try_into().ok()?) as usize;
            p += 4;
            if p + len > data.len() { return None; }
            let s = unsafe { String::from_utf8_unchecked(data[p..p+len].to_vec()) };
            Some((Value::Text(s), p + len))
        }
        TAG_BYTES => {
            if p + 4 > data.len() { return None; }
            let len = u32::from_le_bytes(data[p..p+4].try_into().ok()?) as usize;
            p += 4;
            if p + len > data.len() { return None; }
            let b = data[p..p+len].to_vec();
            Some((Value::Bytes(b), p + len))
        }
        TAG_JSON => {
            if p + 4 > data.len() { return None; }
            let len = u32::from_le_bytes(data[p..p+4].try_into().ok()?) as usize;
            p += 4;
            if p + len > data.len() { return None; }
            let s = std::str::from_utf8(&data[p..p+len]).ok()?;
            let j: serde_json::Value = serde_json::from_str(s).ok()?;
            Some((Value::Json(j), p + len))
        }
        TAG_TIMESTAMP => {
            if p + 8 > data.len() { return None; }
            let t = i64::from_le_bytes(data[p..p+8].try_into().ok()?);
            Some((Value::Timestamp(t), p + 8))
        }
        TAG_DATE => {
            if p + 4 > data.len() { return None; }
            let d = i32::from_le_bytes(data[p..p+4].try_into().ok()?);
            Some((Value::Date(d), p + 4))
        }
        TAG_DECIMAL => {
            if p + 17 > data.len() { return None; }
            let m = i128::from_le_bytes(data[p..p+16].try_into().ok()?);
            let s = data[p+16];
            Some((Value::Decimal(m, s), p + 17))
        }
        TAG_UUID => {
            if p + 16 > data.len() { return None; }
            let mut b = [0u8; 16];
            b.copy_from_slice(&data[p..p+16]);
            Some((Value::Uuid(b), p + 16))
        }
        _ => None,
    }
}

fn skip_value(data: &[u8], pos: usize) -> Option<usize> {
    if pos >= data.len() {
        return None;
    }
    let tag = data[pos];
    let p = pos + 1;
    match tag {
        TAG_NULL => Some(p),
        TAG_BOOL => Some(p + 1),
        TAG_INT64 | TAG_FLOAT64 | TAG_TIMESTAMP => Some(p + 8),
        TAG_DATE => Some(p + 4),
        TAG_UUID => Some(p + 16),
        TAG_DECIMAL => Some(p + 17),
        TAG_TEXT | TAG_BYTES | TAG_JSON => {
            if p + 4 > data.len() { return None; }
            let len = u32::from_le_bytes(data[p..p+4].try_into().ok()?) as usize;
            Some(p + 4 + len)
        }
        _ => None,
    }
}

use std::cmp::Ordering;

#[inline(always)]
pub fn column_offset(data: &[u8], col_idx: usize) -> Option<usize> {
    if data.is_empty() { return None; }
    let count = data[0] as usize;
    if col_idx >= count { return None; }
    let mut pos = 1;
    for _ in 0..col_idx {
        pos = skip_value(data, pos)?;
    }
    Some(pos)
}

#[inline(always)]
pub fn read_int64_at(data: &[u8], col_idx: usize) -> Option<i64> {
    let pos = column_offset(data, col_idx)?;
    if pos >= data.len() { return None; }
    let tag = data[pos];
    if tag == TAG_INT64 || tag == TAG_TIMESTAMP {
        if pos + 9 > data.len() { return None; }
        return Some(i64::from_le_bytes(data[pos+1..pos+9].try_into().ok()?));
    }
    None
}

#[inline(always)]
pub fn read_value_at(data: &[u8], col_idx: usize) -> Option<Value> {
    let pos = column_offset(data, col_idx)?;
    let (val, _) = decode_value(data, pos)?;
    Some(val)
}

#[inline(always)]
pub fn compare_column_int64(data: &[u8], col_idx: usize, literal: i64) -> Option<Ordering> {
    let pos = column_offset(data, col_idx)?;
    if pos >= data.len() { return None; }
    let tag = data[pos];
    if tag == TAG_NULL { return None; }
    if tag == TAG_INT64 || tag == TAG_TIMESTAMP {
        if pos + 9 > data.len() { return None; }
        let v = i64::from_le_bytes(data[pos+1..pos+9].try_into().ok()?);
        return Some(v.cmp(&literal));
    }
    None
}

#[inline(always)]
pub fn compare_column_float64(data: &[u8], col_idx: usize, literal: f64) -> Option<Ordering> {
    let pos = column_offset(data, col_idx)?;
    if pos >= data.len() { return None; }
    let tag = data[pos];
    if tag == TAG_NULL { return None; }
    if tag == TAG_FLOAT64 {
        if pos + 9 > data.len() { return None; }
        let v = f64::from_le_bytes(data[pos+1..pos+9].try_into().ok()?);
        return v.partial_cmp(&literal);
    }
    if tag == TAG_INT64 {
        if pos + 9 > data.len() { return None; }
        let v = i64::from_le_bytes(data[pos+1..pos+9].try_into().ok()?);
        return (v as f64).partial_cmp(&literal);
    }
    None
}

#[inline(always)]
pub fn compare_column_text<'a>(data: &'a [u8], col_idx: usize, literal: &str) -> Option<Ordering> {
    let pos = column_offset(data, col_idx)?;
    if pos >= data.len() { return None; }
    let tag = data[pos];
    if tag == TAG_NULL { return None; }
    if tag == TAG_TEXT {
        let p = pos + 1;
        if p + 4 > data.len() { return None; }
        let len = u32::from_le_bytes(data[p..p+4].try_into().ok()?) as usize;
        let start = p + 4;
        if start + len > data.len() { return None; }
        let s = std::str::from_utf8(&data[start..start+len]).ok()?;
        return Some(s.cmp(literal));
    }
    None
}

#[inline(always)]
pub fn raw_filter_matches(data: &[u8], col_idx: usize, op: u8, literal: &Value) -> Option<bool> {
    let ord = match literal {
        Value::Int64(v) => compare_column_int64(data, col_idx, *v)?,
        Value::Float64(v) => compare_column_float64(data, col_idx, *v)?,
        Value::Text(v) => compare_column_text(data, col_idx, v)?,
        _ => return None,
    };
    let matches = match op {
        0 => ord == Ordering::Equal,
        1 => ord != Ordering::Equal,
        2 => ord == Ordering::Less,
        3 => ord == Ordering::Greater,
        4 => ord != Ordering::Greater,
        5 => ord != Ordering::Less,
        _ => true,
    };
    Some(matches)
}
