use serde::{Serialize, Deserialize};
use std::cmp::Ordering;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Bool,
    Int64,
    Float64,
    Text,
    Bytes,
    Json,
    Timestamp,
    Date,
    Decimal,
    Uuid,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Bool => write!(f, "BOOL"),
            DataType::Int64 => write!(f, "INT"),
            DataType::Float64 => write!(f, "FLOAT"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Bytes => write!(f, "BYTES"),
            DataType::Json => write!(f, "JSON"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Date => write!(f, "DATE"),
            DataType::Decimal => write!(f, "DECIMAL"),
            DataType::Uuid => write!(f, "UUID"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
    /// Microseconds since UNIX epoch.
    Timestamp(i64),
    /// Days since UNIX epoch (1970-01-01 = 0).
    Date(i32),
    /// Fixed-point decimal: value = mantissa * 10^-scale.
    Decimal(i128, u8),
    /// Stored as 16 raw bytes; rendered as canonical hyphenated form.
    Uuid([u8; 16]),
}

impl Value {
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Bool(_) => Some(DataType::Bool),
            Value::Int64(_) => Some(DataType::Int64),
            Value::Float64(_) => Some(DataType::Float64),
            Value::Text(_) => Some(DataType::Text),
            Value::Bytes(_) => Some(DataType::Bytes),
            Value::Json(_) => Some(DataType::Json),
            Value::Timestamp(_) => Some(DataType::Timestamp),
            Value::Date(_) => Some(DataType::Date),
            Value::Decimal(_, _) => Some(DataType::Decimal),
            Value::Uuid(_) => Some(DataType::Uuid),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float64(v) => Some(*v),
            Value::Int64(v) => Some(*v as f64),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    pub fn deserialize(data: &[u8]) -> Option<Self> {
        serde_json::from_slice(data).ok()
    }

    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.partial_cmp(b),
            (Value::Int64(a), Value::Float64(b)) => (*a as f64).partial_cmp(b),
            (Value::Float64(a), Value::Int64(b)) => a.partial_cmp(&(*b as f64)),
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.partial_cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
            (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.partial_cmp(b),
            (Value::Decimal(a, sa), Value::Decimal(b, sb)) => {
                let (na, nb) = decimal_align(*a, *sa, *b, *sb);
                na.partial_cmp(&nb)
            }
            (Value::Decimal(a, sa), Value::Int64(b)) => {
                let scale = 10i128.pow(*sa as u32);
                let bb = (*b as i128).checked_mul(scale)?;
                a.partial_cmp(&bb)
            }
            (Value::Int64(a), Value::Decimal(b, sb)) => {
                let scale = 10i128.pow(*sb as u32);
                let aa = (*a as i128).checked_mul(scale)?;
                aa.partial_cmp(b)
            }
            _ => None,
        }
    }

    #[inline(always)]
    pub fn cmp_fast(&self, other: &Value) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
            (Value::Int64(a), Value::Float64(b)) => {
                let af = *a as f64;
                af.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Float64(a), Value::Int64(b)) => {
                let bf = *b as f64;
                a.partial_cmp(&bf).unwrap_or(Ordering::Equal)
            }
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
            (Value::Decimal(a, sa), Value::Decimal(b, sb)) => {
                let (na, nb) = decimal_align(*a, *sa, *b, *sb);
                na.cmp(&nb)
            }
            _ => Ordering::Equal,
        }
    }
}

fn decimal_align(a: i128, sa: u8, b: i128, sb: u8) -> (i128, i128) {
    if sa == sb { return (a, b); }
    if sa < sb {
        let mul = 10i128.pow((sb - sa) as u32);
        (a.saturating_mul(mul), b)
    } else {
        let mul = 10i128.pow((sa - sb) as u32);
        (a, b.saturating_mul(mul))
    }
}

pub fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    let bytes: Vec<u8> = s.bytes().filter(|b| *b != b'-').collect();
    if bytes.len() != 32 { return None; }
    let mut out = [0u8; 16];
    for i in 0..16 {
        let hi = hex_nibble(bytes[i * 2])?;
        let lo = hex_nibble(bytes[i * 2 + 1])?;
        out[i] = (hi << 4) | lo;
    }
    Some(out)
}

pub fn format_uuid(b: &[u8; 16]) -> String {
    fn h(b: u8) -> [u8; 2] {
        const T: &[u8; 16] = b"0123456789abcdef";
        [T[(b >> 4) as usize], T[(b & 0xf) as usize]]
    }
    let mut s = String::with_capacity(36);
    for (i, byte) in b.iter().enumerate() {
        if i == 4 || i == 6 || i == 8 || i == 10 { s.push('-'); }
        let h = h(*byte);
        s.push(h[0] as char); s.push(h[1] as char);
    }
    s
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Parse "YYYY-MM-DD" → days since 1970-01-01.
pub fn parse_date(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 { return None; }
    let y: i32 = parts[0].parse().ok()?;
    let m: u32 = parts[1].parse().ok()?;
    let d: u32 = parts[2].parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) { return None; }
    Some(days_from_civil(y, m, d))
}

pub fn format_date(days: i32) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{:04}-{:02}-{:02}", y, m, d)
}

/// Howard Hinnant's date algorithm.
fn days_from_civil(y: i32, m: u32, d: u32) -> i32 {
    let y = y - if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i32 - 719468
}

fn civil_from_days(z: i32) -> (i32, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = y + if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

/// Parse "123.45" or "-12" into (mantissa, scale).
pub fn parse_decimal(s: &str) -> Option<(i128, u8)> {
    let s = s.trim();
    let (sign, body) = if let Some(rest) = s.strip_prefix('-') {
        (-1i128, rest)
    } else if let Some(rest) = s.strip_prefix('+') {
        (1, rest)
    } else { (1, s) };
    let (int_part, frac_part) = match body.split_once('.') {
        Some((i, f)) => (i, f),
        None => (body, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() { return None; }
    let mut mantissa: i128 = 0;
    for c in int_part.chars().chain(frac_part.chars()) {
        if !c.is_ascii_digit() { return None; }
        mantissa = mantissa.checked_mul(10)?.checked_add((c as u8 - b'0') as i128)?;
    }
    let scale = frac_part.len() as u8;
    Some((sign.checked_mul(mantissa)?, scale))
}

pub fn format_decimal(m: i128, scale: u8) -> String {
    if scale == 0 { return m.to_string(); }
    let neg = m < 0;
    let abs = m.unsigned_abs();
    let s = abs.to_string();
    let scale_us = scale as usize;
    let (int_part, frac_part) = if s.len() > scale_us {
        (&s[..s.len() - scale_us], &s[s.len() - scale_us..])
    } else {
        ("0", s.as_str())
    };
    let frac_padded = if s.len() < scale_us {
        format!("{:0>width$}", s, width = scale_us)
    } else {
        frac_part.to_string()
    };
    let body = format!("{}.{}", int_part, frac_padded);
    if neg { format!("-{}", body) } else { body }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Int64(v) => write!(f, "{}", v),
            Value::Float64(v) => write!(f, "{}", v),
            Value::Text(v) => write!(f, "{}", v),
            Value::Bytes(v) => write!(f, "<{} bytes>", v.len()),
            Value::Json(v) => write!(f, "{}", v),
            Value::Timestamp(v) => write!(f, "ts:{}", v),
            Value::Date(v) => write!(f, "{}", format_date(*v)),
            Value::Decimal(m, s) => write!(f, "{}", format_decimal(*m, *s)),
            Value::Uuid(b) => write!(f, "{}", format_uuid(b)),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.compare(other) == Some(Ordering::Equal)
    }
}
