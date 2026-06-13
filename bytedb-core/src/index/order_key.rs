use crate::tuple::value::Value;

pub fn encode_okey(values: &[&Value]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    for v in values {
        encode_one(v, &mut buf);
    }
    buf
}

pub fn encode_okey_owned(values: &[Value]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    for v in values {
        encode_one(v, &mut buf);
    }
    buf
}

fn encode_one(v: &Value, buf: &mut Vec<u8>) {
    match v {
        Value::Null => buf.push(0x00),
        other => {
            buf.push(0x01);
            encode_body(other, buf);
        }
    }
}

fn encode_body(v: &Value, buf: &mut Vec<u8>) {
    match v {
        Value::Null => {}
        Value::Bool(b) => buf.push(if *b { 1 } else { 0 }),
        Value::Int64(n) => buf.extend_from_slice(&((*n as u64) ^ (1 << 63)).to_be_bytes()),
        Value::Timestamp(n) => buf.extend_from_slice(&((*n as u64) ^ (1 << 63)).to_be_bytes()),
        Value::Interval(n) => buf.extend_from_slice(&((*n as u64) ^ (1 << 63)).to_be_bytes()),
        Value::Date(d) => buf.extend_from_slice(&((*d as u32) ^ (1 << 31)).to_be_bytes()),
        Value::Float64(f) => {
            let bits = f.to_bits();
            let ordered = if bits & (1u64 << 63) != 0 { !bits } else { bits | (1u64 << 63) };
            buf.extend_from_slice(&ordered.to_be_bytes());
        }
        Value::Decimal(m, _scale) => {
            buf.extend_from_slice(&((*m as u128) ^ (1u128 << 127)).to_be_bytes());
        }
        Value::Uuid(b) => buf.extend_from_slice(b),
        Value::Text(s) => encode_var(s.as_bytes(), buf),
        Value::Json(j) => encode_var(j.to_string().as_bytes(), buf),
        Value::Bytes(b) => encode_var(b, buf),
    }
}

fn encode_var(bytes: &[u8], buf: &mut Vec<u8>) {
    for &b in bytes {
        if b == 0x00 {
            buf.push(0x00);
            buf.push(0xFF);
        } else {
            buf.push(b);
        }
    }
    buf.push(0x00);
    buf.push(0x00);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(v: &Value) -> Vec<u8> {
        encode_okey(&[v])
    }

    fn ordered(values: &[Value]) -> bool {
        for w in values.windows(2) {
            if enc(&w[0]) >= enc(&w[1]) {
                return false;
            }
        }
        true
    }

    #[test]
    fn ints_are_order_preserving() {
        assert!(ordered(&[
            Value::Int64(i64::MIN),
            Value::Int64(-1000),
            Value::Int64(-1),
            Value::Int64(0),
            Value::Int64(1),
            Value::Int64(1000),
            Value::Int64(i64::MAX),
        ]));
    }

    #[test]
    fn floats_are_order_preserving() {
        assert!(ordered(&[
            Value::Float64(f64::MIN),
            Value::Float64(-1.5),
            Value::Float64(-0.0),
            Value::Float64(0.0),
            Value::Float64(0.5),
            Value::Float64(1e10),
            Value::Float64(f64::MAX),
        ]));
    }

    #[test]
    fn text_is_order_preserving_and_prefix_free() {
        assert!(ordered(&[
            Value::Text("".into()),
            Value::Text("a".into()),
            Value::Text("ab".into()),
            Value::Text("abc".into()),
            Value::Text("b".into()),
        ]));
        assert!(enc(&Value::Text("a".into())) < enc(&Value::Text("ab".into())));
    }

    #[test]
    fn null_sorts_first() {
        assert!(enc(&Value::Null) < enc(&Value::Int64(i64::MIN)));
        assert!(enc(&Value::Null) < enc(&Value::Text("".into())));
    }

    #[test]
    fn dates_and_decimals_order_preserving() {
        assert!(ordered(&[Value::Date(-100), Value::Date(0), Value::Date(100)]));
        assert!(ordered(&[
            Value::Decimal(-100, 2),
            Value::Decimal(-1, 2),
            Value::Decimal(0, 2),
            Value::Decimal(1, 2),
            Value::Decimal(100, 2),
        ]));
    }

    #[test]
    fn composite_keys_are_prefix_free() {
        let a = encode_okey(&[&Value::Text("a".into()), &Value::Int64(2)]);
        let b = encode_okey(&[&Value::Text("ab".into()), &Value::Int64(1)]);
        assert!(a < b);
    }
}
