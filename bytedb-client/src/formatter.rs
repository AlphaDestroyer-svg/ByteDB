use bytedb_core::tuple::value::Value;
use crate::protocol::Response;

pub fn format_response(response: &Response) -> String {
    match response {
        Response::ResultSet { columns, rows } => {
            if rows.is_empty() {
                return "(0 rows)".to_string();
            }

            let mut col_widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
            for row in rows {
                for (i, val) in row.iter().enumerate() {
                    if i < col_widths.len() {
                        let val_str = format_value(val);
                        col_widths[i] = col_widths[i].max(val_str.len());
                    }
                }
            }

            let mut output = String::new();

            // Top border
            let border: Vec<String> = col_widths.iter()
                .map(|&w| "─".repeat(w + 2))
                .collect();
            output.push('┌');
            output.push_str(&border.join("┬"));
            output.push('┐');
            output.push('\n');

            // Header
            let header: Vec<String> = columns.iter().enumerate()
                .map(|(i, c)| format!(" {:width$} ", c, width = col_widths[i]))
                .collect();
            output.push('│');
            output.push_str(&header.join("│"));
            output.push('│');
            output.push('\n');

            // Header separator
            let sep: Vec<String> = col_widths.iter()
                .map(|&w| "─".repeat(w + 2))
                .collect();
            output.push('├');
            output.push_str(&sep.join("┼"));
            output.push('┤');
            output.push('\n');

            // Rows
            for row in rows {
                let row_str: Vec<String> = row.iter().enumerate()
                    .map(|(i, val)| {
                        let s = format_value(val);
                        let width = col_widths.get(i).copied().unwrap_or(s.len());
                        format!(" {:width$} ", s, width = width)
                    })
                    .collect();
                output.push('│');
                output.push_str(&row_str.join("│"));
                output.push('│');
                output.push('\n');
            }

            // Bottom border
            let bottom: Vec<String> = col_widths.iter()
                .map(|&w| "─".repeat(w + 2))
                .collect();
            output.push('└');
            output.push_str(&bottom.join("┴"));
            output.push('┘');
            output.push('\n');

            output.push_str(&format!("({} rows)", rows.len()));
            output
        }
        Response::Modified { count } => {
            format!("Modified {} row(s)", count)
        }
        Response::Ok { message } => message.clone(),
        Response::Error { code, message } => {
            format!("ERROR [{}]: {}", code, message)
        }
        Response::Pong => "PONG".to_string(),
        Response::AuthOk { session_id } => format!("Authenticated (session {})", session_id),
        Response::AuthFail { reason } => format!("Auth failed: {}", reason),
    }
}

fn format_value(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => format!("{:.6}", f),
        Value::Text(s) => s.clone(),
        Value::Bytes(b) => format!("<{} bytes>", b.len()),
        Value::Json(j) => j.to_string(),
        Value::Timestamp(t) => format!("ts:{}", t),
        Value::Date(d) => bytedb_core::tuple::value::format_date(*d),
        Value::Decimal(m, s) => bytedb_core::tuple::value::format_decimal(*m, *s),
        Value::Uuid(b) => bytedb_core::tuple::value::format_uuid(b),
    }
}
