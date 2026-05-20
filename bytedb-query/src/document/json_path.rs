use serde_json::Value as JsonValue;

pub fn evaluate_path(doc: &JsonValue, path: &str) -> Option<JsonValue> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = doc;

    for part in parts {
        if let Some(idx) = part.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            if let Ok(i) = idx.parse::<usize>() {
                current = current.get(i)?;
            } else {
                return None;
            }
        } else {
            current = current.get(part)?;
        }
    }

    Some(current.clone())
}

pub fn set_path(doc: &mut JsonValue, path: &str, value: JsonValue) -> bool {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = doc;

    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            if let Some(obj) = current.as_object_mut() {
                obj.insert(part.to_string(), value);
                return true;
            }
            return false;
        }

        if current.get(part).is_none() {
            if let Some(obj) = current.as_object_mut() {
                obj.insert(part.to_string(), JsonValue::Object(serde_json::Map::new()));
            }
        }
        match current.get_mut(part) {
            Some(next) => current = next,
            None => return false,
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_evaluate_path() {
        let doc = json!({"name": "Alice", "address": {"city": "NYC"}});
        assert_eq!(evaluate_path(&doc, "name"), Some(json!("Alice")));
        assert_eq!(evaluate_path(&doc, "address.city"), Some(json!("NYC")));
        assert_eq!(evaluate_path(&doc, "missing"), None);
    }
}
