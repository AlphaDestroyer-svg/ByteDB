use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use serde::{Serialize, Deserialize};
use super::value::{DataType, Value};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub auto_increment: bool,
    pub default: Option<Value>,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Column {
            name: name.into(),
            data_type,
            nullable: true,
            primary_key: false,
            unique: false,
            auto_increment: false,
            default: None,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false;
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    pub fn auto_increment(mut self) -> Self {
        self.auto_increment = true;
        self
    }

    pub fn with_default(mut self, value: Value) -> Self {
        self.default = Some(value);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum FkAction {
    #[default]
    Restrict,
    Cascade,
    SetNull,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKey {
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    #[serde(default)]
    pub on_delete: FkAction,
    #[serde(default)]
    pub on_update: FkAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<Column>,
    pub table_name: String,
    #[serde(default)]
    pub check_constraints: Vec<String>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKey>,
    #[serde(skip)]
    column_map: HashMap<String, usize>,
}

impl Schema {
    pub fn new(table_name: impl Into<String>, columns: Vec<Column>) -> Self {
        let column_map: HashMap<String, usize> = columns.iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        Schema {
            columns,
            table_name: table_name.into(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            column_map,
        }
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.column_map.get(name).copied()
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.column_map.get(name).map(|&i| &self.columns[i])
    }

    pub fn primary_key_columns(&self) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect()
    }

    pub fn unique_columns(&self) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.unique && !c.primary_key)
            .map(|(i, _)| i)
            .collect()
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

#[derive(Debug)]
pub struct SequenceGenerator {
    pub counter: AtomicI64,
}

impl SequenceGenerator {
    pub fn new(start: i64) -> Self {
        SequenceGenerator { counter: AtomicI64::new(start) }
    }

    pub fn next(&self) -> i64 {
        self.counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn observe(&self, value: i64) {
        let mut current = self.counter.load(std::sync::atomic::Ordering::SeqCst);
        while value >= current {
            match self.counter.compare_exchange(current, value + 1, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
}
