use serde::{Serialize, Deserialize};
use crate::tuple::schema::Schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub name: String,
    pub schema: Schema,
    pub table_id: u32,
    pub index_ids: Vec<u32>,
}

impl TableMeta {
    pub fn new(name: impl Into<String>, schema: Schema, table_id: u32) -> Self {
        TableMeta {
            name: name.into(),
            schema,
            table_id,
            index_ids: Vec::new(),
        }
    }

    pub fn add_index(&mut self, index_id: u32) {
        self.index_ids.push(index_id);
    }
}
