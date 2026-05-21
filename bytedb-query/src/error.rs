use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Plan error: {0}")]
    Plan(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Core error: {0}")]
    Core(#[from] bytedb_core::error::CoreError),

    #[error("Unsupported operation: {0}")]
    Unsupported(String),

    #[error("Invalid expression: {0}")]
    InvalidExpression(String),

    #[error("Type error: {0}")]
    TypeError(String),

    #[error("Constraint violation: {0}")]
    Constraint(String),

    #[error("Table '{0}' not found")]
    TableNotFound(String),

    #[error("Column '{0}' not found")]
    ColumnNotFound(String),

    #[error("Query cancelled")]
    Cancelled,

    #[error("Query timed out after {0} ms")]
    QueryTimeout(u64),

    #[error("Resource limit exceeded: {0}")]
    ResourceLimit(String),
}

impl QueryError {
    pub fn unique_violation(col: &str) -> Self {
        QueryError::Constraint(format!("UNIQUE constraint violated for column '{}'", col))
    }
    pub fn not_null_violation(col: &str) -> Self {
        QueryError::Constraint(format!("NOT NULL constraint violated for column '{}'", col))
    }
    pub fn check_violation(table: &str) -> Self {
        QueryError::Constraint(format!("CHECK constraint failed on table '{}'", table))
    }
    pub fn fk_violation(parent: &str) -> Self {
        QueryError::Constraint(format!("FOREIGN KEY violation: no matching row in '{}'", parent))
    }
    pub fn fk_referenced(child: &str) -> Self {
        QueryError::Constraint(format!("FOREIGN KEY violation: row in '{}' references this row", child))
    }
}

pub type Result<T> = std::result::Result<T, QueryError>;
