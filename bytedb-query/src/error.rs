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
}

pub type Result<T> = std::result::Result<T, QueryError>;
