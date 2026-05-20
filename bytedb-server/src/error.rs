use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Auth error: {0}")]
    #[allow(dead_code)]
    Auth(String),

    #[error("Query error: {0}")]
    Query(#[from] bytedb_query::error::QueryError),

    #[error("{0}")]
    #[allow(dead_code)]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, ServerError>;
