use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Page {0} not found")]
    PageNotFound(u32),

    #[error("Buffer pool full")]
    BufferPoolFull,

    #[error("Page corrupted: checksum mismatch on page {0}")]
    ChecksumMismatch(u32),

    #[error("Key not found")]
    KeyNotFound,

    #[error("Duplicate key")]
    DuplicateKey,

    #[error("Transaction {0} aborted")]
    TransactionAborted(u64),

    #[error("Transaction {0} not found")]
    TransactionNotFound(u64),

    #[error("Deadlock detected")]
    Deadlock,

    #[error("Serialization conflict")]
    SerializationConflict,

    #[error("Write conflict: row was modified by concurrent transaction")]
    WriteConflict,

    #[error("Lock wait timeout for txn {0}")]
    LockTimeout(u64),

    #[error("Transaction {0} timed out")]
    TransactionTimeout(u64),

    #[error("WAL corrupted at LSN {0}")]
    WalCorrupted(u64),

    #[error("Table '{0}' not found")]
    TableNotFound(String),

    #[error("Table '{0}' already exists")]
    TableAlreadyExists(String),

    #[error("Column '{0}' not found")]
    ColumnNotFound(String),

    #[error("Type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },

    #[error("Overflow: value too large ({0} bytes)")]
    ValueTooLarge(usize),

    #[error("{0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, CoreError>;
