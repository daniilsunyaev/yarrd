use crate::lexer::SqlValue;
use crate::hash_index::SerDeError;

use std::fmt;
use std::io;

#[derive(Debug)]
pub enum HashIndexError {
    IoError(io::Error),
    FloatIndexError(String),
    SerDeError(SerDeError),
    BucketIsFull,
    UnexpectedBucketNumber(u64),
    RowAlreadyExists(SqlValue, u64),
    RowDoesNotExists(u64),
}

impl From<io::Error> for HashIndexError {
    fn from(error: io::Error) -> Self {
        Self::IoError(error)
    }
}

impl From<SerDeError> for HashIndexError {
    fn from(error: SerDeError) -> Self {
        Self::SerDeError(error)
    }
}

impl fmt::Display for HashIndexError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(io_error) => write!(f, "io error: {}", io_error),
            Self::FloatIndexError(column_name) => write!(f, "float column '{}' cannot be hashed, only ints and strings are allowed for indexing", column_name),
            Self::SerDeError(serde_error) => write!(f, "{}", serde_error),
            Self::BucketIsFull => write!(f, "bucket is full, need to reindex"),
            Self::UnexpectedBucketNumber(number) => write!(f, "bucket {} does not exist, and cannot be a new overflow bucket", number),
            Self::RowAlreadyExists(value, row_id) => write!(f, "attempted to insert value '{}' with row_id '{}' but is already present in the index", value, row_id),
            Self::RowDoesNotExists(hash_row_id) => write!(f, "attempted to find hash row value '{}' but this row does not present in index", hash_row_id),
        }
    }
}
