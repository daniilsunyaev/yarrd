use crate::lexer::SqlValue;
use crate::hash_index::SerDeError;

use std::fmt;
use std::io;

#[derive(Debug)]
pub enum HashIndexError {
    IoError(io::Error),
    FloatIndexError(SqlValue),
    SerDeError(SerDeError),
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
            Self::FloatIndexError(value) => write!(f, "float value {} cannot be hashed, only ints and strings are allowed for indexing", value),
            Self::SerDeError(serde_error) => write!(f, "{}", serde_error),
        }
    }
}
