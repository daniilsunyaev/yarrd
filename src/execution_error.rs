use std::error::Error;
use std::fmt;
use std::io;

use crate::serialize::SerDeError;
use crate::pager::PagerError;
use crate::table::error::TableError;

#[derive(Debug)]
pub enum ExecutionError {
    TableAlreadyExist(String),
    TableNotExist(String),
    SerDeError(SerDeError),
    PagerError(PagerError),
    IoError(io::Error),
    TableError(TableError),
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            Self::TableAlreadyExist(table_name) => format!("table '{}' already exists", table_name),
            Self::TableNotExist(table_name) => format!("table '{}' not exists", table_name),
            Self::SerDeError(ser_de_error) => ser_de_error.to_string(),
            Self::PagerError(pager_error) => pager_error.to_string(),
            Self::IoError(io_error) => io_error.to_string(),
            Self::TableError(table_error) => table_error.to_string(),
        };

        write!(f, "{}", message)
    }
}

impl From<SerDeError> for ExecutionError {
    fn from(error: SerDeError) -> Self {
        Self::SerDeError(error)
    }
}

impl From<PagerError> for ExecutionError {
    fn from(error: PagerError) -> Self {
        Self::PagerError(error)
    }
}

impl From<TableError> for ExecutionError {
    fn from(error: TableError) -> Self {
        Self::TableError(error)
    }
}

impl From<io::Error> for ExecutionError {
    fn from(error: io::Error) -> Self {
        Self::IoError(error)
    }
}

impl Error for ExecutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SerDeError(ser_de_error) => Some(ser_de_error),
            _ => None,
        }
    }
}
