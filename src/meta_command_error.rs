use std::error::Error;
use std::fmt;
use std::io;
use std::path::PathBuf;

use crate::table::error::TableError;
use crate::execution_error::ExecutionError;

#[derive(Debug)]
pub enum MetaCommandError {
    IoError(io::Error),
    DatabaseFileAlreadyExist(PathBuf),
    DatabaseTablesDirNotExist(PathBuf),
    TableError(TableError),
    ParseError(String),
    UnknownCommand(String),
    ExecutionError(ExecutionError),
    ConnectionPresent,
}

impl fmt::Display for MetaCommandError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            Self::IoError(io_error) => io_error.to_string(),
            Self::DatabaseTablesDirNotExist(tables_dir) =>
                format!("database file specified '{}' as a tables dir, but it does not exist",
                        tables_dir.to_str().unwrap()),
            Self::DatabaseFileAlreadyExist(tables_dir) =>
                format!("cannot create database file at '{}': file already exist",
                        tables_dir.to_str().unwrap()),
            Self::TableError(table_error) => table_error.to_string(),
            Self::ParseError(parser_error) => format!("failed to parse metacommand: {}", parser_error),
            Self::UnknownCommand(input) => format!("unknown metacommand: {}", input),
            Self::ExecutionError(exec_error) => format!("failed to execute metacommand: {}", exec_error),
            Self::ConnectionPresent => "this metacommand cannot be executed while \
                                     connected to existing database, consider running `.close`".to_string()
        };
        write!(f, "{}", message)
    }
}

impl From<io::Error> for MetaCommandError {
    fn from(error: io::Error) -> Self {
        Self::IoError(error)
    }
}

impl From<TableError> for MetaCommandError {
    fn from(error: TableError) -> Self {
        Self::TableError(error)
    }
}

impl Error for MetaCommandError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::IoError(io_error) => Some(io_error),
            _ => None,
        }
    }
}
