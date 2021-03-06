use std::error::Error;
use std::fmt;
use std::io;
use std::path::PathBuf;

use crate::table::error::TableError;

#[derive(Debug)]
pub enum MetaCommandError {
    IoError(io::Error),
    DatabaseTablesDirNotExist(PathBuf),
    SchemaDefinitionMissing,
    SchemaDefinitionInvalid { table_name: String, expected: &'static str, actual: String },
    TableError(TableError),
}

impl fmt::Display for MetaCommandError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            Self::IoError(io_error) => io_error.to_string(),
            Self::DatabaseTablesDirNotExist(tables_dir) =>
                format!("database file specified '{}' as a tables dir, but it does not exist",
                        tables_dir.to_str().unwrap()),
            Self::SchemaDefinitionMissing => "no schema definition found".to_string(),
            Self::SchemaDefinitionInvalid { table_name, expected, actual } =>
                format!("failed to parse schema definition for table '{}', expected {}, got '{}'",
                        table_name, expected, actual),
            Self::TableError(table_error) => table_error.to_string(),
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
