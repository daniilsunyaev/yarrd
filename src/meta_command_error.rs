use std::error::Error;
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum MetaCommandError {
    IoError(io::Error),
    TableRowSizeDoesNotMatchSource(usize, u64),
    DatabaseTablesDirNotExist(String),
    SchemaDefinitionMissing,
    SchemaDefinitionInvalid { table_name: String, expected: &'static str, actual: String },
}

impl fmt::Display for MetaCommandError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            Self::IoError(io_error) => io_error.to_string(),
            Self::TableRowSizeDoesNotMatchSource(table_row_size, source_size) =>
                format!("cannot read rows of size '{}' from source of size '{}', \
                        since it does not contain whole number of rows",
                        table_row_size, source_size),
            Self::DatabaseTablesDirNotExist(tables_dir) =>
                format!("database file specified '{}' as a tables dir, but it does not exist",
                        tables_dir),
            Self::SchemaDefinitionMissing => "no schema definition found".to_string(),
            Self::SchemaDefinitionInvalid { table_name, expected, actual } =>
                format!("failed to parse schema definition for table '{}', expected {}, got '{}'",
                        table_name, expected, actual),

        };
        write!(f, "{}", message)
    }
}

impl From<io::Error> for MetaCommandError {
    fn from(error: io::Error) -> Self {
        Self::IoError(error)
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
