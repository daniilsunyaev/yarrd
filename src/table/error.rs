use std::fmt;
use std::error::Error;

use crate::pager::PagerError;
use crate::table::ColumnType;
use crate::table::Constraint;
use crate::lexer::SqlValue;
use crate::serialize::SerDeError;
use crate::cmp_operator::CmpError;

#[derive(Debug)]
pub enum TableError {
    CreateError(PagerError),
    ColumnNotExist { table_name: String, column_name: String },
    ColumnNthNotExist { table_name: String, column_index: usize },
    CannotGetRow(PagerError),
    CannotSetCell(SerDeError),
    CannotGetCell(SerDeError),
    ValueColumnMismatch { value: SqlValue, column_name: String, column_type: ColumnType },
    CannotInsertRow(PagerError),
    CannotUpdateRow(PagerError),
    CannotDeleteRow(PagerError),
    CmpError(CmpError),
    VacuumFailed(PagerError),
    ConstraintAlreadyExists { table_name: String, column_name: String, constraint: Constraint },
    ConstraintNotExists { table_name: String, column_name: String, constraint: Constraint },
    ConstraintViolation { table_name: String, constraint: Constraint, column_name: String, value: SqlValue },
}

impl fmt::Display for TableError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::CreateError(_pager_error) => write!(f, "unable to create table: error initializing a pager"),
            Self::ColumnNotExist { table_name, column_name } =>
                write!(f, "table '{}' does not have column '{}'", table_name, column_name),
            Self::ColumnNthNotExist { table_name, column_index } =>
                write!(f, "table '{}' does not have column with index [{}]", table_name, column_index),
            Self::CannotGetRow(_pager_error) => write!(f, "cannot to get a row from pager"),
            Self::CannotSetCell(_ser_de_error) => write!(f, "cannot set row bytes for a cell"),
            Self::CannotGetCell(_ser_de_error) => write!(f, "cannot get sql value from a row cell"),
            Self::ValueColumnMismatch { value, column_name, column_type } =>
                write!(f,
                    "value {} is not acceptable for column '{}' which has type '{}'",
                    value, column_name, column_type),
            Self::CannotInsertRow(_pager_error) => write!(f, "cannot insert row into table"),
            Self::CannotUpdateRow(_pager_error) => write!(f, "cannot update row in the table"),
            Self::CannotDeleteRow(_pager_error) => write!(f, "cannot delete row in the table"),
            Self::CmpError(cmp_error) => write!(f, "{}", cmp_error),
            Self::VacuumFailed(_pager_error) => write!(f, "failed to vaccum table"),
            Self::ConstraintAlreadyExists { table_name, column_name, constraint } =>
                write!(f, "table's '{}' column '{}' already has constraint '{}'", table_name, column_name, constraint),
            Self::ConstraintNotExists { table_name, column_name, constraint } =>
                write!(f, "table's '{}' column '{}' does not have constraint '{}'", table_name, column_name, constraint),
            Self::ConstraintViolation { table_name, constraint, column_name, value } =>
                write!(f,
                    "value {} violates '{}' constraint on column '{}' from table '{}'",
                    value, constraint, column_name, table_name),
        }
    }
}

impl Error for TableError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CmpError(cmp_error) => Some(cmp_error),
            Self::VacuumFailed(vacuum_error) => Some(vacuum_error),
            _ => None,
        }
    }
}
