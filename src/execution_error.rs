use std::error::Error;
use std::fmt;

use crate::table::ColumnType;
use crate::lexer::SqlValue;
use crate::where_clause::CmpOperator;

#[derive(Debug)]
pub enum ExecutionError {
    TableAlreadyExist(String),
    TableNotExist(String),
    ColumnNotExist { table_name: String, column_name: String },
    ColumnNthNotExist { table_name: String, column_index: usize },
    ValueColumnMismatch { value: SqlValue, column_name: String, column_type: ColumnType },
    CannotCompareWithNumber(SqlValue),
    NonEqualityComparisonWithStrings { operator: CmpOperator, lvalue: String, rvalue: String },
    OperatorNotApplicable { operator: CmpOperator, lvalue: SqlValue, rvalue: SqlValue },
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            Self::TableAlreadyExist(table_name) => format!("table '{}' already exists", table_name),
            Self::TableNotExist(table_name) => format!("table '{}' not exists", table_name),
            Self::ValueColumnMismatch { value, column_name, column_type } =>
                format!(
                    "value {} is not acceptable for column '{}' which has type '{}'",
                    value, column_name, column_type),
            Self::ColumnNotExist { table_name, column_name } =>
                format!("table '{}' does not have column '{}'", table_name, column_name),
            Self::ColumnNthNotExist { table_name, column_index } =>
                format!("table '{}' does not have column with index [{}]", table_name, column_index),
            Self::CannotCompareWithNumber(sql_value) => format!("cannot compare '{}' with number", sql_value),
            Self::OperatorNotApplicable { operator, lvalue, rvalue } =>
                format!("operator '{}' cannot be applied to values '{}' and {}",
                        operator, lvalue, rvalue),
            Self::NonEqualityComparisonWithStrings { operator, lvalue, rvalue } =>
                format!("non-equality operator '{}' cannot be applied to strings '{}' and {}, only '=' or '<>' can be used",
                        operator, lvalue, rvalue),
        };

        write!(f, "{}", message)
    }
}

impl Error for ExecutionError {}
