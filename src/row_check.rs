use std::fmt;

use crate::lexer::SqlValue;
use crate::table::error::TableError;
use crate::row::Row;
use crate::table::ColumnType;
use crate::cmp_operator::CmpOperator;

#[derive(Debug, Clone, PartialEq)]
pub enum RowCheckValue {
    TableColumn(usize),
    Static(SqlValue),
}

impl fmt::Display for RowCheckValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::TableColumn(index) => write!(f, "column {}", index),
            Self::Static(sql_value) => write!(f, "{}", sql_value),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowCheck {
    pub operator: CmpOperator,
    pub left: RowCheckValue,
    pub right: RowCheckValue,
}

impl fmt::Display for RowCheck {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.operator, self.right)
    }
}

impl RowCheck {
    pub fn dummy() -> Self {
        Self {
            operator: CmpOperator::Equals,
            left: RowCheckValue::Static(SqlValue::Integer(1)),
            right: RowCheckValue::Static(SqlValue::Integer(1)),
        }
    }

    pub fn matches(&self, row: &Row, column_types: &[ColumnType]) -> Result<bool, TableError> {
        self
            .operator
            .apply(&self.get_value(&self.left, row, column_types)?, &self.get_value(&self.right, row, column_types)?)
            .map_err(TableError::CmpError)

    }

    pub fn is_column_value_eq_static_check(&self) -> Option<(usize, SqlValue)> {
        if self.operator == CmpOperator::Equals {
            match &self.left {
                RowCheckValue::TableColumn(column_number) => {
                    if let RowCheckValue::Static(sql_value) = &self.right {
                        return Some((*column_number, sql_value.clone()))
                    }
                },
                RowCheckValue::Static(sql_value) => {
                    if let RowCheckValue::TableColumn(column_number) = self.right {
                        return Some((column_number, sql_value.clone()))
                    }
                },
            }
        }

        None
    }

    fn get_value(&self, value: &RowCheckValue, row: &Row, column_types: &[ColumnType]) -> Result<SqlValue, TableError> {
        match value {
            RowCheckValue::Static(sql_value) => Ok(sql_value.clone()),
            RowCheckValue::TableColumn(index) =>
                row.get_cell_sql_value(column_types, *index).map_err(TableError::CannotGetCell),
        }
    }
}

