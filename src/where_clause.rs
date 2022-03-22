use std::fmt;

use crate::lexer::SqlValue;
use crate::execution_error::ExecutionError;
use crate::row::Row;
use crate::table::ColumnType;

#[derive(Debug)]
pub enum WhereValue {
    TableColumn(usize),
    Value(SqlValue),
}

#[derive(Debug)]
pub struct WhereFilter<'a> {
    operator: CmpOperator,
    left: WhereValue,
    right: WhereValue,
    column_types: &'a [ColumnType],
}

impl<'a> WhereFilter<'a> {
    pub fn dummy() -> Self {
        Self {
            operator: CmpOperator::Equals,
            left: WhereValue::Value(SqlValue::Integer(1)),
            right: WhereValue::Value(SqlValue::Integer(1)),
            column_types: &[],
        }
    }

    pub fn matches(&'a self, row: &'a Row) -> Result<bool, ExecutionError> {
        self.operator.apply(&self.get_value(&self.left, row)?, &self.get_value(&self.right, row)?)
    }

    fn get_value(&'a self, value: &'a WhereValue, row: &'a Row) -> Result<SqlValue, ExecutionError> {
        match value {
            WhereValue::Value(sql_value) => Ok(sql_value.clone()),
            WhereValue::TableColumn(index) => row.get_cell_sql_value(self.column_types, *index),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CmpOperator {
    Less,
    Greater,
    Equals,
    NotEquals,
    LessEquals,
    GreaterEquals,
}

impl<'a> fmt::Display for CmpOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Less => write!(f, "<"),
            Self::Greater => write!(f, ">"),
            Self::Equals => write!(f, "="),
            Self::NotEquals => write!(f, "<>"),
            Self::LessEquals => write!(f, "<="),
            Self::GreaterEquals => write!(f, ">="),
        }
    }
}

impl CmpOperator {
    pub fn apply(&self, left: &SqlValue, right: &SqlValue) -> Result<bool, ExecutionError> {
        match left {
            SqlValue::Integer(lvalue) => {
                match right {
                    SqlValue::Integer(rvalue) => Ok(self.cmp_ord(lvalue, rvalue)),
                    SqlValue::Null => Ok(false),
                    _ => Err(ExecutionError::CannotCompareWithNumber(right.clone())),
                }

            },
            SqlValue::String(ref lvalue) | SqlValue::Identificator(ref lvalue) => {
                match self {
                    Self::Equals | Self::NotEquals => {
                        match right {
                            SqlValue::Integer(_rvalue) =>  Err(ExecutionError::CannotCompareWithNumber(left.clone())),
                            SqlValue::String(ref rvalue) | SqlValue::Identificator(ref rvalue) => self.cmp_eq(lvalue, rvalue),
                            SqlValue::Null => Ok(false),
                        }
                    },
                    _ => Err(ExecutionError::OperatorNotApplicable { operator: *self, lvalue: left.clone(), rvalue: right.clone() })
                }
            },
            SqlValue::Null => Ok(false)
        }
    }

    fn cmp_eq(&self, left: &str, right: &str) -> Result<bool, ExecutionError> {
        match self {
            Self::Equals => Ok(left == right),
            Self::NotEquals => Ok(left != right),
            _ => Err(ExecutionError::NonEqualityComparisonWithStrings { operator: *self, lvalue: left.to_string(), rvalue: right.to_string() })
        }
    }

    fn cmp_ord<Number>(&self, left: Number, right: Number) -> bool
    where
        Number: PartialOrd
    {
        match self {
            Self::Less => left < right,
            Self::Greater => left > right,
            Self::Equals => left == right,
            Self::NotEquals => left != right,
            Self::LessEquals => left <= right,
            Self::GreaterEquals => left >= right,
        }
    }
}

#[derive(Debug)]
pub struct WhereClause {
    pub left_value: SqlValue,
    pub right_value: SqlValue,
    pub operator: CmpOperator,
}

impl WhereClause {
    pub fn compile<'a>(self, column_types: &'a [ColumnType], table_name: &'a str, column_names: &'a [String]) -> WhereFilter<'a> {
        let left = Self::build_where_value(self.left_value, table_name, column_names);
        let right = Self::build_where_value(self.right_value, table_name, column_names);

        WhereFilter {
            operator: self.operator,
            left,
            right,
            column_types,
        }
    }

    pub fn build_where_value(value: SqlValue, table_name: &str, column_names: &[String]) -> WhereValue {
        let string_value = value.to_string();
        let splitted_identificator: Vec<&str> = string_value.split('.').collect();
        match splitted_identificator.len() {
            1 => {
                let index = column_names.iter()
                    .position(|table_column_name| table_column_name.eq(&string_value));
                match index {
                    None => WhereValue::Value(value),
                    Some(i) => WhereValue::TableColumn(i),
                }
            }
            2 => {
                if !splitted_identificator[0].eq(table_name) {
                    WhereValue::Value(value)
                } else {
                    let index = column_names.iter()
                        .position(|table_column_name| table_column_name.eq(splitted_identificator[1]));
                    match index {
                        None => WhereValue::Value(value),
                        Some(i) => WhereValue::TableColumn(i),
                    }
                }
            },
            _ => WhereValue::Value(value),
        }
    }
}
