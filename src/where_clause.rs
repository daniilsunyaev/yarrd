use std::fmt;

use crate::lexer::SqlValue;
use crate::table::Table;
use crate::execution_error::ExecutionError;
use crate::row::Row;

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
    pub fn apply(&self, left: SqlValue, right: SqlValue) -> Result<bool, ExecutionError> {
        match left {
            SqlValue::Integer(lvalue) => {
                match right {
                    SqlValue::Integer(rvalue) => Ok(self.cmp_ord(lvalue, rvalue)),
                    SqlValue::Null => Ok(false),
                    _ => Err(ExecutionError::CannotCompareWithNumber(right)),
                }

            },
            SqlValue::String(ref lvalue) | SqlValue::Identificator(ref lvalue) => {
                match self {
                    Self::Equals | Self::NotEquals => {
                        match right {
                            SqlValue::Integer(_rvalue) =>  Err(ExecutionError::CannotCompareWithNumber(left)),
                            SqlValue::String(ref rvalue) | SqlValue::Identificator(ref rvalue) => self.cmp_eq(lvalue, rvalue),
                            SqlValue::Null => Ok(false),
                        }
                    },
                    _ => Err(ExecutionError::OperatorNotApplicable { operator: *self, lvalue: left, rvalue: right })
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
    pub fn build_filter<'a>(&'a self, table: &'a Table) -> Box<dyn Fn(&'a Row) -> Result<bool, ExecutionError> + 'a> {
        let get_left_value = self.build_value_getter(table, &self.left_value);
        let get_right_value = self.build_value_getter(table, &self.right_value);

        Box::new(move |row: &'a Row| {
            let left = get_left_value(row)?;
            let right = get_right_value(row)?;
            self.operator.apply(left, right)
        })
    }

    fn build_value_getter<'a>(&'a self, table: &'a Table, value: &'a SqlValue) -> Box<dyn Fn(&'a Row) -> Result<SqlValue, ExecutionError> + 'a> {
        let dummy_getter = |_row| Ok(value.clone());
        let table_name = table.name.as_str();
        let string_value = value.to_string();
        let column_name = {
            let splitted_identificator: Vec<&str> = string_value.split('.').collect();
            match splitted_identificator.len() {
                1 => string_value.as_str(),
                2 => {
                    if !splitted_identificator[0].eq(table_name) {
                        return Box::new(dummy_getter);
                    } else {
                        splitted_identificator[1]
                    }
                },
                _ => return Box::new(dummy_getter),
            }
        };

        if let Some(column_index) = table.column_index(column_name) {
           Box::new(move |row: &'a Row| row.get_cell_sql_value(&table.column_types, column_index))
        } else {
           Box::new(dummy_getter)
        }
    }
}
