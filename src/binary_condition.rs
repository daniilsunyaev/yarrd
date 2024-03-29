use std::fmt;

use crate::lexer::SqlValue;
use crate::cmp_operator::CmpOperator;
use crate::row_check::{RowCheck, RowCheckValue};
use crate::table::error::TableError;

#[derive(Debug, PartialEq, Clone)]
pub struct BinaryCondition {
    pub left_value: SqlValue,
    pub right_value: SqlValue,
    pub operator: CmpOperator,
}

impl fmt::Display for BinaryCondition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {} {}", self.left_value, self.operator, self.right_value)
    }
}

impl BinaryCondition {
    pub fn compile(self, table_name: &str, column_names: &[String]) -> Result<RowCheck, TableError> {
        let left = Self::build_row_check_value(self.left_value, table_name, column_names)?;
        let right = Self::build_row_check_value(self.right_value, table_name, column_names)?;

        Ok(
            RowCheck {
                operator: self.operator,
                left,
                right,
            }
        )
    }

    pub fn build_row_check_value(value: SqlValue, table_name: &str, column_names: &[String]) -> Result<RowCheckValue, TableError> {
        match value {
            SqlValue::Identificator(column_string) => {
                let splitted_identificator: Vec<&str> = column_string.splitn(2, '.').collect();
                match splitted_identificator.len() {
                    1 => {
                        let index = column_names.iter()
                            .position(|table_column_name| table_column_name.eq(&column_string));
                        match index {
                            None => Err(TableError::ColumnNotExist {
                                table_name: table_name.to_string(),
                                column_name: column_string
                            }),
                            Some(i) => Ok(RowCheckValue::TableColumn(i)),
                        }
                    },
                    2 => {
                        if !splitted_identificator[0].eq(table_name) {
                            Err(TableError::TableNotExist(table_name.to_string()))
                        } else {
                            let index = column_names.iter()
                                .position(|table_column_name| table_column_name.eq(splitted_identificator[1]));
                            match index {
                                None => Err(TableError::ColumnNotExist {
                                    table_name: table_name.to_string(),
                                    column_name: splitted_identificator[1].to_string()
                                }),
                                Some(i) => Ok(RowCheckValue::TableColumn(i)),
                            }
                        }
                    },
                    _ => Err(TableError::UnexpectedBinaryConditionError {
                        table_name: table_name.to_string(),
                        column_string: column_string.to_string()
                    })
                }
            },
            _ => Ok(RowCheckValue::Static(value)),
        }
    }
}
