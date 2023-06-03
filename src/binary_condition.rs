use std::fmt;

use crate::lexer::SqlValue;
use crate::cmp_operator::CmpOperator;
use crate::row_check::{RowCheck, RowCheckValue};

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
    pub fn compile(self, table_name: &str, column_names: &[String]) -> RowCheck {
        let left = Self::build_row_check_value(self.left_value, table_name, column_names);
        let right = Self::build_row_check_value(self.right_value, table_name, column_names);

        RowCheck {
            operator: self.operator,
            left,
            right,
        }
    }

    pub fn build_row_check_value(value: SqlValue, table_name: &str, column_names: &[String]) -> RowCheckValue {
        let string_value = value.to_string();
        let splitted_identificator: Vec<&str> = string_value.split('.').collect();
        match splitted_identificator.len() {
            1 => {
                let index = column_names.iter()
                    .position(|table_column_name| table_column_name.eq(&string_value));
                match index {
                    None => RowCheckValue::Static(value),
                    Some(i) => RowCheckValue::TableColumn(i),
                }
            },
            2 => {
                if !splitted_identificator[0].eq(table_name) {
                    RowCheckValue::Static(value)
                } else {
                    let index = column_names.iter()
                        .position(|table_column_name| table_column_name.eq(splitted_identificator[1]));
                    match index {
                        None => RowCheckValue::Static(value),
                        Some(i) => RowCheckValue::TableColumn(i),
                    }
                }
            },
            _ => RowCheckValue::Static(value),
        }
    }
}
