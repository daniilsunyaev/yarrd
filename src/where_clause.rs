use crate::lexer::SqlValue;
use crate::table::ColumnType;
use crate::cmp_operator::CmpOperator;
use crate::row_check::{RowCheck, RowCheckValue};

#[derive(Debug)]
pub struct WhereClause {
    pub left_value: SqlValue,
    pub right_value: SqlValue,
    pub operator: CmpOperator,
}

impl WhereClause {
    pub fn compile<'a>(self, column_types: &'a [ColumnType], table_name: &'a str, column_names: &'a [String]) -> RowCheck<'a> {
        let left = Self::build_where_value(self.left_value, table_name, column_names);
        let right = Self::build_where_value(self.right_value, table_name, column_names);

        RowCheck {
            operator: self.operator,
            left,
            right,
            column_types,
        }
    }

    pub fn build_where_value(value: SqlValue, table_name: &str, column_names: &[String]) -> RowCheckValue {
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
