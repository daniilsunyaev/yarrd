use crate::lexer::SqlValue;
use crate::table::error::TableError;
use crate::row::Row;
use crate::table::ColumnType;
use crate::cmp_operator::CmpOperator;

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

    pub fn matches(&'a self, row: &'a Row) -> Result<bool, TableError> {
        self
            .operator
            .apply(&self.get_value(&self.left, row)?, &self.get_value(&self.right, row)?)
            .map_err(TableError::CmpError)

    }

    fn get_value(&'a self, value: &'a WhereValue, row: &'a Row) -> Result<SqlValue, TableError> {
        match value {
            WhereValue::Value(sql_value) => Ok(sql_value.clone()),
            WhereValue::TableColumn(index) =>
                row.get_cell_sql_value(self.column_types, *index).map_err(TableError::CannotGetCell),
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
            },
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
