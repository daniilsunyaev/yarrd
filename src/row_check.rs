use crate::lexer::SqlValue;
use crate::table::error::TableError;
use crate::row::Row;
use crate::table::ColumnType;
use crate::cmp_operator::CmpOperator;

#[derive(Debug)]
pub enum RowCheckValue {
    TableColumn(usize),
    Static(SqlValue),
}

#[derive(Debug)]
pub struct RowCheck<'a> {
    pub operator: CmpOperator,
    pub left: RowCheckValue,
    pub right: RowCheckValue,
    pub column_types: &'a [ColumnType],
}

impl<'a> RowCheck<'a> {
    pub fn dummy() -> Self {
        Self {
            operator: CmpOperator::Equals,
            left: RowCheckValue::Static(SqlValue::Integer(1)),
            right: RowCheckValue::Static(SqlValue::Integer(1)),
            column_types: &[],
        }
    }

    pub fn matches(&'a self, row: &'a Row) -> Result<bool, TableError> {
        self
            .operator
            .apply(&self.get_value(&self.left, row)?, &self.get_value(&self.right, row)?)
            .map_err(TableError::CmpError)

    }

    fn get_value(&'a self, value: &'a RowCheckValue, row: &'a Row) -> Result<SqlValue, TableError> {
        match value {
            RowCheckValue::Static(sql_value) => Ok(sql_value.clone()),
            RowCheckValue::TableColumn(index) =>
                row.get_cell_sql_value(self.column_types, *index).map_err(TableError::CannotGetCell),
        }
    }
}

