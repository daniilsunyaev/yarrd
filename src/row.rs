use std::convert::From;

use crate::lexer::SqlValue;
use crate::table::ColumnType;


#[derive(Debug)]
pub struct Row {
    pub column_types: Vec<ColumnType>,
    pub column_values: Vec<SqlValue>, // this will be raw data later on
}

impl Row {
    // pub fn get<'a, T: From<&'a SqlValue>>(&self, index: usize) -> Result<T, String> {
    //     let value = self.column_values.get(index)
    //         .ok_or(format!("row does not contain data with offset {}", index))?;

    //     Ok(value.into())
    // }

    // pub fn get(&self, index: usize) -> Result<SqlValue, String> {
    //     let value_ref = self.column_values.get(index)
    //         .ok_or(format!("row does not contain data with offset {}", index))?;

    //     Ok(value_ref.clone())
    // }
}
