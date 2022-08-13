use crate::table::ColumnType;
use crate::row::Row;


/// This struct represents simple collection of rows,
/// plus information on its columns types and names. It does not check if row matches
/// column types - that is a job of the code that generates the result.
#[derive(Debug)]
pub struct QueryResult {
    pub column_types: Vec<ColumnType>,
    pub column_names: Vec<String>,
    pub rows: Vec<Row>,
}

impl QueryResult {
    pub fn spawn_row(&mut self) -> &mut Row {
        let row = Row::new(&self.column_types);
        self.rows.push(row);
        self.rows.last_mut().unwrap()
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    //pub fn get<'a, T: From<&'a SqlValue>>(&self, index: usize) -> Result<T, String> {
    //    let value = self.column_values.get(index)
    //        .ok_or(format!("row does not contain data with offset {}", index))?;

    //    Ok(value.into())
    //}

    // pub fn get(&self, index: usize) -> Result<SqlValue, String> {
    //     let value_ref = self.column_values.get(index)
    //         .ok_or(format!("row does not contain data with offset {}", index))?;

    //     Ok(value_ref.clone())
    // }
}
