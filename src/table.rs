use crate::command::{ColumnDefinition, WhereClause, SelectColumnName};
use crate::lexer::SqlValue;
use crate::row::Row;

#[derive(Debug, Clone)]
pub enum ColumnType {
    Integer,
    String
}

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub column_types: Vec<ColumnType>,
    column_names: Vec<String>,
    rows: Vec<Vec<SqlValue>>,
}


impl Table {
    // TODO: do we need result?
    pub fn new(name: String, column_definitions: Vec<ColumnDefinition>) -> Table {
        let mut column_names = vec![];
        let mut column_types = vec![];

        for column_definition in column_definitions {
            column_names.push(column_definition.name.to_string());
            column_types.push(column_definition.kind);
        }

        Self { name, column_types, column_names, rows: vec![] }
    }

    pub fn select(&self, column_names: Vec<SelectColumnName>, where_clause: Option<WhereClause>) -> Result<Vec<Row>, String> {
        let mut result_rows = vec![];
        let row_fits_where_clause = match &where_clause {
            None => Box::new(|_| Ok(true)),
            Some(where_clause) => where_clause.build_filter(self),
        };

        for i in 0..self.rows.len() {
            let row = &self.rows[i];
            if !row_fits_where_clause(row)? { continue };

            let mut column_values: Vec<SqlValue> = vec![];
            let mut column_types: Vec<ColumnType> = vec![];

            for select_column_name in &column_names {
                match select_column_name {
                    SelectColumnName::Name(column_name) => {
                        let column_index = self.column_index(&column_name.to_string())
                            .ok_or(format!("column {} does not exist", column_name))?;
                        let column_value = row.get(column_index)
                            .ok_or(format!("table {} does not have a column with index {}", self.name, column_index))?;
                        let column_type = self.column_types.get(column_index)
                            .ok_or(format!("table {} does not have a column with index {}", self.name, column_index))?;
                        column_values.push(column_value.clone());
                        column_types.push(column_type.clone());
                    },
                    SelectColumnName::AllColumns => {
                        let mut column_values_clone = row.clone();
                        let mut column_types_clone = self.column_types.clone();
                        column_values.append(&mut column_values_clone);
                        column_types.append(&mut column_types_clone);
                    },
                }
            }

            result_rows.push(Row { column_values, column_types });
        }

        Ok(result_rows)
    }

    // TODO: add hashmap of name -> indices to avoid names scanning
    pub fn column_index(&self, column_name: &str) -> Option<usize> {
        self.column_names.iter()
            .position(|table_column_name| table_column_name.eq(column_name))
    }

    // fn page_number(row: &Row) -> Option<uint> {
    //     Some(row.id)
    // }
}
