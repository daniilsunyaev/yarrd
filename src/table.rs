use std::fmt;
use std::io::{self, Write, Read};

use crate::command::{ColumnDefinition, FieldAssignment, SelectColumnName};
use crate::where_clause::WhereClause;
use crate::lexer::SqlValue;
use crate::row::Row;
use crate::execution_error::ExecutionError;
use crate::meta_command_error::MetaCommandError;
use crate::query_result::QueryResult;

#[derive(Debug, Clone, Copy)]
pub enum ColumnType {
    Integer,
    String
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Integer => write!(f, "INT"),
            Self::String => write!(f, "STRING"),
        }
    }
}

impl ColumnType {
    pub fn matches_value(&self, value: &SqlValue) -> bool {
        match self {
            Self::Integer => {
                match value {
                    SqlValue::Integer(_) | SqlValue::Null => true,
                    _ => false,
                }
            },
            Self::String => {
                match value {
                    SqlValue::Integer(_) => false,
                    _ => true,
                }
            }
        }

    }
}

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub column_types: Vec<ColumnType>,
    pub column_names: Vec<String>,
    rows: Vec<Row>,
    free_rows: Vec<usize>, // TODO: this should be stored in database file or in table file
}


impl Table {
    pub fn new(name: String, column_definitions: Vec<ColumnDefinition>) -> Table {
        let mut column_names = vec![];
        let mut column_types = vec![];

        for column_definition in column_definitions {
            column_names.push(column_definition.name.to_string());
            column_types.push(column_definition.kind);
        }

        Self { name, column_types, column_names, rows: vec![], free_rows: vec![] }
    }

    pub fn write_rows<W: Write>(&self, mut target: W) -> Result<(), io::Error> {
        for i in 0..self.rows.len() {
        //for row in &self.rows {
            if self.free_rows.contains(&i) { continue }

            let row = &self.rows[i];
            target.write(row.as_bytes())?;
        }
        Ok(())
    }

    pub fn read_rows<R: Read>(&mut self, mut source: R, total_size: u64) -> Result<(), MetaCommandError> {
        let row_size = Row::calculate_row_size(&self.column_types);
        if total_size % row_size as u64 != 0 {
            return Err(MetaCommandError::TableRowSizeDoesNotMatchSource(row_size, total_size));
        }
        for _ in 0..(total_size / row_size as u64) {
            let mut row_data = vec![0u8; row_size];
            source.read(&mut row_data)?;
            let row = Row::from_bytes(row_data);//, &self.column_types);
            self.rows.push(row);
        }

        Ok(())
    }

    pub fn select(&self, select_column_names: Vec<SelectColumnName>, where_clause: Option<WhereClause>) -> Result<QueryResult, ExecutionError> {
        let mut column_names = vec![];
        let mut column_types = vec![];
        let mut column_indices = vec![];

        for select_column_name in &select_column_names {
            match select_column_name {
                SelectColumnName::Name(column_name) => {
                    let column_name = column_name.to_string();
                    let column_index = self.column_index(&column_name)
                        .ok_or(ExecutionError::ColumnNotExist { column_name: column_name.clone(), table_name: self.name.clone() })?;
                    let column_type = *self.column_types.get(column_index)
                        .ok_or(ExecutionError::ColumnNthNotExist { column_index, table_name: self.name.clone() })?;
                    column_names.push(column_name);
                    column_types.push(column_type);
                    column_indices.push(column_index);
                },
                SelectColumnName::AllColumns => {
                    column_names.extend_from_slice(&self.column_names[..]);
                    column_types.extend_from_slice(&self.column_types[..]);
                    for i in 0..column_types.len() { column_indices.push(i) };
                }
            }
        }

        let mut result = QueryResult { column_names, column_types, rows: vec![] };

        let matching_rows_indices = self.get_matching_rows_indices(where_clause)?;

        for i in matching_rows_indices {
            let row = &self.rows[i];

            let mut column_values_data: Vec<u8> = vec![];
            let result_row = result.spawn_row();

            for column_index in column_indices.iter() {
                column_values_data.extend_from_slice(row.get_cell_bytes(&self.column_types, *column_index));
                let column_is_null = row.cell_is_null(*column_index);
                result_row.set_cell_bytes(&self.column_types, *column_index, &column_values_data, column_is_null)?;
            }
        }

        Ok(result)
    }

    pub fn insert(&mut self, column_names: Option<Vec<String>>, values: Vec<SqlValue>) -> Result<(), ExecutionError> {
        let column_names = match &column_names {
            Some(column_names) => column_names,
            None => &self.column_names,
        };

        let column_indices = self.get_columns_indices(column_names)?;
        self.validate_values_type(&values, &column_indices)?;
        let mut result_values = vec![SqlValue::Null; self.column_types.len()];
        for (value, column_index) in values.into_iter().zip(column_indices.into_iter()) {
            result_values[column_index] = value;
        }

        let row = Row::from_sql_values(result_values, &self.column_types)?;

        match self.free_rows.pop() {
            Some(i) => self.rows[i] = row,
            None => self.rows.push(row),
        }

        Ok(())
    }

    pub fn update(&mut self, field_assignments: Vec<FieldAssignment>, where_clause: Option<WhereClause>) -> Result<(), ExecutionError> {
        let (column_names, column_values): (Vec<String>, Vec<SqlValue>) = field_assignments.into_iter()
            .map(|assignment| (assignment.column_name, assignment.value))
            .unzip();

        let column_indices = self.get_columns_indices(&column_names)?;
        self.validate_values_type(&column_values, &column_indices)?;

        let update_rows_indices = self.get_matching_rows_indices(where_clause)?;

        for update_row_index in update_rows_indices {
            let row = self.rows.get_mut(update_row_index).unwrap();

            for (column_number, column_value) in column_values.iter().enumerate() {
                let column_table_number = column_indices[column_number];
                row.set_cell(&self.column_types, column_table_number, column_value)?;
            }
        }

        Ok(())
    }

    pub fn delete(&mut self, where_clause: Option<WhereClause>) -> Result<(), ExecutionError> {
        let mut delete_rows_indices = self.get_matching_rows_indices(where_clause)?;
        self.free_rows.append(&mut delete_rows_indices);
        Ok(())
    }

    fn get_matching_rows_indices(&self, where_clause: Option<WhereClause>) -> Result<Vec<usize>, ExecutionError> {
        let mut rows_indices = vec![];
        let row_fits_where_clause = match &where_clause {
            None => Box::new(|_| Ok(true)),
            Some(where_clause) => where_clause.build_filter(self),
        };

        for (i, row) in self.rows.iter().enumerate() {
            if !row_fits_where_clause(row)? || self.free_rows.contains(&i) { continue };
            rows_indices.push(i)
        }

        Ok(rows_indices)
    }

    fn get_columns_indices(&self, column_names: &[String]) -> Result<Vec<usize>, ExecutionError> {
        let mut column_indices = Vec::new();
        for column_name in column_names {
            column_indices.push(
                self.column_index(column_name)
                    .ok_or(ExecutionError::ColumnNotExist { column_name: column_name.clone(), table_name: self.name.clone() })?
            );
        }

        Ok(column_indices)
    }

    fn validate_values_type(&self, columns_values: &[SqlValue], column_indices: &[usize]) -> Result<(), ExecutionError> {
        for (value_index, value) in columns_values.iter().enumerate() {
            let column_index = column_indices[value_index];

            if !self.column_types[column_index].matches_value(value) {
                return Err(ExecutionError::ValueColumnMismatch {
                    value: value.clone(), column_name: self.column_names[column_index].clone(), column_type: self.column_types[column_index]
                });
            }
        }
        Ok(())
    }

    // TODO: add hashmap of name -> indices to avoid names scanning
    pub fn column_index(&self, column_name: &str) -> Option<usize> {
        self.column_names.iter()
            .position(|table_column_name| table_column_name.eq(column_name))
    }
}
