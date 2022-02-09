use std::fmt;
use std::io::{self, Write, Read};

use crate::command::{ColumnDefinition, FieldAssignment, SelectColumnName};
use crate::where_clause::WhereClause;
use crate::lexer::SqlValue;
use crate::row::Row;
use crate::execution_error::ExecutionError;
use crate::meta_command_error::MetaCommandError;
use crate::serialize::{serialize_into, deserialize};

const INTEGER_SIZE: usize = 8;
const STRING_SIZE: usize = 256;

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
                    SqlValue::Integer(_) => true,
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
    rows: Vec<Vec<u8>>,
    free_rows: Vec<usize>,
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
        for row in &self.rows {
            target.write(&row)?;
        }
        Ok(())
    }

    pub fn read_rows<R: Read>(&mut self, mut source: R, total_size: u64) -> Result<(), MetaCommandError> {
        let row_size = self.row_size();
        if total_size % row_size as u64 != 0 {
            return Err(MetaCommandError::TableRowSizeDoesNotMatchSource(row_size, total_size));
        }
        for _ in 0..(total_size / row_size as u64) {
            let mut row = vec![0u8; row_size];
            source.read(&mut row)?;
            self.rows.push(row);
        }

        Ok(())
    }

    pub fn select(&self, column_names: Vec<SelectColumnName>, where_clause: Option<WhereClause>) -> Result<Vec<Row>, ExecutionError> {
        let mut result_rows = vec![];

        let matching_rows_indices = self.get_matching_rows_indices(where_clause)?;

        for i in matching_rows_indices {
            let row = &self.rows[i];

            let mut column_is_null: Vec<bool> = vec![];
            let mut column_values_data: Vec<u8> = vec![];
            let mut column_types: Vec<ColumnType> = vec![];

            for select_column_name in &column_names {
                match select_column_name {
                    SelectColumnName::Name(column_name) => {
                        let column_name = column_name.to_string();
                        let column_index = self.column_index(&column_name)
                            .ok_or(ExecutionError::ColumnNotExist { column_name, table_name: self.name.clone() })?;
                        let column_type = self.column_types.get(column_index)
                            .ok_or(ExecutionError::ColumnNthNotExist { column_index, table_name: self.name.clone() })?;

                        column_values_data.extend_from_slice(self.get_cell(i, column_index));
                        column_types.push(*column_type);
                        column_is_null.push(self.cell_is_null(i, column_index));
                    },
                    SelectColumnName::AllColumns => {
                        let mut column_types_clone = self.column_types.clone();
                        column_values_data.extend_from_slice(&row[self.column_offset(0)..]);
                        column_types.append(&mut column_types_clone);
                        for (column_number, _) in column_types.iter().enumerate() {
                            column_is_null.push(self.cell_is_null(i, column_number));
                        }
                    },
                }
            }

            let mut column_values = vec![0u8; (column_types.len() + 7) / 8];
            println!("bitmask size {} bytes", column_values.len());
            println!("column is null {:?}", column_is_null);
            column_values.extend_from_slice(&column_values_data[..]);
            for i in 0..column_types.len() {
                if column_is_null[i] {
                    Self::nullify_cell(&mut column_values, i);
                }
            }

            result_rows.push(Row { column_values, column_types }); // TODO: should be query result (mini table)
        }

        Ok(result_rows)
    }

    pub fn insert(&mut self, column_names: Option<Vec<String>>, values: Vec<SqlValue>) -> Result<(), ExecutionError> {
        let column_names = match &column_names {
            Some(column_names) => column_names,
            None => &self.column_names,
        };

        let column_indices = self.get_columns_indices(column_names)?;
        self.validate_values_type(&values, &column_indices)?;

        let mut row = vec![255u8; self.null_bitmask_size()];
        row.resize(self.row_size(), 0u8);
        for (value_index, value) in values.iter().enumerate() {
            let column_index = column_indices[value_index];
            let column_offset = self.column_offset(column_index);
            let column_type = self.column_types[column_index];

            serialize_into(&mut row[column_offset..], column_type, value)?;
            if *value != SqlValue::Null {
                Self::denullify_cell(&mut row, column_index);
            }
        }

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
            for (column_number, column_value) in column_values.iter().enumerate() {
                let column_table_number = column_indices[column_number];
                let column_type = self.column_types[column_table_number];
                let cell = self.get_cell_mut(update_row_index, column_table_number);

                serialize_into(cell, column_type, column_value)?;
                if *column_value == SqlValue::Null {
                    Self::nullify_cell(&mut self.rows[update_row_index], column_table_number);
                } else {
                    Self::denullify_cell(&mut self.rows[update_row_index], column_table_number);
                }
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

        for i in 0..self.rows.len() {
            if !row_fits_where_clause(i)? || self.free_rows.contains(&i) { continue };
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

    fn column_offset(&self, column_index: usize) -> usize {
        self.null_bitmask_size() +
            (0..column_index).fold(0, |total_size, i| total_size + Self::column_size(self.column_types[i]))
    }

    fn column_size(column_type: ColumnType) -> usize {
        match column_type {
            ColumnType::Integer => INTEGER_SIZE,
            ColumnType::String => STRING_SIZE,
        }
    }

    fn row_size(&self) -> usize {
        self.null_bitmask_size() +
            self.column_types.iter().map(|ct| Self::column_size(*ct)).sum::<usize>()
    }

    fn null_bitmask_size(&self) -> usize {
        (self.column_types.len() + 7) / 8
    }

    fn null_bitmask(&self, row_index: usize) -> &[u8] {
        &self.rows[row_index][0..self.null_bitmask_size()]
    }

    fn cell_is_null(&self, row_index: usize, column_index: usize) -> bool {
        self.null_bitmask(row_index)[column_index / 8] & (1 << (column_index % 8)) != 0
    }

    fn nullify_cell(row: &mut Vec<u8>, column_index: usize) {
        row[column_index / 8] |= 1 << (column_index % 8);
    }

    fn denullify_cell(row: &mut Vec<u8>, column_index: usize) {
        row[column_index / 8] &= !(1 << (column_index % 8));
    }

    fn get_cell(&self, row_index: usize, column_index: usize) -> &[u8] {
        let offset = self.column_offset(column_index);
        let cell_size = Self::column_size(self.column_types[column_index]);
        &self.rows[row_index][offset..(offset + cell_size)]
    }

    fn get_cell_mut(&mut self, row_index: usize, column_index: usize) -> &mut [u8] {
        let offset = self.column_offset(column_index);
        let cell_size = Self::column_size(self.column_types[column_index]);
        &mut self.rows[row_index][offset..(offset + cell_size)]
    }

    pub fn get_cell_sql_value(&self, row_index: usize, column_index: usize) -> Result<SqlValue, ExecutionError> {
        if self.cell_is_null(row_index, column_index) {
            Ok(SqlValue::Null)
        } else {
            let cell = self.get_cell(row_index, column_index);
            let column_type = self.column_types[column_index];
            deserialize(cell, column_type).map_err(|e| e.into())
        }
    }
}
