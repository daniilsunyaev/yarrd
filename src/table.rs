use std::fmt;
use std::path::PathBuf;

use crate::command::{ColumnDefinition, FieldAssignment, SelectColumnName};
use crate::where_clause::WhereClause;
use crate::lexer::SqlValue;
use crate::row::Row;
use crate::execution_error::ExecutionError;
use crate::query_result::QueryResult;
use crate::pager::{Pager, PagerError};
use crate::where_clause::WhereFilter;

#[derive(Debug, Clone, Copy)]
pub enum ColumnType {
    Integer,
    Float,
    String,
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Integer => write!(f, "INT"),
            Self::Float => write!(f, "FLOAT"),
            Self::String => write!(f, "STRING"),
        }
    }
}

impl ColumnType {
    pub fn matches_value(&self, value: &SqlValue) -> bool {
        match self {
            Self::Integer => matches!(value,
                                      SqlValue::Integer(_) | SqlValue::Null),
            Self::Float => matches!(value,
                                    SqlValue::Float(_) | SqlValue::Null),
            Self::String => matches!(value,
                                     SqlValue::String(_) | SqlValue::Identificator(_) | SqlValue::Null),
        }
    }
}

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub column_types: Vec<ColumnType>,
    pub column_names: Vec<String>,
    pager: Pager,
}

impl Table {
    // TODO: maybe this should be table error or init error?
    pub fn new(table_filepath: PathBuf, name: &str, column_definitions: Vec<ColumnDefinition>) -> Result<Table, PagerError> {
        let mut column_names = vec![];
        let mut column_types = vec![];

        for column_definition in column_definitions {
            column_names.push(column_definition.name.to_string());
            column_types.push(column_definition.kind);
        }
        let row_size = Row::calculate_row_size(&column_types);
        let pager = Pager::new(table_filepath.as_path(), row_size)?;

        Ok(Self { name: name.to_string(), column_types, column_names, pager })
    }

    pub fn select(&mut self, select_column_names: Vec<SelectColumnName>, where_clause: Option<WhereClause>) -> Result<QueryResult, ExecutionError> {
        let mut result_column_names = vec![];
        let mut result_column_types = vec![];
        let mut result_column_indices = vec![];

        for select_column_name in &select_column_names {
            match select_column_name {
                SelectColumnName::Name(column_name) => {
                    let column_name = column_name.to_string();
                    let column_index = self.column_index(&column_name)
                        .ok_or(ExecutionError::ColumnNotExist { column_name: column_name.clone(), table_name: self.name.clone() })?;
                    let column_type = *self.column_types.get(column_index)
                        .ok_or(ExecutionError::ColumnNthNotExist { column_index, table_name: self.name.clone() })?;
                    result_column_names.push(column_name);
                    result_column_types.push(column_type);
                    result_column_indices.push(column_index);
                },
                SelectColumnName::AllColumns => {
                    result_column_names.extend_from_slice(&self.column_names[..]);
                    result_column_types.extend_from_slice(&self.column_types[..]);
                    for i in 0..self.column_types.len() { result_column_indices.push(i) };
                }
            }
        }

        // need to clone because of borrow checker
        let mut result = QueryResult { column_names: result_column_names, column_types: result_column_types.clone(), rows: vec![] };
        let column_types = self.column_types.clone();

        for row_check in self.matching_rows(where_clause) {
            let (_row_number, row) = row_check?;
            let result_row = result.spawn_row();

            for (i, column_index) in result_column_indices.iter().enumerate() {
                let column_values_data = row.get_cell_bytes(&column_types, *column_index);
                let column_is_null = row.cell_is_null(*column_index);
                result_row.set_cell_bytes(&result_column_types, i, column_values_data, column_is_null)?;
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
        self.pager.insert_row(row).map_err(ExecutionError::PagerError)
    }

    pub fn update(&mut self, field_assignments: Vec<FieldAssignment>, where_clause: Option<WhereClause>) -> Result<(), ExecutionError> {
        let (column_names, column_values): (Vec<String>, Vec<SqlValue>) = field_assignments.into_iter()
            .map(|assignment| (assignment.column_name, assignment.value))
            .unzip();

        let column_indices = self.get_columns_indices(&column_names)?;
        let column_types = self.column_types.clone(); // need to clone this because of borrow checker
        self.validate_values_type(&column_values, &column_indices)?;
        let pager_raw: *mut Pager = &mut self.pager;

        for row_check in self.matching_rows(where_clause) {
            let (row_number, mut row) = row_check?;

            for (column_number, column_value) in column_values.iter().enumerate() {
                let column_table_number = column_indices[column_number];
                row.set_cell(&column_types, column_table_number, column_value)?;
            }
            // pager will not reallocate to a new space during matching_rows iteration
            // so we can safely dereference raw mut pointer
            unsafe {
                (*pager_raw).update_row(row_number, &row)?;
            }
        }

        Ok(())
    }

    pub fn delete(&mut self, where_clause: Option<WhereClause>) -> Result<(), ExecutionError> {
        let pager_raw: *mut Pager = &mut self.pager;
        for row_check in self.matching_rows(where_clause) {
            let (row_number, _row) = row_check?;
            // pager will not reallocate to a new space during matching_rows iteration
            // so we can safely dereference raw mut pointer
            unsafe {
                (*pager_raw).delete_row(row_number)?;
            }
        }
        Ok(())
    }

    fn matching_rows(&mut self, where_clause: Option<WhereClause>) -> impl Iterator<Item = Result<(u64, Row), ExecutionError>> + '_ {
        let where_filter = match where_clause {
            None => WhereFilter::dummy(),
            Some(where_clause) => where_clause.compile(&self.column_types[..], &self.name, &self.column_names),
        };

        Self::seq_scan(&mut self.pager).filter_map(move |(i, row_check)| {
            match row_check {
                Ok(row) => {
                    match where_filter.matches(&row) {
                        Ok(true) => Some(Ok((i, row))),
                        Ok(false) => None,
                        Err(error) => Some(Err(error)),
                    }
                },
                Err(error) => Some(Err(error.into())),
            }

        })
    }

    fn seq_scan(pager: &mut Pager) -> impl Iterator<Item = (u64, Result<Row, PagerError>)> + '_ {
        let max_rows = pager.max_rows();
        (0..max_rows).map(|row_number| (row_number, pager.get_row(row_number)))
            .filter(|(_row_number, row_check)| row_check.is_err() || row_check.as_ref().unwrap().is_some())
            .map(|(row_number, row_check)| (row_number, row_check.map(|row_opt| row_opt.unwrap())))
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
    // and pass hash ref to compile
    pub fn column_index(&self, column_name: &str) -> Option<usize> {
        self.column_names.iter()
            .position(|table_column_name| table_column_name.eq(column_name))
    }
}
