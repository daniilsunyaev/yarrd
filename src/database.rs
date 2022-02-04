use std::collections::HashMap;

use crate::command::{Command, ColumnDefinition, FieldAssignment, SelectColumnName};
use crate::where_clause::WhereClause;
use crate::lexer::SqlValue;
use crate::table::Table;
use crate::row::Row;
use crate::execution_error::ExecutionError;

pub struct Database {
    tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Database {
        Self { tables: HashMap::new() }
    }

    pub fn execute(&mut self, command: Command) -> Result<Option<Vec<Row>>, ExecutionError> {
        match command {
            Command::CreateTable { table_name, columns } => self.create_table(table_name, columns),
            Command::DropTable { table_name } => self.drop_table(table_name),
            Command::Select { table_name, column_names, where_clause } => self.select_rows(table_name, column_names, where_clause),
            Command::InsertInto { table_name, column_names, values } => self.insert_rows(table_name, column_names, values),
            Command::Update { table_name, field_assignments, where_clause } => self.update_rows(table_name, field_assignments, where_clause),
            Command::Delete { table_name, where_clause } => self.delete_rows(table_name, where_clause),
            Command::VoidCommand => Ok(None),
            // _ => Err(format!("unrecognized command {:?}", command)),
        }
    }

    fn create_table(&mut self, table_name: SqlValue, columns: Vec<ColumnDefinition>) -> Result<Option<Vec<Row>>, ExecutionError> {
        let table_name_string = table_name.to_string();

        if self.tables.contains_key(table_name_string.as_str()) {
            return Err(ExecutionError::TableAlreadyExist(table_name_string));
        }

        let table = Table::new(table_name_string.clone(), columns);
        self.tables.insert(table_name_string, table);

        Ok(None)
    }

    fn drop_table(&mut self, table_name: SqlValue) -> Result<Option<Vec<Row>>, ExecutionError> {
        let table_name_string = table_name.to_string();

        match self.tables.remove(table_name_string.as_str()) {
            None => Err(ExecutionError::TableNotExist(table_name_string)),
            Some(_) => Ok(None)
        }
    }

    fn select_rows(&self, table_name: SqlValue, column_names: Vec<SelectColumnName>, where_clause: Option<WhereClause>) -> Result<Option<Vec<Row>>, ExecutionError> {
        let table_name_string = table_name.to_string();
        let table = match self.tables.get(table_name_string.as_str()) {
            None => return Err(ExecutionError::TableNotExist(table_name_string)),
            Some(existing_table) => existing_table,
        };

        Ok(Some(table.select(column_names, where_clause)?))
    }

    fn insert_rows(&mut self, table_name: SqlValue, column_names: Option<Vec<SqlValue>>, values: Vec<SqlValue>) -> Result<Option<Vec<Row>>, ExecutionError> {
        let table_name_string = table_name.to_string();
        let table = match self.tables.get_mut(table_name_string.as_str()) {
            None => return Err(ExecutionError::TableNotExist(table_name_string)),
            Some(existing_table) => existing_table,
        };

        let column_names = column_names.map(|sql_names| sql_names.iter().map(|sql_name| sql_name.to_string()).collect());

        table.insert(column_names, values)?;
        Ok(None)
    }

    fn update_rows(&mut self, table_name: SqlValue, field_assignments: Vec<FieldAssignment>, where_clause: Option<WhereClause>) -> Result<Option<Vec<Row>>, ExecutionError> {
        let table_name_string = table_name.to_string();
        let table = match self.tables.get_mut(table_name_string.as_str()) {
            None => return Err(ExecutionError::TableNotExist(table_name_string)),
            Some(existing_table) => existing_table,
        };

        table.update(field_assignments, where_clause)?;
        Ok(None)
    }

    fn delete_rows(&mut self, table_name: SqlValue, where_clause: Option<WhereClause>) -> Result<Option<Vec<Row>>, ExecutionError> {
        let table_name_string = table_name.to_string();
        let table = match self.tables.get_mut(table_name_string.as_str()) {
            None => return Err(ExecutionError::TableNotExist(table_name_string)),
            Some(existing_table) => existing_table,
        };

        table.delete(where_clause)?;
        Ok(None)
    }
}