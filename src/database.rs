use std::collections::HashMap;

use crate::command::{Command, ColumnDefinition, SelectColumnName};
use crate::where_clause::WhereClause;
use crate::lexer::SqlValue;
use crate::table::Table;
use crate::row::Row;

pub struct Database {
    tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Database {
        Self { tables: HashMap::new() }
    }

    pub fn execute(&mut self, command: Command) -> Result<(), String> {
        match command {
            Command::CreateTable { table_name, columns } => self.create_table(table_name, columns),
            Command::DropTable { table_name } => self.drop_table(table_name),
            Command::Select { table_name, column_names, where_clause } => {
                println!("{:?}", self.select_rows(table_name, column_names, where_clause)?);
                Ok(())
            },
            Command::InsertInto { table_name, column_names, values } => self.insert_rows(table_name, column_names, values),
            _ => Err(format!("unrecognized command {:?}", command)),
        }
    }

    fn create_table(&mut self, table_name: SqlValue, columns: Vec<ColumnDefinition>) -> Result<(), String> {
        let table_name_string = table_name.to_string();

        if self.tables.contains_key(table_name_string.as_str()) {
            return Err(format!("table '{}' already exists", table_name_string));
        }

        let table = Table::new(table_name_string.clone(), columns);
        self.tables.insert(table_name_string, table);

        Ok(())
    }

    fn drop_table(&mut self, table_name: SqlValue) -> Result<(), String> {
        let table_name_string = table_name.to_string();

        match self.tables.remove(table_name_string.as_str()) {
            None => Err(format!("table '{}' does not exist", table_name_string)),
            Some(_) => Ok(())
        }
    }

    fn select_rows(&self, table_name: SqlValue, column_names: Vec<SelectColumnName>, where_clause: Option<WhereClause>) -> Result<Vec<Row>, String> {
        let table_name_string = table_name.to_string();
        let table = match self.tables.get(table_name_string.as_str()) {
            None => return Err(format!("table '{}' does not exist", table_name_string)),
            Some(existing_table) => existing_table,
        };

        table.select(column_names, where_clause)
    }

    fn insert_rows(&mut self, table_name: SqlValue, column_names: Option<Vec<SqlValue>>, values: Vec<SqlValue>) -> Result<(), String> {
        let table_name_string = table_name.to_string();
        let table = match self.tables.get_mut(table_name_string.as_str()) {
            None => return Err(format!("table '{}' does not exist", table_name_string)),
            Some(existing_table) => existing_table,
        };

        let column_names = column_names.map(|sql_names| sql_names.iter().map(|sql_name| sql_name.to_string()).collect());

        table.insert(column_names, values)
    }
}
