use std::collections::HashMap;

use crate::command::{Command, ColumnDefinition};
use crate::lexer::SqlValue;
use crate::table::Table;

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
}
