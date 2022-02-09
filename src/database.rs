use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Write};
use std::fs::OpenOptions;
use std::path::Path;

use crate::command::{Command, ColumnDefinition, FieldAssignment, SelectColumnName};
use crate::where_clause::WhereClause;
use crate::lexer::SqlValue;
use crate::table::{Table, ColumnType};
use crate::row::Row;
use crate::execution_error::ExecutionError;
use crate::meta_command_error::MetaCommandError;

pub struct Database {
    tables: HashMap<String, Table>,
    database_filename: String,
    tables_dir: String,
}

impl Database {
    pub fn from(filename: &str) -> Result<Database, MetaCommandError> {
        let mut tables = HashMap::new();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(filename)?;

        let mut reader = BufReader::new(file);
        let mut tables_dir = String::new();
        reader.read_line(&mut tables_dir)?;
        if !Path::new(tables_dir.trim()).is_dir() {
            return Err(MetaCommandError::DatabaseTablesDirNotExist(tables_dir));
        }

        for line in reader.lines() {
            let line = line?;
            let mut table = Self::parse_schema_line(line.trim())?;
            let table_file = OpenOptions::new()
                .read(true)
                .open(format!("{}/{}.table", tables_dir.trim(), table.name.trim()))?;
            let table_file_size = table_file.metadata()?.len();
            table.read_rows(table_file, table_file_size)?;
            tables.insert(table.name.clone(), table);
        }

        Ok(Self { tables, database_filename: filename.to_string(), tables_dir: tables_dir.trim().to_string() })
    }

    fn parse_schema_line(table_definition_line: &str) -> Result<Table, MetaCommandError> {
        let mut word_iter = table_definition_line.split_whitespace();
        let table_name = word_iter.next()
            .ok_or(MetaCommandError::SchemaDefinitionMissing)?;
        let mut column_definitions = vec![];

        loop {
            let column_name = match word_iter.next() {
                Some(column_name) => column_name,
                None => break,
            };

            let column_type_str = word_iter.next()
                .ok_or(MetaCommandError::SchemaDefinitionInvalid {
                    table_name: table_name.to_string(),
                    expected: "column type",
                    actual: "".to_string(),
                })?;

            let column_type = match column_type_str {
                "INT" => ColumnType::Integer,
                "STRING" => ColumnType::String,
                _ => return Err(MetaCommandError::SchemaDefinitionInvalid {
                    table_name: table_name.to_string(),
                    expected: "column type (INT/STRING)",
                    actual: column_type_str.to_string(),
                }),
            };

            column_definitions.push(ColumnDefinition {
                name: SqlValue::Identificator(column_name.to_string()),
                kind: column_type
            });
        }

        Ok(Table::new(table_name.to_string(), column_definitions))
    }

    pub fn close(self) -> Result<(), io::Error> {
        let mut database_file = OpenOptions::new()
            .write(true)
            .open(self.database_filename)?;

        writeln!(database_file, "{}", self.tables_dir)?;
        for (table_name, table) in self.tables {
            let table_file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(format!("{}/{}.table", self.tables_dir, table_name))?;
            table.write_rows(table_file)?;

            write!(database_file, "{}", table_name)?;
            for i in 0..table.column_types.len() {
                write!(database_file, " {} {}", table.column_names[i], table.column_types[i])?;
            }
            writeln!(database_file, "")?;
        }
        Ok(())
    }

    pub fn execute(&mut self, command: Command) -> Result<Option<Vec<Row>>, ExecutionError> {
        match command {
            Command::CreateTable { table_name, columns } => self.create_table(table_name, columns),
            Command::DropTable { table_name } => self.drop_table(table_name),
            Command::Select { table_name, column_names, where_clause } => self.select_rows(table_name, column_names, where_clause),
            Command::InsertInto { table_name, column_names, values } => self.insert_rows(table_name, column_names, values),
            Command::Update { table_name, field_assignments, where_clause } => self.update_rows(table_name, field_assignments, where_clause),
            Command::Delete { table_name, where_clause } => self.delete_rows(table_name, where_clause),
            Command::Void => Ok(None),
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

        match self.tables.remove(table_name_string.as_str()) { // TODO: use sql_value.to_string()
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
