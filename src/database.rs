use std::time;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::command::{Command, ColumnDefinition, FieldAssignment, SelectColumnName};
use crate::where_clause::WhereClause;
use crate::lexer::SqlValue;
use crate::table::{Table, ColumnType};
use crate::execution_error::ExecutionError;
use crate::meta_command_error::MetaCommandError;
use crate::query_result::QueryResult;

const TABLE_EXTENSION: &str = "table";

pub struct Database {
    tables: HashMap<String, Table>,
    database_filepath: PathBuf,
    tables_dir: PathBuf,
}

impl Database {
    pub fn from(database_filepath: &Path) -> Result<Database, MetaCommandError> {
        let mut tables = HashMap::new();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(database_filepath)?;

        let mut reader = BufReader::new(file);
        let mut tables_dir = String::new();
        reader.read_line(&mut tables_dir)?;
        let tables_dir = PathBuf::from(tables_dir.trim());
        if !tables_dir.is_dir() {
            return Err(MetaCommandError::DatabaseTablesDirNotExist(tables_dir));
        }

        for line in reader.lines() {
            let line = line?;
            let table = Self::parse_schema_line(tables_dir.as_path(), line.trim())?;
            tables.insert(table.name.clone(), table);
        }

        Ok(Self { tables, database_filepath: PathBuf::from(database_filepath), tables_dir })
    }

    fn parse_schema_line(tables_dir: &Path, table_definition_line: &str) -> Result<Table, MetaCommandError> {
        let mut word_iter = table_definition_line.split_whitespace();
        let table_name = word_iter.next()
            .ok_or(MetaCommandError::SchemaDefinitionMissing)?;
        let mut column_definitions = vec![];

        while let Some(column_name) = word_iter.next() {
            let column_type_str = word_iter.next()
                .ok_or(MetaCommandError::SchemaDefinitionInvalid {
                    table_name: table_name.to_string(),
                    expected: "column type",
                    actual: "".to_string(),
                })?;

            let column_type = match column_type_str {
                "INT" => ColumnType::Integer,
                "FLOAT" => ColumnType::Float,
                "STRING" => ColumnType::String,
                _ => return Err(MetaCommandError::SchemaDefinitionInvalid {
                    table_name: table_name.to_string(),
                    expected: "column type (INT/FLOAT/STRING)",
                    actual: column_type_str.to_string(),
                }),
            };

            column_definitions.push(ColumnDefinition {
                name: SqlValue::Identificator(column_name.to_string()),
                kind: column_type
            });
        }
        let table_filepath = Self::table_filepath(tables_dir, table_name);

        Ok(Table::new(table_filepath, table_name, column_definitions)?)
    }

    pub fn close(self) {
        self.flush_schema();
    }

    // TODO: return result instead of unwrapping and handle err (probably via logging)
    fn flush_schema(&self) {
        let mut database_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.database_filepath).unwrap();

        writeln!(database_file, "{}", self.tables_dir.to_str().unwrap()).unwrap();
        for (table_name, table) in &self.tables {
            write!(database_file, "{}", table_name).unwrap();
            for i in 0..table.column_types.len() {
                write!(database_file, " {} {}", table.column_names[i], table.column_types[i]).unwrap();
            }
            writeln!(database_file).unwrap();
        }
    }

    pub fn execute(&mut self, command: Command) -> Result<Option<QueryResult>, ExecutionError> {
        match command {
            Command::CreateTable { table_name, columns } => self.create_table(table_name, columns),
            Command::DropTable { table_name } => self.drop_table(table_name),
            Command::Select { table_name, column_names, where_clause } => self.select_rows(table_name, column_names, where_clause),
            Command::InsertInto { table_name, column_names, values } => self.insert_rows(table_name, column_names, values),
            Command::Update { table_name, field_assignments, where_clause } => self.update_rows(table_name, field_assignments, where_clause),
            Command::Delete { table_name, where_clause } => self.delete_rows(table_name, where_clause),
            Command::RenameTable { table_name, new_table_name } => self.rename_table(table_name, new_table_name),
            Command::RenameTableColumn { table_name, column_name, new_column_name } =>
                self.rename_table_column(table_name, column_name, new_column_name),
            Command::AddTableColumn { table_name, column_definition } => self.add_table_column(table_name, column_definition),
            Command::Void => Ok(None),
            _ => Err(ExecutionError::TableNotExist("foo".to_string())), // TODO: this is temporary before we write implementation
        }
    }

    fn create_table(&mut self, table_name: SqlValue, columns: Vec<ColumnDefinition>) -> Result<Option<QueryResult>, ExecutionError> {
        let table_name_string = table_name.to_string();
        let table_filepath = Self::table_filepath(self.tables_dir.as_path(), table_name_string.as_str());

        if self.tables.contains_key(table_name_string.as_str()) {
            return Err(ExecutionError::TableAlreadyExist(table_name_string));
        }

        File::create(table_filepath.as_path())?;
        match Table::new(table_filepath.clone(), table_name_string.as_str(), columns) {
            Ok(table) => {
                self.tables.insert(table_name_string, table);
                Ok(None)
            },
            Err(create_table_error) => {
                fs::remove_file(table_filepath.as_path())
                    .unwrap_or_else(|_| panic!(
                                "failed to create table: {}, failed to remove table file '{}', try to remove it manually",
                                create_table_error, table_filepath.to_str().unwrap()
                            ));

                Err(create_table_error.into())
            }
        }
    }

    fn drop_table(&mut self, table_name: SqlValue) -> Result<Option<QueryResult>, ExecutionError> {
        let table_name_string = table_name.to_string();

        match self.tables.remove(table_name_string.as_str()) {
            None => Err(ExecutionError::TableNotExist(table_name_string)),
            Some(_) => {
                fs::remove_file(Self::table_filepath(self.tables_dir.as_path(), table_name_string.as_str()))?;
                Ok(None)
            },
        }
    }

    fn select_rows(&mut self, table_name: SqlValue, column_names: Vec<SelectColumnName>, where_clause: Option<WhereClause>) -> Result<Option<QueryResult>, ExecutionError> {
        let table_name_string = table_name.to_string();
        let table = match self.tables.get_mut(table_name_string.as_str()) {
            None => return Err(ExecutionError::TableNotExist(table_name_string)),
            Some(existing_table) => existing_table,
        };

        Ok(Some(table.select(column_names, where_clause)?))
    }

    fn insert_rows(&mut self, table_name: SqlValue, column_names: Option<Vec<SqlValue>>, values: Vec<SqlValue>) -> Result<Option<QueryResult>, ExecutionError> {
        let table_name_string = table_name.to_string();
        let table = match self.tables.get_mut(table_name_string.as_str()) {
            None => return Err(ExecutionError::TableNotExist(table_name_string)),
            Some(existing_table) => existing_table,
        };

        let column_names = column_names.map(|sql_names| sql_names.iter().map(|sql_name| sql_name.to_string()).collect());

        table.insert(column_names, values)?;
        Ok(None)
    }

    fn update_rows(&mut self, table_name: SqlValue, field_assignments: Vec<FieldAssignment>, where_clause: Option<WhereClause>) -> Result<Option<QueryResult>, ExecutionError> {
        let table_name_string = table_name.to_string();
        let table = match self.tables.get_mut(table_name_string.as_str()) {
            None => return Err(ExecutionError::TableNotExist(table_name_string)),
            Some(existing_table) => existing_table,
        };

        table.update(field_assignments, where_clause)?;
        Ok(None)
    }

    fn delete_rows(&mut self, table_name: SqlValue, where_clause: Option<WhereClause>) -> Result<Option<QueryResult>, ExecutionError> {
        let table_name_string = table_name.to_string();
        let table = match self.tables.get_mut(table_name_string.as_str()) {
            None => return Err(ExecutionError::TableNotExist(table_name_string)),
            Some(existing_table) => existing_table,
        };

        table.delete(where_clause)?;
        Ok(None)
    }

    fn rename_table(&mut self, table_name: SqlValue, new_table_name: SqlValue) -> Result<Option<QueryResult>, ExecutionError> {
        let table_name_string = table_name.to_string();
        let new_table_name_string = new_table_name.to_string();
        let table_filepath = Self::table_filepath(self.tables_dir.as_path(), table_name_string.as_str());
        let new_table_filepath = Self::table_filepath(self.tables_dir.as_path(), new_table_name_string.as_str());

        let table = match self.tables.remove(table_name_string.as_str()) {
            None => return Err(ExecutionError::TableNotExist(table_name_string)),
            Some(table) => table,
        };

        match fs::rename(table_filepath, new_table_filepath) {
            Err(io_error) => {
                self.tables.insert(table_name_string, table);
                Err(io_error.into())
            },
            Ok(_) => {
                self.tables.insert(new_table_name_string, table);
                Ok(None)
            }
        }
    }

    fn rename_table_column(&mut self, table_name: SqlValue, column_name: SqlValue, new_column_name: SqlValue) -> Result<Option<QueryResult>, ExecutionError> {
        let table_name_string = table_name.to_string();
        let column_name_string = column_name.to_string();
        let new_column_name_string = new_column_name.to_string();

        let table = match self.tables.get_mut(table_name_string.as_str()) {
            None => return Err(ExecutionError::TableNotExist(table_name_string)),
            Some(existing_table) => existing_table,
        };

        table.rename_column(column_name_string, new_column_name_string)?;

        // TODO: use result, and rename column back if flush is not possible
        self.flush_schema();
        Ok(None)
    }

    fn add_table_column(&mut self, table_name: SqlValue, column_definition: ColumnDefinition) -> Result<Option<QueryResult>, ExecutionError> {
        let table_name_string = table_name.to_string();
        let table = match self.tables.get_mut(table_name_string.as_str()) {
            None => return Err(ExecutionError::TableNotExist(table_name_string)),
            Some(existing_table) => existing_table,
        };
        let mut new_column_definitions = table.column_definitions();
        let table_column_types = table.column_types.clone();
        new_column_definitions.push(column_definition);
        let temp_table_name = SqlValue::String(format!("{}-{}", table_name, Self::get_timestamp()));
        self.create_table(temp_table_name.clone(), new_column_definitions)?;

        match self.move_records_to_new_table_and_swap_tables(table_name.clone(), temp_table_name.clone(), &table_column_types) {
            Ok(result) => Ok(result),
            Err(move_error) => {
                self.drop_table(temp_table_name.clone())
                    .unwrap_or_else(|_| panic!("error selecting from table {}, \
                                      and was unable to cleanup temporary table {}, consider dropping in manually",
                                      table_name, temp_table_name));
                Err(move_error)
            }
        }
    }

    fn move_records_to_new_table_and_swap_tables(&mut self, table_name: SqlValue, temp_table_name: SqlValue, table_column_types: &[ColumnType]) -> Result<Option<QueryResult>, ExecutionError> {
        let temp_table_name_string = temp_table_name.to_string();
        let all_rows_query_option = self.select_rows(table_name.clone(), vec![SelectColumnName::AllColumns], None)?;
        let new_table = match self.tables.get_mut(temp_table_name_string.as_str()) {
            None => return Err(ExecutionError::TableNotExist(temp_table_name_string)),
            Some(existing_table) => existing_table,
        };

        if let Some(all_rows_query) = all_rows_query_option {
            for row in all_rows_query.rows {
                let mut sql_values = row.get_sql_values(table_column_types)?;
                sql_values.push(SqlValue::Null);
                new_table.insert(None, sql_values)?;
            }
        }

        self.swap_tables_and_drop_old_table(table_name, temp_table_name)
    }

    fn swap_tables_and_drop_old_table(&mut self, table_name: SqlValue, temp_table_name: SqlValue) -> Result<Option<QueryResult>, ExecutionError> {
        let old_table_temp_name = SqlValue::String(format!("{}-{}", table_name.to_string(), Self::get_timestamp()));
        self.rename_table(table_name.clone(), old_table_temp_name.clone())?;

        match self.rename_table(temp_table_name.clone(), table_name.clone()) {
            Ok(_) => {
                match self.drop_old_table_and_flush_schema(old_table_temp_name) {
                    Ok(result) => Ok(result),
                    Err(error) => {
                        self.rename_table(table_name.clone(), temp_table_name.clone()).
                            unwrap_or_else(|_| panic!(
                                    "failed to rename {} back to {}, also new table {} needs to be cleaned up manually",
                                    table_name, temp_table_name, temp_table_name.clone()) // TODO: Adjust names
                              );
                        Err(error)
                    }
                }
            },
            Err(error) => {
                self.rename_table(old_table_temp_name.clone(), table_name.clone()).
                    unwrap_or_else(|_| panic!("failed to rename {} back to {}, \
                                              also new table {} needs to be cleaned up manually",
                                              old_table_temp_name, table_name, temp_table_name));
                Err(error)
            }
        }
    }

    fn drop_old_table_and_flush_schema(&mut self, old_table_name: SqlValue) -> Result<Option<QueryResult>, ExecutionError> {
        self.drop_table(old_table_name)?;
        self.flush_schema(); //TODO: full rollback on flush error
        Ok(None)
    }

    fn table_filepath(tables_dir: &Path, table_name: &str) -> PathBuf {
        let mut path = tables_dir.join(table_name);
        path.set_extension(TABLE_EXTENSION);
        path
    }

    fn get_timestamp() -> u128 {
        time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }
}
