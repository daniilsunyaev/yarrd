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
use crate::helpers::get_timestamp;

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

    pub fn create(database_filepath: &Path, tables_dir_path: &Path) -> Result<(), MetaCommandError> {
        let tables_dir = PathBuf::from(tables_dir_path);
        let database_filepath = PathBuf::from(database_filepath);
        let mut need_to_create_tables_dir = true;

        if database_filepath.exists() {
            return Err(MetaCommandError::DatabaseFileAlreadyExist(database_filepath));
        } else if tables_dir.exists() {
            need_to_create_tables_dir = false;
        }

        let mut database_file = File::create(database_filepath.clone())?;

        if need_to_create_tables_dir {
            match fs::create_dir(tables_dir.clone()) {
                Err(create_tables_dir_error) => {
                    fs::remove_file(database_filepath.as_path())
                        .unwrap_or_else(|_| panic!(
                                "failed to create tables dir: {}, failed to remove database file '{}', try to remove it manually",
                                create_tables_dir_error, database_filepath.to_str().unwrap()
                            ));
                    return Err(MetaCommandError::IoError(create_tables_dir_error))

                },
                Ok(()) => { },
            }
        }

        write!(database_file, "{}\n", tables_dir.into_os_string().into_string().unwrap());
        // ideally we should check if it is succesfull, should handle in "cascade" file
        // manager

        Ok(())
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
            Command::DropTableColumn { table_name, column_name } => self.drop_table_column(table_name, column_name),
            Command::VacuumTable { table_name } => self.vacuum_table(&table_name),
            Command::Void => Ok(None),
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
                self.flush_schema();
                Ok(None)
            },
        }
    }

    fn select_rows(&mut self, table_name: SqlValue, column_names: Vec<SelectColumnName>, where_clause: Option<WhereClause>) -> Result<Option<QueryResult>, ExecutionError> {
        let table = self.get_table(&table_name)?;

        Ok(Some(table.select(column_names, where_clause)?))
    }

    fn insert_rows(&mut self, table_name: SqlValue, column_names: Option<Vec<SqlValue>>, values: Vec<SqlValue>) -> Result<Option<QueryResult>, ExecutionError> {
        let column_names = column_names
            .map(|sql_names|
                 sql_names.iter()
                     .map(|sql_name| sql_name.to_string()).collect()
                );

        let table = self.get_table(&table_name)?;
        table.insert(column_names, values)?;
        Ok(None)
    }

    fn update_rows(&mut self, table_name: SqlValue, field_assignments: Vec<FieldAssignment>, where_clause: Option<WhereClause>) -> Result<Option<QueryResult>, ExecutionError> {
        let table = self.get_table(&table_name)?;
        table.update(field_assignments, where_clause)?;
        Ok(None)
    }

    fn delete_rows(&mut self, table_name: SqlValue, where_clause: Option<WhereClause>) -> Result<Option<QueryResult>, ExecutionError> {
        let table = self.get_table(&table_name)?;

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
        let column_name_string = column_name.to_string();
        let new_column_name_string = new_column_name.to_string();

        let table = self.get_table(&table_name)?;
        table.rename_column(column_name_string, new_column_name_string)?;

        // TODO: use result, and rename column back if flush is not possible
        self.flush_schema();
        Ok(None)
    }

    fn add_table_column(&mut self, table_name: SqlValue, column_definition: ColumnDefinition) -> Result<Option<QueryResult>, ExecutionError> {
        let table = self.get_table(&table_name)?;
        let mut new_column_definitions = table.column_definitions();
        let table_column_types = table.column_types.clone();
        new_column_definitions.push(column_definition);
        let temp_new_table_name = Self::temporary_table_name(&table_name);
        self.create_table(temp_new_table_name.clone(), new_column_definitions)?;

        match self.move_extended_records_to_new_table_and_swap_tables(&table_name, &temp_new_table_name, &table_column_types) {
            Ok(result) => Ok(result),
            Err(move_error) => {
                self.drop_table(temp_new_table_name.clone())
                    .unwrap_or_else(|error| panic!("error selecting from table {}: {}, \
                                      and was unable to rollback: cleanup temporary table {} failed: {}, \
                                      consider dropping in manually",
                                      table_name, move_error, temp_new_table_name, error));
                Err(move_error)
            }
        }
    }

    fn move_extended_records_to_new_table_and_swap_tables(&mut self, target_table_name: &SqlValue, temp_new_table_name: &SqlValue,
                                                 table_column_types: &[ColumnType]) -> Result<Option<QueryResult>, ExecutionError> {
        let all_rows_query_option = self.select_rows(target_table_name.clone(), vec![SelectColumnName::AllColumns], None)?;
        let new_table = self.get_table(temp_new_table_name)?;

        if let Some(all_rows_query) = all_rows_query_option {
            for row in all_rows_query.rows {
                let mut sql_values = row.get_sql_values(table_column_types)?;
                sql_values.push(SqlValue::Null);
                new_table.insert(None, sql_values)?;
            }
        }

        self.swap_tables_and_drop_old_table(target_table_name, temp_new_table_name)
    }

    fn swap_tables_and_drop_old_table(&mut self, target_table_name: &SqlValue, temp_new_table_name: &SqlValue) -> Result<Option<QueryResult>, ExecutionError> {
        let temp_old_table_name = Self::temporary_table_name(target_table_name);
        self.rename_table(target_table_name.clone(), temp_old_table_name.clone())?;

        match self.rename_table(temp_new_table_name.clone(), target_table_name.clone()) {
            Ok(_) => self.try_drop_old_table_and_flush_schema(target_table_name, temp_new_table_name, &temp_old_table_name),
            Err(rename_error) => {
                self.rename_table(temp_old_table_name.clone(), target_table_name.clone()).
                    unwrap_or_else(|back_rename_error| panic!("failed to rename {} back to {}: {}, \
                                              and was not able to rollback: {}, \
                                              new table {} needs to be cleaned up manually, \
                                              and old table {0} needs to be renamed back to {1} manually",
                                              temp_old_table_name, target_table_name, rename_error,
                                              back_rename_error, temp_new_table_name));
                Err(rename_error)
            }
        }
    }

    fn try_drop_old_table_and_flush_schema(&mut self, target_table_name: &SqlValue, temp_new_table_name: &SqlValue,
                                           temp_old_table_name: &SqlValue) -> Result<Option<QueryResult>, ExecutionError> {
        match self.drop_old_table_and_flush_schema(temp_old_table_name) {
            Ok(result) => Ok(result),
            Err(drop_error) => {
                self.rename_table(target_table_name.clone(), temp_new_table_name.clone()).
                    unwrap_or_else(|rename_error| panic!(
                            "failed to rename {} back to {}: {}, \
                            and was not able to rollback: {},
                            new table {0} needs to be cleaned up manually, \
                            and replacec with {}",
                            target_table_name, temp_new_table_name, rename_error, drop_error, temp_old_table_name)
                          );
                Err(drop_error)
            }
        }
    }

    fn drop_old_table_and_flush_schema(&mut self, old_table_name: &SqlValue) -> Result<Option<QueryResult>, ExecutionError> {
        self.drop_table(old_table_name.clone())?;
        self.flush_schema(); //TODO: full rollback on flush error
        Ok(None)
    }

    fn drop_table_column(&mut self, table_name: SqlValue, column_name: SqlValue) -> Result<Option<QueryResult>, ExecutionError> {
        let table = self.get_table(&table_name)?;
        let droped_column_index = table.column_index_result(column_name.to_string().as_str())?;
        let mut new_column_definitions = table.column_definitions();
        let table_column_types = table.column_types.clone();
        new_column_definitions.remove(droped_column_index);
        let temp_new_table_name = Self::temporary_table_name(&table_name);
        self.create_table(temp_new_table_name.clone(), new_column_definitions)?;

        match self.move_shrinked_records_to_new_table_and_swap_tables(&table_name, &temp_new_table_name, &table_column_types, droped_column_index) {
            Ok(result) => Ok(result),
            Err(move_error) => {
                self.drop_table(temp_new_table_name.clone())
                    .unwrap_or_else(|error| panic!("error selecting from table {}: {}, \
                                      and was unable to rollback: cleanup temporary table {} failed: {}, \
                                      consider dropping in manually",
                                      table_name, move_error, temp_new_table_name, error));
                Err(move_error)
            }
        }
    }

    fn move_shrinked_records_to_new_table_and_swap_tables(&mut self, target_table_name: &SqlValue, temp_new_table_name: &SqlValue,
                                                 table_column_types: &[ColumnType], drop_index: usize) -> Result<Option<QueryResult>, ExecutionError> {
        let all_rows_query_option = self.select_rows(target_table_name.clone(), vec![SelectColumnName::AllColumns], None)?;
        let new_table = self.get_table(temp_new_table_name)?;

        if let Some(all_rows_query) = all_rows_query_option {
            for row in all_rows_query.rows {
                let mut sql_values = row.get_sql_values(table_column_types)?;
                sql_values.remove(drop_index);
                new_table.insert(None, sql_values)?;
            }
        }

        self.swap_tables_and_drop_old_table(target_table_name, temp_new_table_name)
    }

    fn vacuum_table(&mut self, table_name: &SqlValue) -> Result<Option<QueryResult>, ExecutionError> {
        let table = self.get_table(table_name)?;
        table.vacuum()?;
        Ok(None)
    }

    fn get_table(&mut self, table_name: &SqlValue) -> Result<&mut Table, ExecutionError> {
        let table_name_string = table_name.to_string();
        match self.tables.get_mut(table_name_string.as_str()) {
            None => Err(ExecutionError::TableNotExist(table_name_string)),
            Some(existing_table) => Ok(existing_table),
        }
    }

    fn table_filepath(tables_dir: &Path, table_name: &str) -> PathBuf {
        let mut path = tables_dir.join(table_name);
        path.set_extension(TABLE_EXTENSION);
        path
    }

    fn temporary_table_name(table_name: &SqlValue) -> SqlValue {
        SqlValue::String(format!("{}-{}", table_name, get_timestamp()))
    }
}
