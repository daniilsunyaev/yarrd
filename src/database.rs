use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::command::{Command, ColumnDefinition, FieldAssignment, SelectColumnName};
use crate::binary_condition::BinaryCondition;
use crate::lexer::SqlValue;
use crate::table::{Table, ColumnType, Constraint};
use crate::execution_error::ExecutionError;
use crate::meta_command_error::MetaCommandError;
use crate::query_result::QueryResult;
use crate::helpers::get_timestamp;
use crate::parser;

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
            tables.insert(table.name().to_string(), table);
        }

        Ok(Self { tables, database_filepath: PathBuf::from(database_filepath), tables_dir })
    }

    pub fn create(database_filepath: &Path, tables_dir_path: &Path) -> Result<(), MetaCommandError> {
        let tables_dir = PathBuf::from(tables_dir_path);
        let database_filepath = PathBuf::from(database_filepath);

        if database_filepath.exists() {
            return Err(MetaCommandError::DatabaseFileAlreadyExist(database_filepath));
        }

        let mut database_file = File::create(database_filepath.clone())?;

        if !tables_dir.exists() {
            if let Err(create_tables_dir_error) = fs::create_dir(tables_dir.clone()) {
                fs::remove_file(database_filepath.as_path())
                    .unwrap_or_else(|_| panic!(
                            "failed to create tables dir: {}, failed to remove database file '{}', try to remove it manually",
                            create_tables_dir_error, database_filepath.to_str().unwrap()
                            ));
                return Err(MetaCommandError::IoError(create_tables_dir_error))
            }
        }

        writeln!(database_file, "{}", tables_dir.display())?;
        // ideally we should check if it is succesfull, should handle in "cascade" file
        // manager

        Ok(())
    }

    pub fn drop(database_filepath: &Path) -> Result<(), MetaCommandError> {
        let mut database = Self::from(database_filepath)?;
        let mut table_names = vec![];

        for table_name in database.tables.keys() {
            table_names.push(SqlValue::Identificator(table_name.to_string()));
        }
        for table_name in table_names {
            database.drop_table(table_name).map_err(MetaCommandError::ExecutionError)?;
        }

        // TODO: use cascade file manager to panic from unrecoverable errors with correct message
        fs::remove_file(database_filepath).map_err(MetaCommandError::IoError)?;
        Ok(())
    }

    pub fn close(self) {
        self.flush_schema();
    }

    pub fn parse_schema_line(tables_dir: &Path, table_definition_line: &str) -> Result<Table, MetaCommandError> {
        let parser::TableSchemaDefinitionLine { name, row_count, column_definitions, indexes_definitions } =
            parser::parse_schema_line(table_definition_line)
            .map_err(|parser_error| MetaCommandError::ParseError(parser_error.to_string()))?;

        let table_filepath = Self::table_filepath(tables_dir, &name);

        Ok(Table::new(table_filepath, &name, row_count, column_definitions, indexes_definitions)?)
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
            write!(database_file, " {}", table.row_count).unwrap();
            for i in 0..table.column_types().len() {
                write!(database_file, " {} {}", table.column_names()[i], table.column_types()[i]).unwrap();
                for constraint in &table.column_constraints()[i] {
                    write!(database_file, " {}", constraint).unwrap();
                }

                if i < table.column_types().len() - 1 {
                    write!(database_file, ",").unwrap();
                }
            }

            write!(database_file, ";").unwrap();

            let indexes: Vec<_> =
                table.column_indexes().iter().enumerate()
                .filter(|(_i, index_option)| index_option.is_some())
                .map(|(i, index_option)| (i, index_option.as_ref().unwrap()))
                .collect();

            for index_number in 0..indexes.len() {
                let (column_number, index_ref) = indexes[index_number];
                write!(database_file, " {} {}", column_number, index_ref.name).unwrap();
                if index_number < indexes.len() - 1 {
                    write!(database_file, ",").unwrap();
                }
            }

            write!(database_file, ";").unwrap();
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
            Command::AddColumnConstraint { table_name, column_name, constraint } =>
                self.add_table_column_constraint(table_name, column_name, constraint),
            Command::DropColumnConstraint { table_name, column_name, constraint } =>
                self.drop_table_column_constraint(table_name, column_name, constraint),
            Command::DropTableColumn { table_name, column_name } => self.drop_table_column(table_name, column_name),
            Command::CreateIndex { table_name, index_name, column_name } => self.create_table_index(index_name, table_name, column_name),
            Command::DropIndex { table_name, index_name } => self.drop_table_index(index_name, table_name),
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
        match Table::new(table_filepath.clone(), table_name_string.as_str(), 0, columns, vec![]) {
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
            Some(table) => {
                table.destroy()?;
                self.flush_schema();
                Ok(None)
            },
        }
    }

    fn select_rows(&mut self, table_name: SqlValue, column_names: Vec<SelectColumnName>, where_clause: Option<BinaryCondition>) -> Result<Option<QueryResult>, ExecutionError> {
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

    fn update_rows(&mut self, table_name: SqlValue, field_assignments: Vec<FieldAssignment>, where_clause: Option<BinaryCondition>) -> Result<Option<QueryResult>, ExecutionError> {
        let table = self.get_table(&table_name)?;
        table.update(field_assignments, where_clause)?;
        Ok(None)
    }

    fn delete_rows(&mut self, table_name: SqlValue, where_clause: Option<BinaryCondition>) -> Result<Option<QueryResult>, ExecutionError> {
        let table = self.get_table(&table_name)?;

        table.delete(where_clause)?;
        Ok(None)
    }

    fn rename_table(&mut self, table_name: SqlValue, new_table_name: SqlValue) -> Result<Option<QueryResult>, ExecutionError> {
        let table_name_string = table_name.to_string();
        let new_table_name_string = new_table_name.to_string();
        let new_table_filepath = Self::table_filepath(self.tables_dir.as_path(), new_table_name_string.as_str());

        let mut table = match self.tables.remove(table_name_string.as_str()) {
            None => return Err(ExecutionError::TableNotExist(table_name_string)),
            Some(table) => table,
        };
        match table.rename(&new_table_name_string, new_table_filepath.as_path()) {
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

    fn add_table_column_constraint(&mut self, table_name: SqlValue, column_name: SqlValue, constraint: Constraint) -> Result<Option<QueryResult>, ExecutionError> {
        let column_name_string = column_name.to_string();

        let table = self.get_table(&table_name)?;
        table.add_column_constraint(column_name_string, constraint)?;

        // TODO: use result, and rename column back if flush is not possible
        self.flush_schema();
        Ok(None)
    }

    fn drop_table_column_constraint(&mut self, table_name: SqlValue, column_name: SqlValue, constraint: Constraint) -> Result<Option<QueryResult>, ExecutionError> {
        let column_name_string = column_name.to_string();

        let table = self.get_table(&table_name)?;
        table.drop_column_constraint(column_name_string, constraint)?;

        // TODO: use result, and rename column back if flush is not possible
        self.flush_schema();
        Ok(None)
    }

    fn add_table_column(&mut self, table_name: SqlValue, column_definition: ColumnDefinition) -> Result<Option<QueryResult>, ExecutionError> {
        let table = self.get_table(&table_name)?;
        let mut new_column_definitions = table.column_definitions();
        let table_column_types = table.column_types().to_vec();
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

    fn create_table_index(&mut self, index_name: SqlValue, table_name: SqlValue, column_name: SqlValue) -> Result<Option<QueryResult>, ExecutionError> {
        let tables_dir = self.tables_dir.clone();
        let table = self.get_table(&table_name)?;
        let column_name_string = column_name.to_string();
        let index_name_string = index_name.to_string();
        table.create_index(&column_name_string, index_name_string, tables_dir.as_path())?;
        Ok(None)
    }

    fn drop_table_index(&mut self, index_name: SqlValue, table_name: SqlValue) -> Result<Option<QueryResult>, ExecutionError> {
        let table = self.get_table(&table_name)?;
        let index_name_string = index_name.to_string();
        table.drop_index_by_name(index_name_string)?;
        Ok(None)
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
                            and replace with {}",
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
        let droped_column_index = table.column_number_result(column_name.to_string().as_str())?;
        let mut new_column_definitions = table.column_definitions();
        let table_column_types = table.column_types().to_vec();
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
