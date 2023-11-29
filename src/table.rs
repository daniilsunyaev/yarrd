use std::fmt;
use std::path::{Path, PathBuf};
use std::iter::zip;

use crate::command::{ColumnDefinition, FieldAssignment, SelectColumnName};
use crate::binary_condition::BinaryCondition;
use crate::lexer::SqlValue;
use crate::row::Row;
use crate::query_result::QueryResult;
use crate::pager::Pager;
use crate::row_check::RowCheck;
use crate::hash_index::HashIndex;
use crate::hash_index::error::HashIndexError;
use error::TableError;

pub mod error;

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

#[derive(Debug, Clone, PartialEq)]
pub enum Constraint {
    NotNull,
    Default(SqlValue),
    Check(BinaryCondition),
}

impl fmt::Display for Constraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::NotNull => write!(f, "NOT NULL"),
            Self::Default(value) => write!(f, "DEFAULT {}", value),
            Self::Check(row_check) => write!(f, "CHECK ({})", row_check),
        }
    }
}

struct ScanProduct {
    row_id: u64,
    row: Row,
}

#[derive(Debug)]
struct TableHeaders {
    pub name: String,
    pub column_types: Vec<ColumnType>,
    pub column_names: Vec<String>,
    pub column_constraints: Vec<Vec<Constraint>>,
    pub defaults: Vec<SqlValue>,
    checks: Vec<RowCheck>,
}

#[derive(Debug)]
pub struct Table {
    pub row_count: usize, // this should go to metadata if we'll introduce more stats
    headers: TableHeaders,
    pager: Pager,
    // eventually index type will be boxed, bus as it looks like I won't have time to implement
    // B-Tree, inverted, or any other type of index soon, I'm leaving straight index class inside
    // Option
    column_indexes: Vec<Option<HashIndex>>,
}

impl Table {
    pub fn new(table_filepath: PathBuf, name: &str, row_count: usize,
               column_definitions: Vec<ColumnDefinition>, indexes_definitions: Vec<(usize, String)>)
        -> Result<Table, TableError> {

        let tables_dir = table_filepath.parent().unwrap();

        let mut column_names = vec![];
        let mut column_types = vec![];
        let mut column_constraints = vec![vec![]; column_definitions.len()];
        let mut defaults = vec![SqlValue::Null; column_definitions.len()];
        let mut column_indexes = vec![];
        column_indexes.reserve(column_definitions.len());
        for _ in 0..column_definitions.len() {
            column_indexes.push(None);
        } // we have to do this explicitly to avoid implementing Clone trait on hash index
        for (column_number, index_name) in indexes_definitions {
            column_indexes[column_number] =
                Some(HashIndex::new(tables_dir, name, index_name, column_number)?);
        }

        for (i, column_definition) in column_definitions.into_iter().enumerate() {
            column_names.push(column_definition.name.to_string());
            column_types.push(column_definition.kind);

            for constraint in column_definition.column_constraints {
                match constraint {
                    Constraint::Default(value) => {
                        if defaults[i] != SqlValue::Null {
                            return Err(TableError::ConstraintAlreadyExists {
                                table_name: name.to_string(),
                                column_name: column_names[i].clone(),
                                constraint: Constraint::Default(defaults[i].clone())
                            })
                        } else {
                          defaults[i] = value;
                        }
                    },
                    _ => column_constraints[i].push(constraint),
                }
            }
        }
        let row_size = Row::calculate_row_size(&column_types);
        let pager = Pager::new(table_filepath.as_path(), row_size)
            .map_err(TableError::CreateError)?;
        let headers = TableHeaders {
            name: name.to_string(),
            checks: vec![],
            column_types,
            column_names,
            column_constraints,
            defaults,
        };

        let mut table = Self { pager, headers, column_indexes, row_count };
        table.compile_checks()?;

        Ok(table)
    }

    pub fn column_types(&self) -> &[ColumnType] {
        &self.headers.column_types
    }

    pub fn column_names(&self) -> &[String] {
        &self.headers.column_names
    }

    pub fn name(&self) -> &str {
        &self.headers.name
    }

    pub fn column_indexes(&self) -> &[Option<HashIndex>] {
        &self.column_indexes
    }

    pub fn set_name(&mut self, name: &str) {
        self.headers.name = name.to_string();
    }

    pub fn column_constraints(&self) -> &[Vec<Constraint>] {
        &self.headers.column_constraints
    }

    pub fn defaults(&self) -> &[SqlValue] {
        &self.headers.defaults
    }

    pub fn select(&mut self, select_column_names: Vec<SelectColumnName>, where_clause: Option<BinaryCondition>) -> Result<QueryResult, TableError> {
        let mut result_column_names = vec![];
        let mut result_column_types = vec![];
        let mut result_column_numbers = vec![];

        for select_column_name in &select_column_names {
            match select_column_name {
                SelectColumnName::Name(column_name) => {
                    let column_name = column_name.to_string();
                    let column_number = self.column_number_result(&column_name)?;
                    let column_type = *self.column_types().get(column_number)
                        .ok_or(TableError::ColumnNthNotExist { column_number, table_name: self.name().to_string() })?;
                    result_column_names.push(column_name);
                    result_column_types.push(column_type);
                    result_column_numbers.push(column_number);
                },
                SelectColumnName::AllColumns => {
                    result_column_names.extend_from_slice(self.column_names());
                    result_column_types.extend_from_slice(self.column_types());
                    for i in 0..self.column_types().len() { result_column_numbers.push(i) };
                }
            }
        }

        let mut result = QueryResult { column_names: result_column_names, column_types: result_column_types.clone(), rows: vec![] };

        for scan_result in Self::matching_rows(&mut self.pager, &mut self.column_indexes, &self.headers, where_clause)? {
            let row = scan_result?.row;
            let result_row = result.spawn_row();

            for (i, column_number) in result_column_numbers.iter().enumerate() {
                let column_values_data = row.get_cell_bytes(&self.headers.column_types, *column_number);
                let column_is_null = row.cell_is_null(*column_number);
                result_row.set_cell_bytes(&result_column_types, i, column_values_data, column_is_null)
                    .map_err(TableError::CannotSetCell)?
            }
        }

        Ok(result)
    }

    pub fn insert(&mut self, column_names: Option<Vec<String>>, values: Vec<SqlValue>) -> Result<(), TableError> {
        let column_names = match &column_names {
            Some(column_names) => column_names,
            None => self.column_names(),
        };

        let input_column_numbers = self.get_columns_numbers(column_names)?;
        self.validate_values_type(&values, &input_column_numbers)?;

        let (result_values, _numbers) = self.apply_defaults(&values, &input_column_numbers);

        let row = Row::from_sql_values(&result_values, self.column_types())
            .map_err(TableError::CannotGetCell)?;

        Self::validate_constraints(&self.headers, &row)?;

        // TODO: this should be rollbackable if index update fails
        let row_id = self.pager.insert_row(row).map_err(TableError::CannotInsertRow)?;
        self.row_count += 1;
        self.update_indexes_on_insert(&input_column_numbers, &result_values, row_id)
    }

    pub fn update(&mut self, field_assignments: Vec<FieldAssignment>, where_clause: Option<BinaryCondition>) -> Result<(), TableError> {
        let (column_names, column_values): (Vec<String>, Vec<SqlValue>) = field_assignments.into_iter()
            .map(|assignment| (assignment.column_name, assignment.value))
            .unzip();

        let column_numbers = self.get_columns_numbers(&column_names)?;
        self.validate_values_type(&column_values, &column_numbers)?;
        let pager_raw: *mut Pager = &mut self.pager;

        let matching_rows = Self::matching_rows(&mut self.pager, &self.column_indexes, &self.headers, where_clause)?;
        let updation_error = matching_rows
            .map(|scan_result| {
                let mut scan_product = scan_result?;

                let mut old_column_values = vec![];

                for (column_number, column_value) in column_values.iter().enumerate() {
                    let column_table_number = column_numbers[column_number];
                    old_column_values
                        .push(scan_product.row.get_cell_sql_value(&self.headers.column_types, column_table_number).map_err(TableError::CannotGetCell)?);
                    scan_product.row.set_cell(&self.headers.column_types, column_table_number, column_value)
                        .map_err(TableError::CannotSetCell)?;

                }

                Self::validate_constraints(&self.headers, &scan_product.row)?;

                Self::update_indexes_on_update(&self.column_indexes, scan_product.row_id, &column_numbers, &old_column_values, &column_values)?;

                // pager will not reallocate to a new space during matching_rows iteration
                // so we can safely dereference raw mut pointer
                // TODO: check if we can move pager_raw and give it back, i.e.
                // pager_raw = paer_raw.update_row(...) or by using RefCell
                unsafe {
                    (*pager_raw)
                        .update_row(scan_product.row_id, &scan_product.row)
                        .map_err(TableError::CannotUpdateRow)
                }
            })
            .skip_while(|updation_result: &Result<u64, TableError>| updation_result.is_ok())
            .next();

        match updation_error {
            None => Ok(()),
            Some(error) => Err(error.unwrap_err()),
        }
    }

    pub fn delete(&mut self, where_clause: Option<BinaryCondition>) -> Result<(), TableError> {
        let pager_raw: *mut Pager = &mut self.pager;
        let mut column_values = vec![];

        Self::matching_rows(&mut self.pager, &self.column_indexes, &self.headers, where_clause)?
            .map(|scan_result| {
                let scan_product = scan_result?;
                for column_number in 0..self.headers.column_types.len() {
                    column_values
                        .push(
                            scan_product
                            .row
                            .get_cell_sql_value(&self.headers.column_types, column_number)
                            .map_err(TableError::CannotGetCell)?
                        );
                }

                let row_number = scan_product.row_id;
                Self::update_indexes_on_delete(&self.column_indexes, row_number, &column_values)?;
                // pager will not reallocate to a new space during matching_rows iteration
                // so we can safely dereference raw mut pointer
                unsafe {
                    (*pager_raw).delete_row(row_number).map_err(TableError::CannotDeleteRow)?;
                }
                self.row_count -= 1;
                Ok::<(), TableError>(())
        })
        .for_each(drop);

        Ok(())
    }

    pub fn rename_column(&mut self, column_name: String, new_column_name: String) -> Result<(), TableError> {
        let column_number = self.column_number_result(column_name.as_str())?;

        self.headers.column_names[column_number] = new_column_name;
        Ok(())
    }

    pub fn add_column_constraint(&mut self, column_name: String, constraint: Constraint) -> Result<(), TableError> {
        let column_number = self.column_number_result(column_name.as_str())?;
        let column_constraints = &mut self.headers.column_constraints[column_number];

        if column_constraints.contains(&constraint) {
            return Err(TableError::ConstraintAlreadyExists { table_name: self.name().to_string(), column_name, constraint })
        }

        column_constraints.push(constraint);
        self.compile_checks()?;

        Ok(())
    }

    pub fn drop_column_constraint(&mut self, column_name: String, constraint: Constraint) -> Result<(), TableError> {
        let column_number = self.column_number_result(column_name.as_str())?;
        let column_constraints = &mut self.headers.column_constraints[column_number];

        match column_constraints.iter().position(|existing_constraint| *existing_constraint == constraint) {
            None => {
                return Err(TableError::ConstraintNotExists {
                    table_name: self.name().to_string(),
                    column_name,
                    constraint,
                })
            },
            Some(index) => {
                column_constraints.swap_remove(index);
            },
        }
        self.compile_checks()?;

        Ok(())
    }

    pub fn create_index(&mut self, column_name: &str, index_name: String, tables_dir: &Path) -> Result<(), TableError> {
        let column_number = self.column_number_result(column_name)?;
        if matches!(self.column_types()[column_number], ColumnType::Float) {
            return Err(HashIndexError::FloatIndexError(column_name.to_string()).into())
        }

        if let Some(index) = &self.column_indexes[column_number] {
            return Err(TableError::IndexAlreadyExists {
                table_name: self.name().to_string(),
                column_name: column_name.to_string(),
                index_name: index.name().to_string(),
            })
        }

        let index = HashIndex::new(tables_dir, self.name(), index_name, column_number)?;
        self.column_indexes[column_number] = Some(index);
        self.reindex_column(column_number)
    }

    fn update_indexes_on_insert(&mut self, input_column_numbers: &[usize], result_values: &Vec<SqlValue>, row_id: u64) -> Result<(), TableError> {
        for (column_number, value) in zip(input_column_numbers, result_values) {
            match &mut self.column_indexes[*column_number] {
                Some(hash_index) => hash_index.insert_row(value, row_id, self.row_count)?,
                None => {},
            }
        }

        Ok(())
    }

    fn update_indexes_on_update(column_indexes: &[Option<HashIndex>], row_id: u64, input_column_numbers: &[usize],
                                old_column_values: &Vec<SqlValue>, new_column_values: &Vec<SqlValue>)
        -> Result<(), TableError> {

        for (index, (old_value, new_value)) in zip(input_column_numbers, zip(old_column_values, new_column_values)) {
            match &column_indexes[*index] {
                Some(hash_index) => hash_index.update_row(row_id, old_value, new_value)?,
                None => {},
            }
        }

        Ok(())
    }

    fn update_indexes_on_delete(column_indexes: &[Option<HashIndex>], row_id: u64, column_values: &[SqlValue]) -> Result<(), TableError> {
        for (column_index, value) in zip(column_indexes, column_values) {
            match column_index {
                Some(hash_index) => hash_index.delete_row(row_id, value)?,
                None => {},
            }
        }

        Ok(())
    }

    pub fn column_definitions(&self) -> Vec<ColumnDefinition> {
        self.column_names().iter().enumerate().zip(self.column_types().iter())
            .map(|((i, name), kind)| {
                ColumnDefinition {
                    name: SqlValue::String(name.clone()),
                    kind: *kind,
                    column_constraints: self.column_constraints()[i].clone(),
                }
            })
            .collect()
    }

    pub fn vacuum(&mut self) -> Result<(), TableError> {
        self.pager.vacuum().map_err(TableError::VacuumFailed)?;
        self.reindex()
    }

    fn reindex(&mut self) -> Result<(), TableError> {
        //let mut enumerated_column_indexes: Vec<(usize, &mut HashIndex)> = self.column_indexes
        //    .iter_mut()
        //    .enumerate()
        //    .filter(|(_i, column_index_option)| column_index_option.is_some())
        //    .map(|(i, column_index_option)| (i, column_index_option.as_mut().unwrap()))
        //    .collect();

        self.reindex_columns((0..self.column_indexes.len()).collect())
    }

    fn reindex_column(&mut self, column_number: usize) -> Result<(), TableError> {
        //let table_name = self.name().to_string();
        //let column_index = self.column_indexes[column_number]
        //    .as_mut()
        //    .ok_or(TableError::ColumnNotExist {
        //        table_name,
        //        column_name: self.headers.column_names[column_number].clone(),
        //    })?;

        self.reindex_columns(vec![column_number])
    }

    fn reindex_columns(&mut self, column_numbers: Vec<usize>) -> Result<(), TableError> {
        //let mut enumerated_column_indexes: Vec<_> = column_numbers
        //    .into_iter()
        //    .map(|column_number| (column_number, self.column_indexes[column_number].as_mut()))
        //    .filter(|(_i, column_index)| column_index.is_some())
        //    .map(|(i, column_index_option)| (i, column_index_option.unwrap()))
        //    .collect();

        //enumerated_column_indexes
        //    .iter_mut()
        //    .map(|(_i, column_index)| Ok(column_index.clear().map_err(TableError::HashIndexError)?))
        //    .collect::<Result<(),TableError>>()?;
        //    //.map(|(i, column_index)| (i, column_index.clear().map_err(TableError::HashIndexError)?))
        //    //.collect::<Result<Vec<(usize, &mut HashIndex)>, TableError>>()?;
        let mut indexed_column_numbers = vec![];

        for column_number in column_numbers {
            if let Some(index) = self.column_indexes[column_number].as_mut() {
                index.clear()?;
                indexed_column_numbers.push(column_number);
            }
        }

        Self::seq_scan(&mut self.pager)
            .map(|scan_result| {
                let scan_product = scan_result?;
                for column_number in &indexed_column_numbers {
                    let column_index = self.column_indexes[*column_number].as_mut().unwrap();
                    let value = scan_product
                        .row
                        .get_cell_sql_value(&self.headers.column_types, *column_number)
                        .map_err(TableError::CannotGetCell)?;

                    column_index.insert_row(&value, scan_product.row_id, self.row_count)
                        .map_err(TableError::HashIndexError)?;
                }

                Ok(())
                //enumerated_column_indexes
                //    .iter_mut()
                //    .map(|(column_number, column_index)| {
                //        let value = scan_product
                //            .row
                //            .get_cell_sql_value(&self.headers.column_types, *column_number)
                //            .map_err(TableError::CannotGetCell)?;

                //        column_index.insert_row(&value, scan_product.row_id, self.row_count)
                //            .map_err(TableError::HashIndexError)
                //    })
                //.collect::<Result<(), TableError>>()
            })
            .collect::<Result<(), TableError>>()
    }

    fn matching_rows<'a>(pager: &'a mut Pager, column_indexes: &'a Vec<Option<HashIndex>>,
                         table_headers: &'a TableHeaders, where_clause: Option<BinaryCondition>)
        -> Result<impl Iterator<Item = Result<ScanProduct, TableError>> + 'a, TableError> {

        let where_filter = match where_clause {
            None => RowCheck::dummy(),
            Some(where_clause) => where_clause.compile(&table_headers.name, &table_headers.column_names)?,
        };

        let base_query_iter = Self::plan_query(pager, column_indexes, &where_filter);

        let filter_closure = {
            let column_types = &table_headers.column_types;

            move |scan_result: Result<ScanProduct, TableError>| {
                match scan_result {
                    Ok(scan_product) =>
                        match where_filter.matches(&scan_product.row, column_types) {
                            Ok(true) => Some(Ok(scan_product)),
                            Ok(false) => None,
                            Err(error) => Some(Err(error)),
                        }
                    Err(error) => Some(Err(error)),
                }
            }
        };

        Ok(base_query_iter.filter_map(filter_closure))
    }

    fn plan_query<'a, 'b>(pager: &'a mut Pager, column_indexes: &'a Vec<Option<HashIndex>>, where_filter: &'b RowCheck)
        -> Box<dyn Iterator<Item = Result<ScanProduct, TableError>> + 'a> {

        if let Some((column_number, value)) = where_filter.is_column_value_eq_static_check() {
            if let Some(ref column_index) = column_indexes[column_number] {
                return Self::index_scan(pager, column_index, value)
            }
        }

        Self::seq_scan(pager)
    }

    fn seq_scan(pager: &mut Pager) -> Box<dyn Iterator<Item = Result<ScanProduct, TableError>> + '_> {
        let max_rows = pager.max_rows();

        Box::new(
            (0..max_rows)
            .map(|row_number| (row_number, pager.get_row(row_number)))
            .filter(|(_, get_row_result)| get_row_result.is_err() || get_row_result.as_ref().unwrap().is_some())
            .map(|(row_number, get_row_result)| {
                match get_row_result {
                    Err(error) => Err(TableError::CannotGetRow(error)),
                    Ok(Some(row)) => Ok(ScanProduct { row_id: row_number, row: row }),
                    Ok(None) => panic!("unexpected error: scan result cannot be none, this should be filtered out"),
                }
            })
        )
    }

    fn index_scan<'a>(pager: &'a mut Pager, column_index: &'a HashIndex, value: SqlValue)
        -> Box<dyn Iterator<Item = Result<ScanProduct, TableError>> + 'a> {

            Box::new(
                column_index
                .find_row_ids(&value)
                .map(|row_number_result| {
                    let row_number = row_number_result?;
                    let row = pager.get_row(row_number).map_err(TableError::CannotGetRow)?.unwrap();
                    // if this is None, row_number points to a blank row, and index has invalid data
                    // TODO: we probably can reindex to recover from this error
                    Ok(ScanProduct {
                        row_id: row_number,
                        row: row,
                    })
                })
            )
    }

    fn compile_checks(&mut self) -> Result<(), TableError> {
        self.headers.checks.clear();
        for column_constraints in self.headers.column_constraints.iter() {
            for constraint in column_constraints {
                match constraint {
                    Constraint::Check(binary_condition) => {
                        let check_condition = binary_condition.clone();
                        self.headers.checks.push(check_condition.compile(&self.headers.name, &self.headers.column_names)?);
                    },
                    _ => continue,
                }
            }
        }
        Ok(())
    }

    fn get_columns_numbers(&self, column_names: &[String]) -> Result<Vec<usize>, TableError> {
        let mut column_numbers = Vec::new();
        for column_name in column_names {
            column_numbers.push(
                self.column_number_result(column_name)?
            );
        }

        Ok(column_numbers)
    }

    fn validate_constraints(table_headers: &TableHeaders, row: &Row) -> Result<(), TableError> {
        for (column_number, column_constraints) in table_headers.column_constraints.iter().enumerate() {
            for constraint in column_constraints {
                match Self::validate_row_over_constraint(row, constraint, column_number, &table_headers.column_types) {
                    true => continue,
                    false => return Err(
                        TableError::ColumnConstraintViolation {
                            table_name: table_headers.name.to_string(),
                            column_name: table_headers.column_names[column_number].clone(),
                            constraint: constraint.clone(),
                            value: row.get_cell_sql_value(&table_headers.column_types, column_number).unwrap(),
                        }),
                }
            }
        }

        for check in table_headers.checks.iter() {
            match check.matches(row, &table_headers.column_types)? {
                true => continue,
                false => return Err(
                    TableError::CheckViolation {
                        table_name: table_headers.name.to_string(),
                        row_check: check.clone(),
                        row: row.clone(),
                    }),
            }
        }

        Ok(())
    }


    fn apply_defaults(&self, values: &[SqlValue], column_numbers: &[usize]) -> (Vec<SqlValue>, Vec<usize>) {
        let result_column_numbers: Vec<usize> = (0..self.column_types().len()).collect();
        let mut result_values = self.defaults().to_vec();

        for (value, column_number) in values.iter().zip(column_numbers.iter()) {
            result_values[*column_number] = value.clone();
        }

        (result_values, result_column_numbers)
    }

    // TODO: remove column types after implementation of all constriants
    fn validate_row_over_constraint(row: &Row, constraint: &Constraint, column_number: usize, _column_types: &[ColumnType]) -> bool {
        match constraint {
            Constraint::NotNull => !row.cell_is_null(column_number),
            Constraint::Default(_) => { true },
            Constraint::Check(_) => { true },
        }
    }

    fn validate_values_type(&self, columns_values: &[SqlValue], column_numbers: &[usize]) -> Result<(), TableError> {
        for (value_index, value) in columns_values.iter().enumerate() {
            let column_number = column_numbers[value_index];

            if !self.column_types()[column_number].matches_value(value) {
                return Err(TableError::ValueColumnMismatch {
                    value: value.clone(),
                    column_name: self.column_names()[column_number].clone(),
                    column_type: self.column_types()[column_number],
                });
            }
        }
        Ok(())
    }

    // TODO: add hashmap of name -> numbers to avoid names scanning
    // and pass hash ref to compile
    pub fn column_number(&self, column_name: &str) -> Option<usize> {
        self.column_names().iter()
            .position(|table_column_name| table_column_name.eq(column_name))
    }

    pub fn column_number_result(&self, column_name: &str) -> Result<usize, TableError> {
        self.column_number(column_name)
            .ok_or(TableError::ColumnNotExist { column_name: column_name.to_string(), table_name: self.name().to_string() })
    }
}
