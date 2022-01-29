use crate::command::{ColumnDefinition, FieldAssignment, SelectColumnName};
use crate::where_clause::WhereClause;
use crate::lexer::SqlValue;
use crate::row::Row;

#[derive(Debug, Clone)]
pub enum ColumnType {
    Integer,
    String
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
    column_names: Vec<String>,
    rows: Vec<Vec<SqlValue>>,
}


impl Table {
    // TODO: do we need result?
    pub fn new(name: String, column_definitions: Vec<ColumnDefinition>) -> Table {
        let mut column_names = vec![];
        let mut column_types = vec![];

        for column_definition in column_definitions {
            column_names.push(column_definition.name.to_string());
            column_types.push(column_definition.kind);
        }

        Self { name, column_types, column_names, rows: vec![] }
    }

    pub fn select(&self, column_names: Vec<SelectColumnName>, where_clause: Option<WhereClause>) -> Result<Vec<Row>, String> {
        let mut result_rows = vec![];

        let matching_rows_indices = self.get_matching_rows_indices(where_clause)?;

        for i in matching_rows_indices {
            let row = &self.rows[i];

            let mut column_values: Vec<SqlValue> = vec![];
            let mut column_types: Vec<ColumnType> = vec![];

            for select_column_name in &column_names {
                match select_column_name {
                    SelectColumnName::Name(column_name) => {
                        let column_index = self.column_index(&column_name.to_string())
                            .ok_or(format!("column {} does not exist", column_name))?;
                        let column_value = row.get(column_index)
                            .ok_or(format!("table {} does not have a column with index {}", self.name, column_index))?;
                        let column_type = self.column_types.get(column_index)
                            .ok_or(format!("table {} does not have a column with index {}", self.name, column_index))?;
                        column_values.push(column_value.clone());
                        column_types.push(column_type.clone());
                    },
                    SelectColumnName::AllColumns => {
                        let mut column_values_clone = row.clone();
                        let mut column_types_clone = self.column_types.clone();
                        column_values.append(&mut column_values_clone);
                        column_types.append(&mut column_types_clone);
                    },
                }
            }

            result_rows.push(Row { column_values, column_types });
        }

        Ok(result_rows)
    }

    pub fn insert(&mut self, column_names: Option<Vec<String>>, values: Vec<SqlValue>) -> Result<(), String> {
        let column_names = match &column_names {
            Some(column_names) => column_names,
            None => &self.column_names,
        };

        let column_indices = self.get_columns_indices(column_names)?;
        self.validate_values_type(&values, &column_indices)?;

        let mut row = vec![SqlValue::Null; self.column_types.len()];
        for (value_index, value) in values.into_iter().enumerate() {
            let column_index = column_indices[value_index];

            row[column_index] = value;
        }

        self.rows.push(row);

        Ok(())
    }

    pub fn update(&mut self, field_assignments: Vec<FieldAssignment>, where_clause: Option<WhereClause>) -> Result<(), String> {
        let (column_names, column_values): (Vec<String>, Vec<SqlValue>) = field_assignments.into_iter()
            .map(|assignment| (assignment.column_name, assignment.value))
            .unzip();

        let column_indices = self.get_columns_indices(&column_names)?;
        self.validate_values_type(&column_values, &column_indices)?;

        let update_rows_indices = self.get_matching_rows_indices(where_clause)?;

        for update_row_index in update_rows_indices {
            for (column_index, column_value) in column_values.iter().enumerate() {
                let column_index = column_indices[column_index];
                self.rows[update_row_index][column_index] = column_value.clone();
            }
        }

        Ok(())
    }

    fn get_matching_rows_indices(&self, where_clause: Option<WhereClause>) -> Result<Vec<usize>, String> {
        let mut rows_indices = vec![];
        let row_fits_where_clause = match &where_clause {
            None => Box::new(|_| Ok(true)),
            Some(where_clause) => where_clause.build_filter(self),
        };

        for i in 0..self.rows.len() {
            if !row_fits_where_clause(&self.rows[i])? { continue };
            rows_indices.push(i)
        }

        Ok(rows_indices)
    }

    fn get_columns_indices(&self, column_names: &[String]) -> Result<Vec<usize>, String> {
        let mut column_indices = Vec::new();
        for column_name in column_names {
            column_indices.push(
                self.column_index(column_name)
                    .ok_or(format!("column '{}' does not exist for table '{}'", column_name, self.name))?
            );
        }

        Ok(column_indices)
    }

    fn validate_values_type(&self, columns_values: &[SqlValue], column_indices: &[usize]) -> Result<(), String> {
        for (value_index, value) in columns_values.iter().enumerate() {
            let column_index = column_indices[value_index];

            if !self.column_types[column_index].matches_value(value) {
                return Err(format!("value {} is not acceptable for column {} which has type {:?}",
                                   value, self.column_names[column_index], self.column_types[column_index]));
            }
        }
        Ok(())
    }

    // TODO: add hashmap of name -> indices to avoid names scanning
    pub fn column_index(&self, column_name: &str) -> Option<usize> {
        self.column_names.iter()
            .position(|table_column_name| table_column_name.eq(column_name))
    }

    // fn page_number(row: &Row) -> Option<uint> {
    //     Some(row.id)
    // }
}
