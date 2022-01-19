use crate::command::ColumnDefinition;

#[derive(Debug)]
pub enum ColumnType {
    Integer,
    String
}

pub struct Table {
    table_name: String,
    column_types: Vec<ColumnType>,
    column_names: Vec<String>,
}

impl Table {
    // TODO: do we need result?
    pub fn new(table_name: String, column_definitions: Vec<ColumnDefinition>) -> Table {
        let mut column_names = vec![];
        let mut column_types = vec![];

        for column_definition in column_definitions {
            column_names.push(column_definition.name.to_string());
            column_types.push(column_definition.kind);
        }

        Self { table_name, column_types, column_names }
    }
}
