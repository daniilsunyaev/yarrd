use crate::table::ColumnType;
use crate::lexer::SqlValue;

pub enum MetaCommand {
    Exit,
}

#[derive(Debug)]
pub enum Command {
    InsertInto {
        table_name: SqlValue,
        column_names: Option<Vec<SqlValue>>,
        values: Vec<SqlValue>,
    },
    Select {
        table_name: SqlValue,
        column_names: Vec<SelectColumnName>,
    },
    // Update,
    // Delete,
    CreateTable {
        table_name: SqlValue,
        columns: Vec<ColumnDefinition>,
    },
    DropTable {
        table_name: SqlValue,
    }
}

#[derive(Debug)]
pub enum SelectColumnName {
    Name(SqlValue),
    AllColumns
}

#[derive(Debug)]
pub struct ColumnDefinition {
    pub name: SqlValue,
    pub kind: ColumnType, // TODO: maybe use token instead, transition to sematic types should be on exec stage?
}
