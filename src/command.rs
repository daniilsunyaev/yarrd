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
    // Select,
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
pub struct ColumnDefinition {
    pub name: SqlValue,
    pub kind: ColumnType,
}
