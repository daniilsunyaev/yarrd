use crate::table::ColumnType;

pub enum MetaCommand {
    Exit,
}

#[derive(Debug)]
pub enum Command {
    // Insert,
    // Select,
    // Update,
    // Delete,
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDefinition>,
    },
    DropTable {
        table_name: String,
    }
}

#[derive(Debug)]
pub struct ColumnDefinition {
    pub name: String,
    pub kind: ColumnType,
}
