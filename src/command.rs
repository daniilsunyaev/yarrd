use crate::table::ColumnType;

#[derive(Debug)]
pub enum Command {
    Exit,
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
