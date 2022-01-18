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
        where_clause: Option<WhereClause>,
    },
    Update {
        table_name: SqlValue,
        field_assignments: Vec<FieldAssignment>,
        where_clause: Option<WhereClause>,
    },
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
pub struct WhereClause {
    pub left_value: SqlValue,
    pub right_value: SqlValue,
    pub operator: CmpOperator,
}

#[derive(Debug)]
pub enum CmpOperator {
    Less,
    Greater,
    Equals,
    LessEquals,
    GreaterEquals,
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

#[derive(Debug)]
pub struct FieldAssignment {
    pub column_name: SqlValue,
    pub value: SqlValue,
}
