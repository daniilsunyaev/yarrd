use crate::table::ColumnType;
use crate::lexer::SqlValue;
use crate::database::Database;

pub enum MetaCommand {
    Exit,
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
    Delete {
        table_name: SqlValue,
        where_clause: Option<WhereClause>,
    },
    CreateTable {
        table_name: SqlValue,
        columns: Vec<ColumnDefinition>,
    },
    DropTable {
        table_name: SqlValue,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_drop_table() {
        let mut database = Database::new();
        let create_table = Command::CreateTable {
            table_name: SqlValue::Identificator("users".to_string()),
            columns: vec![
                ColumnDefinition {
                    name: SqlValue::Identificator("id".to_string()),
                    kind: ColumnType::Integer,
                },
                ColumnDefinition {
                    name: SqlValue::String("name full".to_string()),
                    kind: ColumnType::String,
                },
            ],
        };

        assert!(database.execute(create_table).is_ok());

        let drop_table = Command::DropTable {
            table_name: SqlValue::Identificator("users".to_string()),
        };

        assert!(database.execute(drop_table).is_ok());
    }

    #[test]
    fn drop_non_existing_table() {
        let mut database = Database::new();
        let drop_table = Command::DropTable {
            table_name: SqlValue::Identificator("users".to_string()),
        };

        assert!(database.execute(drop_table).is_err());
    }

    #[test]
    fn select_from_table() {
        let mut database = Database::new();
        let create_table = Command::CreateTable {
            table_name: SqlValue::Identificator("users".to_string()),
            columns: vec![
                ColumnDefinition {
                    name: SqlValue::Identificator("id".to_string()),
                    kind: ColumnType::Integer,
                }
            ],
        };
        database.execute(create_table);

        let select_from_table = Command::Select {
            table_name: SqlValue::Identificator("users".to_string()),
            column_names: vec![SelectColumnName::AllColumns, SelectColumnName::Name(SqlValue::Identificator("id".to_string()))],
            where_clause: None,
        };
        let select_result = database.execute(select_from_table);

        assert!(select_result.is_ok());

        // assert_eq!(select_result.unwrap().first().get(0), 1)

        //let select_from_table = Command::Select {
        //    table_name: SqlValue::Identificator("users".to_string()),
        //    column_names: vec![SelectColumnName::Name(SqlValue::Identificator("ip".to_string()))],
        //    where_clause: None,
        //};

        //assert!(database.execute(select_from_table).is_err());
    }
}
