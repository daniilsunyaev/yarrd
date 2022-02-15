use crate::table::ColumnType;
use crate::lexer::SqlValue;
use crate::where_clause::WhereClause;

pub enum MetaCommand {
    Exit,
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
    pub column_name: String,
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
    },
    Void,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use crate::where_clause::CmpOperator;

    #[test]
    fn create_and_drop_table() {
        let mut database = open_test_database();
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
        let mut database = open_test_database();
        let drop_table = Command::DropTable {
            table_name: SqlValue::Identificator("users".to_string()),
        };

        assert!(database.execute(drop_table).is_err());
    }

    #[test]
    fn insert_and_select_from_table() {
        let mut database = open_test_database();
        let create_table = Command::CreateTable {
            table_name: SqlValue::Identificator("users".to_string()),
            columns: vec![
                ColumnDefinition {
                    name: SqlValue::Identificator("id".to_string()),
                    kind: ColumnType::Integer,
                },
                ColumnDefinition {
                    name: SqlValue::Identificator("name".to_string()),
                    kind: ColumnType::String,
                }
            ],
        };
        database.execute(create_table);

        let insert_into_table = Command::InsertInto {
            table_name: SqlValue::Identificator("users".to_string()),
            column_names: Some(vec![SqlValue::Identificator("id".to_string()), SqlValue::String("name".to_string())]),
            values: vec![SqlValue::Integer(1), SqlValue::Identificator("John".to_string())],
        };
        let insert_into_table_result = database.execute(insert_into_table);
        assert!(insert_into_table_result.is_ok());

        let select_from_table = Command::Select {
            table_name: SqlValue::Identificator("users".to_string()),
            column_names: vec![SelectColumnName::AllColumns, SelectColumnName::Name(SqlValue::Identificator("id".to_string()))],
            where_clause: Some(WhereClause {
                left_value: SqlValue::Integer(1),
                right_value: SqlValue::String("users.id".to_string()),
                operator: CmpOperator::Equals,
            }),
        };
        let select_result = database.execute(select_from_table);

        assert!(matches!(select_result, Ok(Some(_))));

        let select_rows = select_result.unwrap().unwrap();
        assert_eq!(select_rows.len(), 1);
        //assert_eq!(select_rows.first().unwrap().get(0), 1);
        //assert_eq!(select_rows.first().unwrap().get(1), "John");

        let select_from_table = Command::Select {
            table_name: SqlValue::Identificator("users".to_string()),
            column_names: vec![SelectColumnName::Name(SqlValue::Identificator("ip".to_string()))],
            where_clause: None,
        };

        assert!(database.execute(select_from_table).is_err());
    }

    #[test]
    fn insert_and_update_table() {
        let mut database = open_test_database();
        let create_table = Command::CreateTable {
            table_name: SqlValue::Identificator("users".to_string()),
            columns: vec![
                ColumnDefinition {
                    name: SqlValue::Identificator("id".to_string()),
                    kind: ColumnType::Integer,
                },
                ColumnDefinition {
                    name: SqlValue::Identificator("name".to_string()),
                    kind: ColumnType::String,
                }
            ],
        };
        database.execute(create_table);

        let insert_into_table = Command::InsertInto {
            table_name: SqlValue::Identificator("users".to_string()),
            column_names: Some(vec![SqlValue::Identificator("id".to_string()), SqlValue::String("name".to_string())]),
            values: vec![SqlValue::Integer(1), SqlValue::Identificator("John".to_string())],
        };
        let insert_into_table_result = database.execute(insert_into_table);
        assert!(insert_into_table_result.is_ok());

        let update_table = Command::Update {
            table_name: SqlValue::Identificator("users".to_string()),
            field_assignments: vec![FieldAssignment {
                column_name: "name".to_string(),
                value: SqlValue::String("Pete Mason".to_string()),
            }],
            where_clause: None,
        };
        let update_table_result = database.execute(update_table);
        assert!(update_table_result.is_ok());

        let update_table = Command::Update {
            table_name: SqlValue::Identificator("users".to_string()),
            field_assignments: vec![FieldAssignment {
                column_name: "name".to_string(),
                value: SqlValue::Integer(1),
            }],
            where_clause: None,
        };
        let update_table_result = database.execute(update_table);
        assert!(update_table_result.is_err());
    }

    #[test]
    fn insert_delete_and_select_from_table() {
        let mut database = open_test_database();
        let create_table = Command::CreateTable {
            table_name: SqlValue::Identificator("users".to_string()),
            columns: vec![
                ColumnDefinition {
                    name: SqlValue::Identificator("id".to_string()),
                    kind: ColumnType::Integer,
                },
                ColumnDefinition {
                    name: SqlValue::Identificator("name".to_string()),
                    kind: ColumnType::String,
                }
            ],
        };
        database.execute(create_table);

        let insert_into_table = Command::InsertInto {
            table_name: SqlValue::Identificator("users".to_string()),
            column_names: Some(vec![SqlValue::Identificator("id".to_string()), SqlValue::String("name".to_string())]),
            values: vec![SqlValue::Integer(1), SqlValue::Identificator("John".to_string())],
        };
        let insert_into_table_result = database.execute(insert_into_table);
        assert!(insert_into_table_result.is_ok());

        let delete_from_table = Command::Delete {
            table_name: SqlValue::Identificator("users".to_string()),
            where_clause: Some(WhereClause {
                left_value: SqlValue::String("John".to_string()),
                right_value: SqlValue::String("name".to_string()),
                operator: CmpOperator::Equals,
            }),
        };
        let delete_from_table_result = database.execute(delete_from_table);
        assert!(delete_from_table_result.is_ok());

        let select_from_table = Command::Select {
            table_name: SqlValue::Identificator("users".to_string()),
            column_names: vec![SelectColumnName::AllColumns],
            where_clause: None,
        };
        let select_result = database.execute(select_from_table);

        assert!(matches!(select_result, Ok(Some(_))));

        let select_rows = select_result.unwrap().unwrap();
        assert_eq!(select_rows.len(), 0);
    }

    fn open_test_database() -> Database {
        Database::from("./fixtures/database.db").unwrap()
    }
}
