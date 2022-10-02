use crate::table::{ColumnType, Constraint};
use crate::lexer::SqlValue;
use crate::where_clause::WhereClause;

#[derive(Debug)]
pub enum SelectColumnName {
    Name(SqlValue),
    AllColumns
}

#[derive(Debug)]
pub struct ColumnDefinition {
    pub name: SqlValue,
    pub kind: ColumnType, // TODO: maybe use token instead, transition to sematic types should be on exec stage?
    pub constraints: Vec<Constraint>,
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
    RenameTable {
        table_name: SqlValue,
        new_table_name: SqlValue,
    },
    RenameTableColumn {
        table_name: SqlValue,
        column_name: SqlValue,
        new_column_name: SqlValue,
    },
    AddTableColumn {
        table_name: SqlValue,
        column_definition: ColumnDefinition,
    },
    DropTableColumn {
        table_name: SqlValue,
        column_name: SqlValue,
    },
    VacuumTable {
        table_name: SqlValue,
    },
    Void,
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;

    use crate::database::Database;
    use crate::cmp_operator::CmpOperator;
    use crate::temp_file::TempFile;
    use crate::pager::page::PAGE_SIZE;

    #[test]
    fn create_and_drop_table() {
        let (_db_file, mut database) = open_test_database();
        let create_table = Command::CreateTable {
            table_name: SqlValue::Identificator("users".to_string()),
            columns: vec![
                ColumnDefinition {
                    name: SqlValue::Identificator("id".to_string()),
                    kind: ColumnType::Integer,
                    constraints: vec![],
                },
                ColumnDefinition {
                    name: SqlValue::Identificator("score".to_string()),
                    kind: ColumnType::Float,
                    constraints: vec![],
                },
                ColumnDefinition {
                    name: SqlValue::String("name full".to_string()),
                    kind: ColumnType::String,
                    constraints: vec![],
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
        let (_db_file, mut database) = open_test_database();
        let drop_table = Command::DropTable {
            table_name: SqlValue::Identificator("users".to_string()),
        };

        assert!(database.execute(drop_table).is_err());
    }

    #[test]
    fn insert_with_constraints_and_select_from_table() {
        let (_db_file, mut database) = open_test_database();
        let create_table = Command::CreateTable {
            table_name: SqlValue::Identificator("users".to_string()),
            columns: vec![
                ColumnDefinition {
                    name: SqlValue::Identificator("id".to_string()),
                    kind: ColumnType::Integer,
                    constraints: vec![Constraint::NotNull],
                },
                ColumnDefinition {
                    name: SqlValue::Identificator("name".to_string()),
                    kind: ColumnType::String,
                    constraints: vec![],
                }
            ],
        };
        database.execute(create_table).unwrap();

        let insert_into_table = Command::InsertInto {
            table_name: SqlValue::Identificator("users".to_string()),
            column_names: Some(vec![SqlValue::Identificator("id".to_string()), SqlValue::String("name".to_string())]),
            values: vec![SqlValue::Integer(1), SqlValue::Identificator("John".to_string())],
        };
        let insert_into_table_result = database.execute(insert_into_table);
        assert!(insert_into_table_result.is_ok());

        let insert_into_table = Command::InsertInto {
            table_name: SqlValue::Identificator("users".to_string()),
            column_names: Some(vec![SqlValue::String("name".to_string())]),
            values: vec![SqlValue::Identificator("John".to_string())],
        };
        let insert_into_table_result = database.execute(insert_into_table);
        assert!(insert_into_table_result.is_err());
        assert_eq!(format!("{}", insert_into_table_result.err().unwrap()),
            "value NULL violates 'NOT NULL' constraint on column 'id' from table 'users'");

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
        let (_db_file, mut database) = open_test_database();
        let create_table = Command::CreateTable {
            table_name: SqlValue::Identificator("users".to_string()),
            columns: vec![
                ColumnDefinition {
                    name: SqlValue::Identificator("id".to_string()),
                    kind: ColumnType::Integer,
                    constraints: vec![],
                },
                ColumnDefinition {
                    name: SqlValue::Identificator("name".to_string()),
                    kind: ColumnType::String,
                    constraints: vec![],
                }
            ],
        };
        database.execute(create_table).unwrap();

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
        let (_db_file, mut database) = open_test_database();
        let create_table = Command::CreateTable {
            table_name: SqlValue::Identificator("users".to_string()),
            columns: vec![
                ColumnDefinition {
                    name: SqlValue::Identificator("id".to_string()),
                    kind: ColumnType::Integer,
                    constraints: vec![],
                },
                ColumnDefinition {
                    name: SqlValue::Identificator("name".to_string()),
                    kind: ColumnType::String,
                    constraints: vec![],
                }
            ],
        };
        database.execute(create_table).unwrap();

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

    #[test]
    fn create_and_rename_table() {
        let (_db_file, mut database) = open_test_database();
        let create_table = Command::CreateTable {
            table_name: SqlValue::Identificator("users".to_string()),
            columns: vec![
                ColumnDefinition {
                    name: SqlValue::Identificator("id".to_string()),
                    kind: ColumnType::Integer,
                    constraints: vec![],
                },
            ],
        };

        assert!(database.execute(create_table).is_ok());

        let rename_table = Command::RenameTable {
            table_name: SqlValue::Identificator("users".to_string()),
            new_table_name: SqlValue::Identificator("users_new".to_string()),
        };

        assert!(database.execute(rename_table).is_ok());
    }

    #[test]
    fn create_table_and_rename_column() {
        let (_db_file, mut database) = open_test_database();
        let create_table = Command::CreateTable {
            table_name: SqlValue::Identificator("users".to_string()),
            columns: vec![
                ColumnDefinition {
                    name: SqlValue::Identificator("id".to_string()),
                    kind: ColumnType::Integer,
                    constraints: vec![],
                },
            ],
        };

        assert!(database.execute(create_table).is_ok());

        let rename_table_column = Command::RenameTableColumn {
            table_name: SqlValue::Identificator("users".to_string()),
            column_name: SqlValue::Identificator("id".to_string()),
            new_column_name: SqlValue::Identificator("id_new".to_string()),
        };

        assert!(database.execute(rename_table_column).is_ok());
    }

    #[test]
    fn create_table_and_add_column() {
        let (_db_file, mut database) = open_test_database();
        let create_table = Command::CreateTable {
            table_name: SqlValue::Identificator("users".to_string()),
            columns: vec![
                ColumnDefinition {
                    name: SqlValue::Identificator("id".to_string()),
                    kind: ColumnType::Integer,
                    constraints: vec![Constraint::NotNull],
                },
            ]
        };

        assert!(database.execute(create_table).is_ok());

        let add_table_column = Command::AddTableColumn {
            table_name: SqlValue::Identificator("users".to_string()),
            column_definition: ColumnDefinition {
                name: SqlValue::String("name".to_string()),
                kind: ColumnType::String,
                constraints: vec![],
            },
        };

        assert!(database.execute(add_table_column).is_ok());
    }

    #[test]
    fn create_table_and_drop_column() {
        let (_db_file, mut database) = open_test_database();
        let create_table = Command::CreateTable {
            table_name: SqlValue::Identificator("users".to_string()),
            columns: vec![
                ColumnDefinition {
                    name: SqlValue::Identificator("id".to_string()),
                    kind: ColumnType::Integer,
                    constraints: vec![],
                },
                ColumnDefinition {
                    name: SqlValue::Identificator("name".to_string()),
                    kind: ColumnType::String,
                    constraints: vec![],
                },
            ],
        };

        assert!(database.execute(create_table).is_ok());

        let drop_table_column = Command::DropTableColumn {
            table_name: SqlValue::Identificator("users".to_string()),
            column_name: SqlValue::String("name".to_string()),
        };

        assert!(database.execute(drop_table_column).is_ok());
    }

    #[test]
    fn create_table_insert_delete_and_vacuum() {
        let (db_file, mut database) = open_test_database();
        let create_table = Command::CreateTable {
            table_name: SqlValue::Identificator("users".to_string()),
            columns: vec![
                ColumnDefinition {
                    name: SqlValue::Identificator("id".to_string()),
                    kind: ColumnType::Integer,
                    constraints: vec![],
                },
                ColumnDefinition {
                    name: SqlValue::Identificator("name".to_string()),
                    kind: ColumnType::String,
                    constraints: vec![],
                },
            ],
        };
        // row size is 1 + 8 + 256 = 265 bytes, i.e. we can fit 15 rows per page

        assert!(database.execute(create_table).is_ok());

        for id in 0..31 {
            let insert_into_table = Command::InsertInto {
                table_name: SqlValue::Identificator("users".to_string()),
                column_names: Some(vec![SqlValue::Identificator("id".to_string())]),
                values: vec![SqlValue::Integer(id)],
            };
            let insert_into_table_result = database.execute(insert_into_table);
            assert!(insert_into_table_result.is_ok());
        }
        let mut users_table_path = db_file.temp_dir_path.clone();
        users_table_path.push("users.table");

        let delete_from_table = Command::Delete {
            table_name: SqlValue::Identificator("users".to_string()),
            where_clause: Some(WhereClause {
                left_value: SqlValue::String("id".to_string()),
                right_value: SqlValue::Integer(1),
                operator: CmpOperator::Equals,
            }),
        };
        let delete_from_table_result = database.execute(delete_from_table);
        assert!(delete_from_table_result.is_ok());
        assert_eq!(fs::metadata(users_table_path.as_path()).unwrap().len(), 3 * PAGE_SIZE as u64);

        let vacuum_table = Command::VacuumTable {
            table_name: SqlValue::Identificator("users".to_string()),
        };
        assert!(database.execute(vacuum_table).is_ok());
        assert_eq!(fs::metadata(users_table_path.as_path()).unwrap().len(), 2 * PAGE_SIZE as u64);

        let delete_from_table = Command::Delete {
            table_name: SqlValue::Identificator("users".to_string()),
            where_clause: Some(WhereClause {
                left_value: SqlValue::String("id".to_string()),
                right_value: SqlValue::Integer(15),
                operator: CmpOperator::LessEquals,
            }),
        };
        let delete_from_table_result = database.execute(delete_from_table);
        assert!(delete_from_table_result.is_ok());
        let vacuum_table = Command::VacuumTable {
            table_name: SqlValue::Identificator("users".to_string()),
        };
        assert!(database.execute(vacuum_table).is_ok());
        assert_eq!(fs::metadata(users_table_path.as_path()).unwrap().len(), PAGE_SIZE as u64);
    }

    fn open_test_database() -> (TempFile, Database) {
        let db_file = TempFile::new("database.db").unwrap();
        let temp_dir_path = db_file.temp_dir_path.to_str().unwrap();
        db_file.writeln_str(temp_dir_path).unwrap();
        let path = db_file.file_path.clone();
        // we need to return db_file because it will be dropped and removed otherwise
        (db_file, Database::from(path.as_path()).unwrap())
    }
}
