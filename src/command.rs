use crate::table::{ColumnType, Table};
use crate::lexer::SqlValue;

pub enum MetaCommand {
    Exit,
}

#[derive(Debug)]
pub enum CmpOperator {
    Less,
    Greater,
    Equals,
    NotEquals,
    LessEquals,
    GreaterEquals,
}

impl CmpOperator {
    pub fn apply(&self, left: &SqlValue, right: &SqlValue) -> Result<bool, String> {
        match left {
            SqlValue::Integer(lvalue) => {
                match right {
                    SqlValue::Integer(rvalue) => Ok(self.cmp_ord(lvalue, rvalue)),
                    _ =>  Err(format!("cannot compare {:?} with number", right)),
                }

            },
            SqlValue::String(lvalue) | SqlValue::Identificator(lvalue) => {
                match self {
                    Self::Equals | Self::NotEquals => {
                        match right {
                            SqlValue::Integer(_rvalue) =>  Err(format!("cannot compare {} with number", lvalue)),
                            SqlValue::String(rvalue) | SqlValue::Identificator(rvalue) => self.cmp_eq(lvalue, rvalue),
                        }
                    },
                    _ => Err(format!("string {} can only be compared with other values with '=' or '<>'", lvalue)),
                }
            }

        }
    }

    fn cmp_eq<Stringlike>(&self, left: Stringlike, right: Stringlike) -> Result<bool, String>
    where
        Stringlike: PartialEq + std::fmt::Display
    {
        match self {
            Self::Equals => Ok(left == right),
            Self::NotEquals => Ok(left != right),
            _ => Err(format!("cannot compare {} with {}", left, right)),
        }
    }

    fn cmp_ord<Number>(&self, left: Number, right: Number) -> bool
    where
        Number: PartialOrd
    {
        match self {
            Self::Less => left < right,
            Self::Greater => left > right,
            Self::Equals => left == right,
            Self::NotEquals => left != right,
            Self::LessEquals => left <= right,
            Self::GreaterEquals => left >= right,
        }
    }
}

#[derive(Debug)]
pub struct WhereClause {
    pub left_value: SqlValue,
    pub right_value: SqlValue,
    pub operator: CmpOperator,
}

impl WhereClause {
    pub fn build_filter<'a>(&'a self, table: &'a Table) -> Box<dyn Fn(&'a Vec<SqlValue>) -> Result<bool, String> + 'a> {
        let get_left_value = self.build_value_getter(table, &self.left_value);
        let get_right_value = self.build_value_getter(table, &self.right_value);

        Box::new(move |row: &Vec<SqlValue>| {
            self.operator.apply(&get_left_value(row), &get_right_value(row))
        })
    }


    fn build_value_getter<'a>(&'a self, table: &'a Table, value: &'a SqlValue) -> Box<dyn Fn(&'a Vec<SqlValue>) -> SqlValue + 'a> {
        let dummy_getter = |_row| value.clone();
        let table_name = table.name.as_str();
        let string_value = value.to_string();
        let column_name = {
            let splitted_identificator: Vec<&str> = string_value.split('.').collect();
            match splitted_identificator.len() {
                1 => string_value.as_str(),
                2 => {
                    if !splitted_identificator[0].eq(table_name) {
                        return Box::new(dummy_getter);
                    } else {
                        splitted_identificator[1]
                    }
                },
                _ => return Box::new(dummy_getter),
            }
        };

        if let Some(column_index) = table.column_index(column_name) {
           Box::new(move |row: &Vec<SqlValue>| row[column_index].clone())
        } else {
           Box::new(dummy_getter)
        }
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
    use crate::database::Database;

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
            where_clause: Some(WhereClause {
                left_value: SqlValue::Integer(1),
                right_value: SqlValue::String("users.id".to_string()),
                operator: CmpOperator::Equals,
            }),
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
