use std::path::{Path, PathBuf};

use crate::command::Command;
use crate::meta_command::MetaCommand;
use crate::meta_command_error::MetaCommandError;
use crate::lexer;
use crate::lexer::Token;
use crate::command::ColumnDefinition;
use crate::parser::error::ParserError;
use create::parse_create_statement;
use drop::parse_drop_statement;
use insert::parse_insert_statement;
use update::parse_update_statement;
use select::parse_select_statement;
use delete::parse_delete_statement;
use alter::parse_alter_statement;
use vacuum::parse_vacuum_statement;
use crate::parser::shared::{parse_column_definition, parse_index_name};

mod create;
mod drop;
mod insert;
mod select;
mod where_clause;
mod update;
mod delete;
mod alter;
mod vacuum;
mod error;
mod shared;

const DEFAULT_PATH: &str = ".";
const CURRENT_FOLDER_PATH: &str = ".";
const DEFAULT_TABLES_DIR_SUFFIX: &str = "_tables";

pub struct TableSchemaDefinitionLine {
    pub name: String,
    pub row_count: usize,
    pub column_definitions: Vec<ColumnDefinition>,
    pub indexes_definitions: Vec<(usize, String)>,
}

pub fn parse_statement<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let command = match token.next() {
        Some(Token::Create) => parse_create_statement(&mut token)?,
        Some(Token::Drop) => parse_drop_statement(&mut token)?,
        Some(Token::Insert) => parse_insert_statement(&mut token)?,
        Some(Token::Select) => parse_select_statement(&mut token)?,
        Some(Token::Update) => parse_update_statement(&mut token)?,
        Some(Token::Delete) => parse_delete_statement(&mut token)?,
        Some(Token::Alter) => parse_alter_statement(&mut token)?,
        Some(Token::Vacuum) => parse_vacuum_statement(&mut token)?,
        Some(command) => return Err(ParserError::UnknownCommand(command)),
        _ => return Ok(Command::Void),
    };

    let remainder = token.collect::<Vec<&Token>>();
    if remainder.is_empty() {
        Ok(command)
    } else {
        Err(ParserError::ExcessTokens(remainder))
    }
}

pub fn parse_meta_command(input: &str) -> MetaCommand {
    if input.starts_with('.') {
        if input.starts_with(".createdb") {
            match parse_createdb(input) {
                Ok(createdb_meta_command) => return createdb_meta_command,
                Err(error) => return MetaCommand::MetacommandWithWrongArgs(MetaCommandError::ParseError(error.to_string())),
            }
        } else if input.starts_with(".dropdb") {
            match parse_dropdb(input) {
                Ok(dropdb_meta_command) => return dropdb_meta_command,
                Err(error) => return MetaCommand::MetacommandWithWrongArgs(MetaCommandError::ParseError(error.to_string())),
            }
        } else if input.starts_with(".connect") {
            match parse_connect(input) {
                Ok(connect_meta_command) => return connect_meta_command,
                Err(error) => return MetaCommand::MetacommandWithWrongArgs(MetaCommandError::ParseError(error.to_string())),
            }
        }

        match input.trim() {
            ".close" => MetaCommand::CloseConnection,
            ".exit" | ".quit" => MetaCommand::Exit,
            _ => MetaCommand::Unknown(input.to_string()),
        }
    } else {
        MetaCommand::Void
    }
}

pub fn parse_schema_line(table_definition_line: &str) -> Result<TableSchemaDefinitionLine, ParserError> {
    let tokens = lexer::to_tokens(table_definition_line).map_err(ParserError::LexerError)?;
    let mut token_iter = tokens.iter();
    let table_name = token_iter.next().ok_or(ParserError::TableNameMissing)?.to_string();
    let row_count_string = token_iter.next().ok_or(ParserError::RowCountMissing)?.to_string();
    let row_count = row_count_string.parse::<usize>().map_err(|_| ParserError::RowCountInvalid(row_count_string))?;

    let mut indexes_definitions = vec![];
    let mut column_definitions = vec![];

    loop {
        let (column_definition, last_token) = parse_column_definition(&mut token_iter)
            .map_err(|parser_error| ParserError::InvalidSchemaDefinition(parser_error.to_string()))?;

        column_definitions.push(column_definition);

        match last_token {
            Some(Token::Comma) => continue,
            Some(Token::Semicolon) => break,
            None => return Ok(TableSchemaDefinitionLine { name: table_name, row_count, column_definitions, indexes_definitions }),
            _ => return Err(ParserError::CommaExpected("column_definitions")),
        }
    }

    loop {
        let (i, index_name, last_token) = parse_index_definition(&mut token_iter)
            .map_err(|parser_error| ParserError::InvalidSchemaDefinition(parser_error.to_string()))?;

        indexes_definitions.push((i, index_name));

        match last_token {
            Some(Token::Comma) => continue,
            Some(Token::Semicolon) => break,
            None => break,
            _ => return Err(ParserError::CommaExpected("index_definitions")),
        }
    }
    Ok(TableSchemaDefinitionLine { name: table_name, row_count, column_definitions, indexes_definitions })
}

pub fn parse_index_definition<'a, I>(mut token: I) -> Result<(usize, String, Option<&'a Token>), ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    let column_number = parse_int(&mut token)?;
    let name = parse_index_name(&mut token)?.to_string();

    Ok((column_number, name, token.next()))
}

pub fn parse_int<'a, I>(mut token: I) -> Result<usize, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(token_val) => {
            let int_string = token_val.to_string();
            let int = int_string.parse::<usize>().map_err(|_| ParserError::IntegerExpected(token_val))?;
            Ok(int)
        },
        None => Err(ParserError::IntegerMissing),
    }
}

pub fn parse_createdb(input: &str) -> Result<MetaCommand, ParserError> {
    let mut input_iterator = input.splitn(3, ' ');
    input_iterator.next(); // skip ".createdb"

    let db_path = pathify(input_iterator.next().ok_or(ParserError::DatabasePathMissing)?);

    let db_file_name = db_path
        .file_name().ok_or(ParserError::CouldNotParseDbFilename(input))?
        .to_str().ok_or(ParserError::CouldNotParseDbFilename(input))?;

    let db_dir_path = db_path.parent()
        .ok_or(ParserError::CouldNotParseDbFilename(input))?;

    let tables_dir = match input_iterator.next() {
        Some(string) => pathify(string),
        None => {
            let mut tables_dir_path = PathBuf::from(db_dir_path);
            tables_dir_path.push(format!("{}{}", db_file_name, DEFAULT_TABLES_DIR_SUFFIX));
            tables_dir_path
        }
    };

    Ok(MetaCommand::Createdb { db_path, tables_dir_path: tables_dir })
}

pub fn parse_dropdb(input: &str) -> Result<MetaCommand, ParserError> {
    let mut input_iterator = input.splitn(2, ' ');
    input_iterator.next(); // skip ".dropdb"

    let db_path_str = input_iterator.next().ok_or(ParserError::DatabasePathMissing)?;
    let db_path = PathBuf::from(db_path_str);

    Ok(MetaCommand::Dropdb(db_path))
}

pub fn parse_connect(input: &str) -> Result<MetaCommand, ParserError> {
    let mut input_iterator = input.splitn(2, ' ');
    input_iterator.next(); // skip ".connect"

    let db_path_str = input_iterator.next().ok_or(ParserError::DatabasePathMissing)?;
    let db_path = PathBuf::from(db_path_str);

    Ok(MetaCommand::Connect(db_path))
}

fn pathify(string: &str) -> PathBuf {
    let input_path = Path::new(string);

    if input_path.is_absolute() || input_path.starts_with(CURRENT_FOLDER_PATH) {
        PathBuf::from(string)
    } else {
        let mut path = PathBuf::from(DEFAULT_PATH);
        path.push(string);
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::SqlValue;
    use crate::table::{ColumnType, Constraint};
    use crate::binary_condition::BinaryCondition;
    use crate::cmp_operator::CmpOperator;

    #[test]
    fn insert_with_column_names() {
        let input = vec![
                Token::Insert, Token::Into, Token::Value(SqlValue::Identificator("table_name".into())),
                Token::LeftParenthesis, Token::Value(SqlValue::Identificator("id".into())),
                Token::Comma, Token::Value(SqlValue::Identificator("name".into())),
                Token::RightParenthesis, Token::Values,
                Token::LeftParenthesis, Token::Value(SqlValue::Integer(1)),
                Token::Comma, Token::Value(SqlValue::Identificator("name".into())),
                Token::RightParenthesis,
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn create_table() {
        let input = vec![
                Token::Create, Token::Table, Token::Value(SqlValue::Identificator("table_name".into())),
                Token::LeftParenthesis,
                Token::Value(SqlValue::String("first_name".into())),
                Token::StringType, Token::Not, Token::Value(SqlValue::Null), Token::Comma,

                Token::Value(SqlValue::Identificator("id".into())), Token::IntegerType, Token::Comma,

                Token::Value(SqlValue::Identificator("age".into())), Token::FloatType,
                Token::Check, Token::LeftParenthesis,
                Token::Value(SqlValue::Identificator("age".into())), Token::Greater,
                Token::Value(SqlValue::Identificator("0".into())),
                Token::RightParenthesis,

                Token::RightParenthesis,
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn select_columns() {
        let input = vec![
                Token::Select,
                Token::Value(SqlValue::String("first_name".into())), Token::Comma,
                Token::Value(SqlValue::Identificator("id".into())),
                Token::From,  Token::Value(SqlValue::Identificator("table_name".into())),
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn select_columns_where() {
        let input = vec![
                Token::Select,
                Token::Value(SqlValue::String("first_name".into())), Token::Comma,
                Token::Value(SqlValue::Identificator("id".into())),
                Token::From,  Token::Value(SqlValue::Identificator("table_name".into())),
                Token::Where, Token::Value(SqlValue::String("id".into())), Token::LessEquals,
                Token::Value(SqlValue::Integer(10))
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn select_all_columns() {
        let input = vec![
                Token::Select, Token::AllColumns,
                Token::From,  Token::Value(SqlValue::Identificator("table_name".into())),
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn select_all_with_separate_columns() {
        let input = vec![
                Token::Select, Token::AllColumns, Token::Comma, Token::Value(SqlValue::Integer(12)),
                Token::From,  Token::Value(SqlValue::Identificator("table_name".into())),
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn update_columns() {
        let input = vec![
                Token::Update,
                Token::Value(SqlValue::Identificator("table_name".into())),
                Token::Set, Token::Value(SqlValue::String("first_name".into())),
                Token::Equals, Token::Value(SqlValue::Integer(2)),
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn update_columns_where() {
        let input = vec![
                Token::Update,
                Token::Value(SqlValue::Identificator("table_name".into())),
                Token::Set, Token::Value(SqlValue::String("first_name".into())),
                Token::Equals, Token::Value(SqlValue::Integer(2)),
                Token::Where, Token::Value(SqlValue::String("id".into())), Token::Greater,
                Token::Value(SqlValue::Integer(10))
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn delete_rows() {
        let input = vec![
                Token::Delete, Token::From,
                Token::Value(SqlValue::Identificator("table_name".into())),
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn delete_columns_where() {
        let input = vec![
                Token::Delete, Token::From,
                Token::Value(SqlValue::Identificator("table_name".into())),
                Token::Where, Token::Value(SqlValue::String("id".into())), Token::Equals,
                Token::Value(SqlValue::Integer(10))
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn alter_rename_table() {
        let input = vec![
                Token::Alter, Token::Table,
                Token::Value(SqlValue::Identificator("table_name".into())),
                Token::Rename, Token::To,
                Token::Value(SqlValue::Identificator("new_table_name".into())),
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn alter_add_constraint() {
        let input = vec![
                Token::Alter, Token::Table,
                Token::Value(SqlValue::Identificator("table_name".into())),
                Token::Add, Token::Constraint, Token::Not, Token::Value(SqlValue::Null),
                Token::LeftParenthesis,
                Token::Value(SqlValue::Identificator("id".into())),
                Token::RightParenthesis,
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn alter_rename_table_column() {
        let input = vec![
                Token::Alter, Token::Table,
                Token::Value(SqlValue::Identificator("table_name".into())),
                Token::Rename, Token::Column,
                Token::Value(SqlValue::Identificator("column_name".into())),
                Token::To,
                Token::Value(SqlValue::Identificator("new_column_name".into())),
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn alter_table_add_column() {
        let input = vec![
                Token::Alter, Token::Table,
                Token::Value(SqlValue::Identificator("table_name".into())),
                Token::Add, Token::Column, Token::Value(SqlValue::Identificator("column_name".into())),
                Token::IntegerType,
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn alter_table_drop_column() {
        let input = vec![
                Token::Alter, Token::Table,
                Token::Value(SqlValue::Identificator("table_name".into())),
                Token::Drop, Token::Column,
                Token::Value(SqlValue::Identificator("column_name".into())),
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn alter_table_drop_constraint() {
        let input = vec![
                Token::Alter, Token::Table,
                Token::Value(SqlValue::Identificator("table_name".into())),
                Token::Drop, Token::Constraint, Token::Not, Token::Value(SqlValue::Null),
                Token::LeftParenthesis,
                Token::Value(SqlValue::Identificator("id".into())),
                Token::RightParenthesis,
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn create_index() {
        let input = vec![
                Token::Create, Token::Index,
                Token::Value(SqlValue::Identificator("index_name".into())),
                Token::On, Token::Value(SqlValue::Identificator("table_name".into())),
                Token::Value(SqlValue::Identificator("id".into())),
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn drop_index() {
        let input = vec![
                Token::Drop, Token::Index,
                Token::Value(SqlValue::Identificator("index_name".into())),
                Token::On, Token::Value(SqlValue::Identificator("table_name".into())),
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn vacuum_table() {
        let input = vec![
                Token::Vacuum,
                Token::Value(SqlValue::Identificator("table_name".into())),
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn exit() {
        assert!(matches!(parse_meta_command(".exit"), MetaCommand::Exit));
        assert!(matches!(parse_meta_command(".quit"), MetaCommand::Exit));
    }

    #[test]
    fn void() {
        assert!(matches!(parse_meta_command(""), MetaCommand::Void));
        assert!(matches!(parse_meta_command("select"), MetaCommand::Void));
        assert!(matches!(parse_meta_command("foo"), MetaCommand::Void));
    }


    #[test]
    fn unknown() {
        assert!(matches!(parse_meta_command(".foo"), MetaCommand::Unknown(_)));
        assert!(matches!(parse_meta_command("."), MetaCommand::Unknown(_)));
        assert!(matches!(parse_meta_command(".select"), MetaCommand::Unknown(_)));
    }

    #[test]
    fn createdb() {
        let valid_expectations = vec![
            (".createdb foo", ("./foo", "./foo_tables")),
            (".createdb foo ./bar", ("./foo", "./bar")),
            (".createdb ./some_path/foo ./foo/bar", ("./some_path/foo", "./foo/bar")),
            (".createdb foo bar", ("./foo", "./bar")),
            (".createdb .foo /some_path/bar", ("./.foo", "/some_path/bar")),
            (".createdb /some_abs_path/foo bar", ("/some_abs_path/foo", "./bar")),
        ];

        assert!(matches!(
                    parse_meta_command(".createdb"),
                    MetaCommand::MetacommandWithWrongArgs(MetaCommandError::ParseError(_))
                ));

        for expectation in valid_expectations {
            assert_createdb(expectation.0, expectation.1.0, expectation.1.1)
        }
    }

    fn assert_createdb(input: &str, metacommand_db_path: &str, metacommand_tables_dir_path: &str) {
        match parse_meta_command(input) {
            MetaCommand::Createdb { db_path, tables_dir_path } => {
                assert_eq!(db_path, PathBuf::from(metacommand_db_path));
                assert_eq!(tables_dir_path, PathBuf::from(metacommand_tables_dir_path));
            },
            _ => panic!("Expected '{}' to be parsed to Createdb", input),
        }
    }

    #[test]
    fn dropdb() {
        assert!(matches!(
                    parse_meta_command(".dropdb"),
                    MetaCommand::MetacommandWithWrongArgs(MetaCommandError::ParseError(_))
                ));

        match parse_meta_command(".dropdb foo") {
            MetaCommand::Dropdb(db_path) => {
                assert_eq!(db_path, PathBuf::from("foo"));
            },
            _ => panic!("Expected '.dropdb foo' to be parsed to Createdb"),
        }
    }

    #[test]
    fn connect() {
        assert!(matches!(
                    parse_meta_command(".connect"),
                    MetaCommand::MetacommandWithWrongArgs(MetaCommandError::ParseError(_))
                ));

        match parse_meta_command(".connect foo") {
            MetaCommand::Connect(db_path) => {
                assert_eq!(db_path, PathBuf::from("foo"));
            },
            _ => panic!("Expected '.connect foo' to be parsed to Createdb"),
        }

        match parse_meta_command(".connect /foo/bar") {
            MetaCommand::Connect(db_path) => {
                assert_eq!(db_path, PathBuf::from("/foo/bar"));
            },
            _ => panic!("Expected '.connect /foo/bar' to be parsed to Createdb"),
        }
    }

    #[test]
    fn close() {
        assert!(matches!(
                    parse_meta_command(".close"),
                    MetaCommand::CloseConnection
                ));

        assert!(matches!(
                    parse_meta_command(".close foo"),
                    MetaCommand::Unknown(_)
                ));
    }

    #[test]
    fn parse_valid_schema() {
        let TableSchemaDefinitionLine { name: table_name, row_count, column_definitions, indexes_definitions } =
            parse_schema_line("users 0 id int not null default 1 check(id > 0), name string").unwrap();
        assert_eq!(table_name, "users");
        assert_eq!(row_count, 0);
        assert_eq!(column_definitions[0].name.to_string(), "id");
        assert!(matches!(column_definitions[0].kind, ColumnType::Integer));
        assert_eq!(column_definitions[0].column_constraints.len(), 3);
        assert!(matches!(column_definitions[0].column_constraints[0], Constraint::NotNull));
        assert!(matches!(column_definitions[0].column_constraints[1], Constraint::Default(SqlValue::Integer(1))));

        assert_eq!(column_definitions[0].column_constraints[2],
                   Constraint::Check(
                       BinaryCondition {
                           left_value: SqlValue::Identificator("id".to_string()),
                           right_value: SqlValue::Integer(0),
                           operator: CmpOperator::Greater,
                       }
                   )
                  );

        assert_eq!(column_definitions[1].name.to_string(), "name");
        assert!(matches!(column_definitions[1].kind, ColumnType::String));
        assert_eq!(column_definitions[1].column_constraints.len(), 0);
        assert_eq!(indexes_definitions.len(), 0);
    }

    #[test]
    fn parse_another_valid_schema() {
        let TableSchemaDefinitionLine { name: table_name, row_count, column_definitions, indexes_definitions } =
            parse_schema_line("users 2 id int, age int; 1 age_hash;").unwrap();
        assert_eq!(table_name, "users");
        assert_eq!(row_count, 2);
        assert_eq!(column_definitions[0].name.to_string(), "id");
        assert!(matches!(column_definitions[0].kind, ColumnType::Integer));
        assert_eq!(column_definitions[0].column_constraints.len(), 0);
        assert_eq!(column_definitions[1].name.to_string(), "age");
        assert!(matches!(column_definitions[1].kind, ColumnType::Integer));
        assert_eq!(column_definitions[1].column_constraints.len(), 0);
        assert_eq!(indexes_definitions.len(), 1);
        assert_eq!(indexes_definitions[0], (1, "age_hash".to_string()));
    }

    #[test]
    fn parse_invalid_schema() {
        assert!(matches!(
                parse_schema_line("users 0 id int not, name string"),
                Err(ParserError::InvalidSchemaDefinition(_))
                )
               );
    }
}
