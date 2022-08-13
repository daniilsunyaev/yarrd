use std::path::{Path, PathBuf};

use crate::command::Command;
use crate::meta_command::MetaCommand;
use crate::meta_command_error::MetaCommandError;
use crate::lexer::Token;
use crate::parser::error::ParserError;
use create::parse_create_statement;
use drop::parse_drop_statement;
use insert::parse_insert_statement;
use update::parse_update_statement;
use select::parse_select_statement;
use delete::parse_delete_statement;
use alter::parse_alter_statement;
use vacuum::parse_vacuum_statement;

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
        }

        match input {
            ".exit" | ".quit" => MetaCommand::Exit,
            _ => MetaCommand::Unknown(input.to_string()),
        }
    } else {
        MetaCommand::Void
    }
}

pub fn parse_createdb(input: &str) -> Result<MetaCommand, ParserError> {
    let mut input_iterator = input.splitn(3, ' ');
    input_iterator.next(); // skip ".createdb"

    let db_path = pathify(input_iterator.next().ok_or(ParserError::DatabaseNameMissing)?);

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

pub fn parse_dropdb(input: &str) -> Result<MetaCommand, ParserError> {
    let mut input_iterator = input.splitn(2, ' ');
    input_iterator.next(); // skip ".dropdb"

    let db_path_str = input_iterator.next().ok_or(ParserError::DatabaseNameMissing)?;
    let db_path = PathBuf::from(db_path_str);

    Ok(MetaCommand::Dropdb(db_path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::SqlValue;

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
                Token::Value(SqlValue::String("first_name".into())), Token::StringType, Token::Comma,
                Token::Value(SqlValue::Identificator("id".into())), Token::IntegerType, Token::Comma,
                Token::Value(SqlValue::Identificator("age".into())), Token::FloatType,
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

        println!("{:?}", parse_statement(input.iter()));
        assert!(parse_statement(input.iter()).is_ok());
    }

    #[test]
    fn select_all_with_separate_columns() {
        let input = vec![
                Token::Select, Token::AllColumns, Token::Comma, Token::Value(SqlValue::Integer(12)),
                Token::From,  Token::Value(SqlValue::Identificator("table_name".into())),
           ];

        println!("{:?}", parse_statement(input.iter()));
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
                Token::Add, Token::Value(SqlValue::Identificator("column_name".into())),
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
}
