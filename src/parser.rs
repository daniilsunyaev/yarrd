use crate::command::{Command, MetaCommand};
use crate::lexer::Token;
use crate::parser::error::ParserError;
use create::parse_create_statement;
use drop::parse_drop_statement;
use insert::parse_insert_statement;
use update::parse_update_statement;
use select::parse_select_statement;
use delete::parse_delete_statement;
use alter::parse_alter_statement;

mod create;
mod drop;
mod insert;
mod select;
mod where_clause;
mod update;
mod delete;
mod alter;
mod error;
mod shared;

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

pub fn parse_meta_command(input: &str) -> Result<Option<MetaCommand>, ParserError> {
    if input.starts_with('.') {
        match input {
            ".exit" | ".quit" => Ok(Some(MetaCommand::Exit)),
            _ => Err(ParserError::UnknownMetaCommand(input)),
        }
    } else {
        Ok(None)
    }
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
}
