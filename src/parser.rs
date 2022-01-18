use crate::command::{Command, MetaCommand};
use crate::lexer::Token;
use create::parse_create_statement;
use drop::parse_drop_statement;
use insert::parse_insert_statement;

mod create;
mod drop;
mod insert;

pub fn parse_statement<'a, I>(mut token: I) -> Result<Command, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let command = match token.next() {
        Some(Token::Create) => parse_create_statement(&mut token)?,
        Some(Token::Drop) => parse_drop_statement(&mut token)?,
        Some(Token::Insert) => parse_insert_statement(&mut token)?,
        _ => return Err("cannot parse statement".to_string()),
    };

    println!("{:?}", command);
    let remainder = token.collect::<Vec<&Token>>();
    if !remainder.is_empty() {
        Err(format!("parsed correct statement, but some excess tokens are present in the input: {:?}", remainder))
    } else {
        Ok(command)
    }
}

pub fn parse_meta_command(input: &str) -> Result<Option<MetaCommand>, String> {
    if input.starts_with('.') {
        match input {
            ".exit" | ".quit" => Ok(Some(MetaCommand::Exit)),
            _ => Err(format!("unrecognized meta_command '{}'", input)),
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
                Token::LeftParenthesis, Token::Value(SqlValue::String("first_name".into())),
                Token::StringType, Token::Comma, Token::Value(SqlValue::Identificator("id".into())),
                Token::IntegerType, Token::RightParenthesis,
           ];

        assert!(parse_statement(input.iter()).is_ok());
    }
}
