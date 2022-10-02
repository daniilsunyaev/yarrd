use crate::parser::error::ParserError;
use crate::lexer::{SqlValue, Token};
use crate::command::ColumnDefinition;
use crate::table::ColumnType;
use crate::table::Constraint;

pub fn parse_table_name<'a, I>(mut token: I) -> Result<SqlValue, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::Value(name)) => Ok(name.clone()),
        Some(token) => Err(ParserError::TableNameInvalid(token)),
        None => Err(ParserError::TableNameMissing),
    }
}

pub fn parse_column_name<'a, I>(mut token: I) -> Result<SqlValue, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::Value(name)) => Ok(name.clone()),
        Some(token) => Err(ParserError::ColumnNameInvalid(token)),
        None => Err(ParserError::ColumnNameMissing),
    }
}

pub fn parse_column_type<'a, I>(mut token: I) -> Result<ColumnType, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
        match token.next() {
            Some(Token::IntegerType) => Ok(ColumnType::Integer),
            Some(Token::FloatType) => Ok(ColumnType::Float),
            Some(Token::StringType) => Ok(ColumnType::String),
            Some(token) => Err(ParserError::ColumnTypeInvalid(token)),
            None => Err(ParserError::ColumnTypeMissing),
        }
}

pub fn parse_column_value<'a, I>(mut token: I) -> Result<SqlValue, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::Value(value)) => Ok(value.clone()),
        Some(token) => Err(ParserError::ColumnValueInvalid(token)),
        None => Err(ParserError::ColumnValueMissing),
    }
}

pub fn parse_left_parenthesis<'a, I>(mut token: I, entity: &'static str) -> Result<(), ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::LeftParenthesis) => Ok(()),
        Some(token) => Err(ParserError::LeftParenthesisExpected(token, entity)),
        None => Err(ParserError::LeftParenthesisMissing(entity)),
    }
}

pub fn parse_csl_right_parenthesis<'a, I>(mut token: I, entity: &'static str) -> Result<bool, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::RightParenthesis) => Ok(true),
        Some(Token::Comma) => Ok(false),
        Some(token) => return Err(ParserError::RightParenthesisExpected(token, entity)),
        None => return Err(ParserError::RightParenthesisMissing(entity)),
    }
}

pub fn parse_column_definition<'a, I>(mut token: I) -> Result<(ColumnDefinition, Option<Token>), ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
        let name = parse_column_name(&mut token)?;
        let kind = parse_column_type(&mut token)?;
        let (constraint_tokens, last_token) = parse_ssl_tokens_with_trailing_token(&mut token);
        let constraints = parse_constraint_tokens(constraint_tokens)?;

        Ok((ColumnDefinition { name, kind, constraints }, last_token))
}

fn parse_ssl_tokens_with_trailing_token<'a, I>(mut token: I) -> (Vec<&'a Token>, Option<Token>)
where
    I: Iterator<Item = &'a Token>
{
    let mut tokens = vec![];
    loop {
        match token.next() {
            Some(Token::RightParenthesis) => return (tokens, Some(Token::RightParenthesis)),
            Some(Token::Comma) => return (tokens, Some(Token::Comma)),
            Some(token) => tokens.push(token),
            None => return (tokens, None),
        }

    }
}

fn parse_constraint_tokens<'a>(tokens: Vec<&'a Token>) -> Result<Vec<Constraint>, ParserError<'a>> {
    let mut iter = tokens.iter();
    let mut result = vec![];

    loop {
        match iter.next() {
            Some(Token::Not) => {
                match iter.next() {
                    Some(Token::Value(SqlValue::Null)) => result.push(Constraint::NotNull),
                    _ => return Err(ParserError::InvalidConstraint(tokens)),
                }
            },
            None => break,
            _ => return Err(ParserError::InvalidConstraint(tokens)),
        }
    }

    Ok(result)
}

