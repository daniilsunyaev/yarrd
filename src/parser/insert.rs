use crate::command::Command;
use crate::lexer::{Token, SqlValue};
use crate::parser::error::ParserError;

pub fn parse_insert_statement<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    match token.next() {
        Some(Token::Into) => parse_insert_into(token),
        Some(token) => Err(ParserError::InsertInvalid(token)),
        None => Err(ParserError::IntoMissing),
    }
}

fn parse_insert_into<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{


    let table_name = match token.next() {
        Some(Token::Value(name)) => name.clone(),
        Some(token) => return Err(ParserError::TableNameInvalid(token)),
        None => return Err(ParserError::TableNameMissing),
    };

    let column_names = parse_column_names(&mut token)?;
    let values = parse_values_expression(&mut token)?;

    Ok(Command::InsertInto { table_name, column_names: Some(column_names), values })
}

fn parse_column_names<'a, I>(mut token: I) -> Result<Vec<SqlValue>, ParserError<'a>>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let mut columns = vec![];

    match token.next() {
        Some(Token::LeftParenthesis) => { },
        Some(token) => return Err(ParserError::LeftParenthesisExpected(token, "column names")),
        None => return Err(ParserError::LeftParenthesisMissing("column names")),
    }

    loop {
        let name = match token.next() {
            Some(Token::Value(name)) => name.clone(),
            Some(token) => return Err(ParserError::ColumnNameInvalid(token)),
            None => return Err(ParserError::ColumnNameMissing),
        };

        columns.push(name);

        match token.next() {
            Some(Token::RightParenthesis) => break,
            Some(Token::Comma) => { },
            Some(token) => return Err(ParserError::RightParenthesisExpected(token, "column names")),
            None => return Err(ParserError::RightParenthesisMissing("column names")),
        };
    }

    Ok(columns)
}

fn parse_values_expression<'a, I>(mut token: I) -> Result<Vec<SqlValue>, ParserError<'a>>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let mut values = vec![];

    match token.next() {
        Some(Token::Values) => { },
        Some(token) => return Err(ParserError::ValuesKeywordMissing(token)),
        None => return Err(ParserError::InsertValuesMissing),
    }

    match token.next() {
        Some(Token::LeftParenthesis) => { },
        Some(token) => return Err(ParserError::LeftParenthesisExpected(token, "column values")),
        None => return Err(ParserError::LeftParenthesisMissing("column values")),
    }

    loop {
        let value = match token.next() {
            Some(Token::Value(value)) => value.clone(),
            Some(token) => return Err(ParserError::ColumnValueInvalid(token)),
            None => return Err(ParserError::ColumnValueMissing),
        };

        values.push(value);

        match token.next() {
            Some(Token::RightParenthesis) => break,
            Some(Token::Comma) => { },
            Some(token) => return Err(ParserError::RightParenthesisExpected(token, "column values")),
            None => return Err(ParserError::RightParenthesisMissing("column values")),
        };
    }

    Ok(values)
}
