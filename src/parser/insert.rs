use crate::command::Command;
use crate::lexer::{Token, SqlValue};

pub fn parse_insert_statement<'a, I>(mut token: I) -> Result<Command, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    match token.next() {
        Some(Token::Into) => parse_insert_into(token),
        None => Err("create type is not provided".to_string()),
        _ => Err("unknown create type".to_string()),
    }
}

fn parse_insert_into<'a, I>(mut token: I) -> Result<Command, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{

    let table_name = match token.next() {
        Some(Token::Value(name)) => name.clone(),
        Some(token) => return Err(format!("expected identificator as a table name, got {:?}", token)),
        None => return Err("no table name is provided".to_string()),
    };

    let column_names = parse_column_names(&mut token)?;
    let values = parse_values_expression(&mut token)?;

    Ok(Command::InsertInto { table_name, column_names: Some(column_names), values })
}

fn parse_column_names<'a, I>(mut token: I) -> Result<Vec<SqlValue>, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let mut columns = vec![];

    match token.next() {
        Some(Token::LeftParenthesis) => { },
        Some(token) => return Err(format!("column names expected to be inside parenthesis, but instead of '(' got {:?}", token)),
        None => return Err("column names expected to be inside parenthesis".to_string()),
    }

    loop {
        let name = match token.next() {
            Some(Token::Value(name)) => name.clone(),
            Some(token) => return Err(format!("expected column name, got {:?}", token)),
            None => return Err("column name is not provided".to_string()),
        };

        columns.push(name);

        match token.next() {
            Some(Token::RightParenthesis) => break,
            Some(Token::Comma) => { },
            Some(token) => return Err(format!("column names list is not finished, expected ',' or ')', got {:?}", token)),
            None => return Err("columns names list is not finished".to_string()),
        };
    }

    Ok(columns)
}

fn parse_values_expression<'a, I>(mut token: I) -> Result<Vec<SqlValue>, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let mut values = vec![];

    match token.next() {
        Some(Token::Values) => { },
        Some(token) => return Err(format!("column VALUES keyword, got {:?}", token)),
        None => return Err("no column values provided".to_string()),
    }

    match token.next() {
        Some(Token::LeftParenthesis) => { },
        Some(token) => return Err(format!("column values expected to be inside parenthesis, but instead of '(' got {:?}", token)),
        None => return Err("column values expected to be inside parenthesis".to_string()),
    }

    loop {
        let value = match token.next() {
            Some(Token::Value(value)) => value.clone(),
            Some(token) => return Err(format!("expected column value, got {:?}", token)),
            None => return Err("column value is not provided".to_string()),
        };

        values.push(value);

        match token.next() {
            Some(Token::RightParenthesis) => break,
            Some(Token::Comma) => { },
            Some(token) => return Err(format!("column values list is not finished, expected ',' or ')', got {:?}", token)),
            None => return Err("column values list is not finished".to_string()),
        };
    }

    Ok(values)
}
