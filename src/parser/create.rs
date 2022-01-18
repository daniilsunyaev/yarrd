use crate::command::{Command, ColumnDefinition};
use crate::table::ColumnType;
use crate::lexer::Token;

pub fn parse_create_statement<'a, I>(mut token: I) -> Result<Command, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    match token.next() {
        Some(Token::Table) => parse_create_table_statement(token),
        None => Err("create type is not provided".to_string()),
        _ => Err("unknown create type".to_string()),
    }
}

fn parse_create_table_statement<'a, I>(mut token: I) -> Result<Command, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{

    let table_name = match token.next() {
        Some(Token::Value(name)) => name.clone(),
        Some(token) => return Err(format!("expected identificator as a table name, got {:?}", token)),
        None => return Err("no table name is provided".to_string()),
    };

    let column_definitions = parse_column_definitions(token)?;
    Ok(Command::CreateTable { table_name, columns: column_definitions })
}

fn parse_column_definitions<'a, I>(mut token: I) -> Result<Vec<ColumnDefinition>, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let mut columns = vec![];

    match token.next() {
        Some(Token::LeftParenthesis) => { },
        Some(token) => return Err(format!("column definitions expected to be inside parenthesis, but instead of '(' got {:?}", token)),
        None => return Err("column definitions expected to be inside parenthesis".to_string()),
    }

    loop {
        let name = match token.next() {
            Some(Token::Value(name)) => name.clone(),
            Some(token) => return Err(format!("expected column name, got {:?}", token)),
            None => return Err("column name is not provided".to_string()),
        };

        let kind = match token.next() {
            Some(Token::IntegerType) => ColumnType::Integer,
            Some(Token::StringType) => ColumnType::String,
            Some(token) => return Err(format!("expected column type, got {:?}", token)),
            None => return Err("column type is not provided".to_string()),
        };

        columns.push(ColumnDefinition { name, kind } );

        match token.next() {
            Some(Token::RightParenthesis) => break,
            Some(Token::Comma) => { },
            Some(token) => return Err(format!("columns definition is not finished, expected ',' or ')', got {:?}", token)),
            None => return Err("columns definition is not finished".to_string()),
        };
    }

    Ok(columns)
}
