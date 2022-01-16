use crate::command::{Command, ColumnDefinition};
use crate::table::ColumnType;
use crate::lexer::Token;

pub fn parse_statement<'a, I>(mut token: I) -> Result<Command, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let command = match token.next() {
        Some(Token::Create) => parse_create_statement(&mut token)?,
        Some(Token::Drop) => parse_drop_statement(&mut token)?,
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

fn parse_create_statement<'a, I>(mut token: I) -> Result<Command, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    match token.next() {
        Some(Token::Table) => { parse_create_table_clause(token) },
        None => return Err("create type is not provided".to_string()),
        _ => return Err("unknown create type".to_string()),
    }
}

fn parse_create_table_clause<'a, I>(mut token: I) -> Result<Command, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{

    let table_name = match token.next() {
        Some(Token::Identificator(name)) => name.clone(),
        Some(token) => return Err(format!("expected identificator as a table name, got {:?}", token)),
        None => return Err("no table name is provided".to_string()),
    };

    let column_definitions = parse_column_definitions(token)?;
    println!("cd {:?}", column_definitions);
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
            Some(Token::Identificator(name)) => name.clone(),
            Some(token) => return Err(format!("expected column name, got {:?}", token)),
            None => return Err("column name is not provided".to_string()),
        };

        let kind = match token.next() {
            Some(Token::Integer) => ColumnType::Integer,
            Some(Token::String) => ColumnType::String,
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

fn parse_drop_statement<'a, I>(mut token: I) -> Result<Command, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    match token.next() {
        Some(Token::Table) => { parse_drop_table_clause(token) },
        None => return Err("drop type is not provided".to_string()),
        _ => return Err("unknown drop type".to_string()),
    }
}

fn parse_drop_table_clause<'a, I>(mut token: I) -> Result<Command, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{

    let table_name = match token.next() {
        Some(Token::Identificator(name)) => name.clone(),
        _ => return Err(format!("expected identificator as a table name, got {:?}", token)),
    };

    Ok(Command::DropTable { table_name })
}
