use crate::command::{Command, ColumnDefinition};
use crate::lexer::Token;
use crate::parser::error::ParserError;
use crate::parser::shared::
    {parse_table_name, parse_left_parenthesis, parse_csl_right_parenthesis, parse_column_definition};

pub fn parse_create_statement<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::Table) => parse_create_table_statement(token),
        None => Err(ParserError::CreateTypeMissing),
        Some(token) => Err(ParserError::CreateTypeUnknown(token)),
    }
}

fn parse_create_table_statement<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    let table_name = parse_table_name(&mut token)?;
    let column_definitions = parse_column_definitions(&mut token)?;
    Ok(Command::CreateTable { table_name, columns: column_definitions })
}

fn parse_column_definitions<'a, I>(mut token: I) -> Result<Vec<ColumnDefinition>, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    let mut columns = vec![];
    parse_left_parenthesis(&mut token, "column definitions")?;

    loop {
        let (column, last_token) = parse_column_definition(&mut token)?;
        columns.push(column);

        match last_token {
            Some(Token::Comma) => continue,
            Some(Token::RightParenthesis) => break,
            _ => return Err(ParserError::RightParenthesisMissing("column_definitions")),
        }
    }

    Ok(columns)
}
