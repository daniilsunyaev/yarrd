use crate::command::{Command, ColumnDefinition};
use crate::table::ColumnType;
use crate::lexer::Token;
use crate::parser::error::ParserError;
use crate::parser::shared::parse_table_name;

pub fn parse_create_statement<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    match token.next() {
        Some(Token::Table) => parse_create_table_statement(token),
        None => Err(ParserError::CreateTypeMissing),
        Some(token) => Err(ParserError::CreateTypeUnknown(token)),
    }
}

fn parse_create_table_statement<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{

    let table_name = parse_table_name(&mut token)?;
    let column_definitions = parse_column_definitions(&mut token)?;
    Ok(Command::CreateTable { table_name, columns: column_definitions })
}

fn parse_column_definitions<'a, I>(mut token: I) -> Result<Vec<ColumnDefinition>, ParserError<'a>>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let mut columns = vec![];

    match token.next() {
        Some(Token::LeftParenthesis) => { },
        Some(token) => return Err(ParserError::LeftParenthesisExpected(token, "column definitions")),
        None => return Err(ParserError::LeftParenthesisMissing("column definitions")),
    }

    loop {
        let name = match token.next() {
            Some(Token::Value(name)) => name.clone(),
            Some(token) => return Err(ParserError::ColumnNameInvalid(token)),
            None => return Err(ParserError::ColumnNameMissing),
        };

        let kind = match token.next() {
            Some(Token::IntegerType) => ColumnType::Integer,
            Some(Token::FloatType) => ColumnType::Float,
            Some(Token::StringType) => ColumnType::String,
            Some(token) => return Err(ParserError::ColumnTypeInvalid(token)),
            None => return Err(ParserError::ColumnTypeMissing),
        };

        columns.push(ColumnDefinition { name, kind } );

        match token.next() {
            Some(Token::RightParenthesis) => break,
            Some(Token::Comma) => { },
            Some(token) => return Err(ParserError::RightParenthesisExpected(token, "column definitions")),
            None => return Err(ParserError::RightParenthesisMissing("column definitions")),
        };
    }

    Ok(columns)
}
