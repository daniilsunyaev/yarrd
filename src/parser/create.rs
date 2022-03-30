use crate::command::{Command, ColumnDefinition};
use crate::table::ColumnType;
use crate::lexer::Token;
use crate::parser::error::ParserError;
use crate::parser::shared::
    {parse_table_name, parse_column_name, parse_left_parenthesis, parse_right_parenthesis};

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
    parse_left_parenthesis(&mut token, "column definitions")?;

    loop {
        let name = parse_column_name(&mut token)?;

        let kind = match token.next() {
            Some(Token::IntegerType) => ColumnType::Integer,
            Some(Token::FloatType) => ColumnType::Float,
            Some(Token::StringType) => ColumnType::String,
            Some(token) => return Err(ParserError::ColumnTypeInvalid(token)),
            None => return Err(ParserError::ColumnTypeMissing),
        };

        columns.push(ColumnDefinition { name, kind } );

        match parse_right_parenthesis(&mut token, "column definitions")? {
            true => break,
            false => { },
        };
    }

    Ok(columns)
}
