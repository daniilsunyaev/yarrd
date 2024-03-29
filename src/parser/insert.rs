use crate::command::Command;
use crate::lexer::{Token, SqlValue};
use crate::parser::error::ParserError;
use crate::parser::shared::{parse_table_name, parse_column_value, parse_left_parenthesis,
    parse_csl_right_parenthesis, parse_parenthesised_cs_column_names};

pub fn parse_insert_statement<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::Into) => parse_insert_into(token),
        Some(token) => Err(ParserError::InsertInvalid(token)),
        None => Err(ParserError::IntoMissing),
    }
}

fn parse_insert_into<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    let table_name = parse_table_name(&mut token)?;
    let column_names = parse_parenthesised_cs_column_names(&mut token)?;
    let values = parse_values_expression(&mut token)?;

    Ok(Command::InsertInto { table_name, column_names: Some(column_names), values })
}

fn parse_values_expression<'a, I>(mut token: I) -> Result<Vec<SqlValue>, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    let mut values = vec![];

    match token.next() {
        Some(Token::Values) => { },
        Some(token) => return Err(ParserError::ValuesKeywordMissing(token)),
        None => return Err(ParserError::InsertValuesMissing),
    }

    parse_left_parenthesis(&mut token, "column values")?;

    loop {
        let value = parse_column_value(&mut token)?;
        values.push(value);

        match parse_csl_right_parenthesis(&mut token, "column values")? {
            true => break,
            false => { },
        };
    }

    Ok(values)
}
