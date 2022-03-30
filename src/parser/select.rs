use crate::command::{Command, SelectColumnName};
use crate::lexer::Token;
use crate::parser::where_clause::parse_where_clause;
use crate::parser::error::ParserError;
use crate::parser::shared::parse_table_name;

pub fn parse_select_statement<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let column_names = parse_column_names(&mut token)?;
    let table_name = parse_table_name(&mut token)?;

    match token.next() {
        Some(Token::Where) => {
            let where_clause = parse_where_clause(token)?;
            Ok(Command::Select { column_names, table_name, where_clause: Some(where_clause) })
        },
        Some(token) => Err(ParserError::WhereExpected(token)),
        None => Ok(Command::Select { column_names, table_name, where_clause: None })
    }
}

fn parse_column_names<'a, I>(mut token: I) -> Result<Vec<SelectColumnName>, ParserError<'a>>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let mut columns = vec![];

    loop {
        let name = match token.next() {
            Some(Token::AllColumns) => SelectColumnName::AllColumns,
            Some(Token::Value(name)) => SelectColumnName::Name(name.clone()),
            Some(token) => return Err(ParserError::ColumnNameInvalid(token)),
            None => return Err(ParserError::ColumnNameMissing),
        };

        columns.push(name);

        match token.next() {
            Some(Token::From) => break,
            Some(Token::Comma) => { },
            Some(token) => return Err(ParserError::SelectColumnNamesInvalid(token)),
            None => return Err(ParserError::SelectColumnNamesNotFinished),
        };
    }

    Ok(columns)
}
