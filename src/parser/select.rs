use crate::command::{Command, SelectColumnName};
use crate::lexer::Token;

pub fn parse_select_statement<'a, I>(mut token: I) -> Result<Command, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let column_names = parse_column_names(&mut token)?;

    let table_name = match token.next() {
        Some(Token::Value(name)) => name.clone(),
        Some(token) => return Err(format!("expected a table name, got {:?}", token)),
        None => return Err("no table name is provided".to_string()),
    };

    Ok(Command::Select { column_names, table_name })
}

fn parse_column_names<'a, I>(mut token: I) -> Result<Vec<SelectColumnName>, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let mut columns = vec![];

    loop {
        let name = match token.next() {
            Some(Token::AllColumns) => SelectColumnName::AllColumns,
            Some(Token::Value(name)) => SelectColumnName::Name(name.clone()),
            Some(token) => return Err(format!("expected column name, got {:?}", token)),
            None => return Err("column name is not provided".to_string()),
        };

        columns.push(name);

        match token.next() {
            Some(Token::From) => break,
            Some(Token::Comma) => { },
            Some(token) => return Err(format!("column names list is not finished, expected ',' or 'FROM', got {:?}", token)),
            None => return Err("columns names list is not finished".to_string()),
        };
    }

    Ok(columns)
}
