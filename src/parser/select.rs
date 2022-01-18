use crate::command::{Command, SelectColumnName};
use crate::lexer::Token;
use crate::parser::where_clause::parse_where_clause;

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

    match token.next() {
        Some(Token::Where) => {
            let where_clause = parse_where_clause(token)?;
            Ok(Command::Select { column_names, table_name, where_clause: Some(where_clause) })
        },
        Some(token) => Err(format!("expected WHERE or end of statement, got {:?}", token)),
        None => Ok(Command::Select { column_names, table_name, where_clause: None })
    }
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
