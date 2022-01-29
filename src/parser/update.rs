use crate::command::{Command, FieldAssignment};
use crate::lexer::Token;
use crate::parser::where_clause::parse_where_clause;

pub fn parse_update_statement<'a, I>(mut token: I) -> Result<Command, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let table_name = match token.next() {
        Some(Token::Value(name)) => name.clone(),
        Some(token) => return Err(format!("expected a table name, got {:?}", token)),
        None => return Err("no table name is provided".to_string()),
    };

    let (field_assignments, where_provided) = parse_field_assignments(&mut token)?;

    let where_clause = if where_provided {
        Some(parse_where_clause(token)?)
    } else {
        None
    };

    Ok(Command::Update { table_name, field_assignments, where_clause })
}

fn parse_field_assignments<'a, I>(mut token: I) -> Result<(Vec<FieldAssignment>, bool), String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let mut field_assignments = vec![];

    match token.next() {
        Some(Token::Set) => { },
        Some(token) => return Err(format!("expected SET keyword, got {:?}", token)),
        None => return Err(format!("expected SET keyword, got {:?}", token))
    }

    let where_provided = loop {
        let column_name = match token.next() {
            Some(Token::Value(name)) => name.to_string(),
            Some(token) => return Err(format!("expected column name, got {:?}", token)),
            None => return Err("column name is not provided".to_string()),
        };

        match token.next() {
            Some(Token::Equals) => { },
            Some(token) => return Err(format!("expected assignment '=' keyword, got {:?}", token)),
            None => return Err("expected '=' keyword".to_string())
        }

        let value = match token.next() {
            Some(Token::Value(value)) => value.clone(),
            Some(token) => return Err(format!("expected column value, got {:?}", token)),
            None => return Err("column value is not provided".to_string()),
        };

        field_assignments.push(FieldAssignment { column_name, value });

        match token.next() {
            Some(Token::Where) => break(true),
            Some(Token::Comma) => { },
            Some(token) => return Err(format!("field assignment list is not finished, expected ',' or 'WHERE', got {:?}", token)),
            None => break(false)
        };
    };

    Ok((field_assignments, where_provided))
}
