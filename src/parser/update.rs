use crate::command::{Command, FieldAssignment};
use crate::lexer::Token;
use crate::parser::where_clause::parse_where_clause;
use crate::parser::error::ParserError;
use crate::parser::shared::{parse_table_name, parse_column_name, parse_column_value};

pub fn parse_update_statement<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let table_name = parse_table_name(&mut token)?;
    let (field_assignments, where_provided) = parse_field_assignments(&mut token)?;

    let where_clause = if where_provided {
        Some(parse_where_clause(token)?)
    } else {
        None
    };

    Ok(Command::Update { table_name, field_assignments, where_clause })
}

fn parse_field_assignments<'a, I>(mut token: I) -> Result<(Vec<FieldAssignment>, bool), ParserError<'a>>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let mut field_assignments = vec![];

    match token.next() {
        Some(Token::Set) => { },
        Some(token) => return Err(ParserError::UpdateSetExpected(token)),
        None => return Err(ParserError::UpdateSetMissing),
    }

    let where_provided = loop {
        let column_name = parse_column_name(&mut token)?.to_string();

        match token.next() {
            Some(Token::Equals) => { },
            Some(token) => return Err(ParserError::EqualsExpected(token)),
            None => return Err(ParserError::EqualsMissing)
        }

        let value = parse_column_value(&mut token)?;
        field_assignments.push(FieldAssignment { column_name, value });

        match token.next() {
            Some(Token::Where) => break(true),
            Some(Token::Comma) => { },
            Some(token) => return Err(ParserError::AssignmentsInvalid(token)),
            None => break(false)
        };
    };

    Ok((field_assignments, where_provided))
}
