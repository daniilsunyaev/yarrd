use crate::command::Command;
use crate::lexer::Token;
use crate::parser::where_clause::parse_where_clause;
use crate::parser::error::ParserError;
use crate::parser::shared::parse_table_name;

pub fn parse_delete_statement<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::From) => { },
        Some(token) => return Err(ParserError::FromExpected(token)),
        None => return Err(ParserError::FromMissing),
    };

    let table_name = parse_table_name(&mut token)?;

    match token.next() {
        Some(Token::Where) => {
            let where_clause = parse_where_clause(token)?;
            Ok(Command::Delete { table_name, where_clause: Some(where_clause) })
        },
        Some(token) => Err(ParserError::WhereExpected(token)),
        None => Ok(Command::Delete { table_name, where_clause: None })
    }
}
