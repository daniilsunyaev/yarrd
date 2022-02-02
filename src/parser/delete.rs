use crate::command::Command;
use crate::lexer::Token;
use crate::parser::where_clause::parse_where_clause;
use crate::parser::error::ParserError;

pub fn parse_delete_statement<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    match token.next() {
        Some(Token::From) => { },
        Some(token) => return Err(ParserError::FromExpected(token)),
        None => return Err(ParserError::FromMissing),
    };

    let table_name = match token.next() {
        Some(Token::Value(name)) => name.clone(),
        Some(token) => return Err(ParserError::TableNameInvalid(token)),
        None => return Err(ParserError::TableNameMissing),
    };

    match token.next() {
        Some(Token::Where) => {
            let where_clause = parse_where_clause(token)?;
            Ok(Command::Delete { table_name, where_clause: Some(where_clause) })
        },
        Some(token) => Err(ParserError::WhereExpected(token)),
        None => Ok(Command::Delete { table_name, where_clause: None })
    }
}
