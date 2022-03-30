use crate::parser::error::ParserError;
use crate::lexer::{SqlValue, Token};

pub fn parse_table_name<'a, I>(mut token: I) -> Result<SqlValue, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::Value(name)) => Ok(name.clone()),
        Some(token) => Err(ParserError::TableNameInvalid(token)),
        None => Err(ParserError::TableNameMissing),
    }
}
