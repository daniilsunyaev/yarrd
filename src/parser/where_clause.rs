use crate::binary_condition::BinaryCondition;
use crate::lexer::Token;
use crate::parser::error::ParserError;
use crate::parser::shared::parse_binary_condition;

pub fn parse_where_clause<'a, I>(mut token: I) -> Result<BinaryCondition, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    parse_binary_condition(&mut token)
}
