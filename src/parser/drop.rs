use crate::command::Command;
use crate::lexer::Token;
use crate::parser::ParserError;
use crate::parser::shared::parse_table_name;

pub fn parse_drop_statement<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::Table) => parse_drop_table_clause(token),
        None => Err(ParserError::DropTypeMissing),
        Some(token) => Err(ParserError::DropTypeUnknown(token, "TABLE")),
    }
}

fn parse_drop_table_clause<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    let table_name = parse_table_name(&mut token)?;
    Ok(Command::DropTable { table_name })
}
