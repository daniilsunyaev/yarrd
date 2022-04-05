use crate::command::Command;
use crate::lexer::Token;
use crate::parser::ParserError;
use crate::parser::shared::parse_table_name;

pub fn parse_vacuum_statement<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    let table_name = parse_table_name(&mut token)?;
    Ok(Command::VacuumTable { table_name })
}
