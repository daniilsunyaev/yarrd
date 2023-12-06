use crate::command::Command;
use crate::lexer::Token;
use crate::parser::ParserError;
use crate::parser::shared::parse_table_name;
use crate::parser::shared::parse_column_name;

pub fn parse_reindex_statement<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    let table_name = parse_table_name(&mut token)?;

    match parse_column_name(&mut token) {
        Ok(column_name) => Ok(Command::Reindex { table_name, column_name: Some(column_name) }),
        Err(ParserError::ColumnNameMissing) => Ok(Command::Reindex { table_name, column_name: None }),
        Err(error) => Err(error),
    }
}
