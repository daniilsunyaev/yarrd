use crate::command::Command;
use crate::lexer::Token;
use crate::parser::error::ParserError;
use crate::parser::shared::parse_table_name;

pub fn parse_alter_statement<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::Table) => parse_alter_table_statement(token),
        None => Err(ParserError::AlterTypeMissing),
        Some(token) => Err(ParserError::AlterTypeUnknown(token)),
    }
}

fn parse_alter_table_statement<'a, I>(mut token: I) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    let table_name = parse_table_name(&mut token)?;
    match token.next() {
        Some(Token::Rename) => {
            match token.next() {
                Some(Token::To) => {
                    let new_table_name = parse_table_name(&mut token)?;
                    Ok(Command::RenameTable { table_name, new_table_name })
                },
                None => Err(ParserError::RenameTypeMissing),
                Some(token) =>  Err(ParserError::RenameTypeUnknown(token)),
            }
        },
        None => Err(ParserError::AlterTableActionMissing),
        Some(token) => Err(ParserError::AlterTableActionUnknown(token)),
    }
}
