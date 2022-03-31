use crate::command::Command;
use crate::lexer::Token;
use crate::parser::error::ParserError;
use crate::parser::shared::{parse_table_name, parse_column_name, parse_column_definition};
use crate::lexer::SqlValue;

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
                Some(Token::To) => parse_rename_table(token, table_name),
                Some(Token::Column) => parse_rename_column(token, table_name),
                None => Err(ParserError::RenameTypeMissing),
                Some(token) =>  Err(ParserError::RenameTypeUnknown(token)),
            }
        },
        Some(Token::Add) => parse_add_column(token, table_name),
        Some(Token::Drop) => parse_drop_entity(token, table_name),
        None => Err(ParserError::AlterTableActionMissing),
        Some(token) => Err(ParserError::AlterTableActionUnknown(token)),
    }
}

fn parse_rename_table<'a, I>(mut token: I, table_name: SqlValue) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    let new_table_name = parse_table_name(&mut token)?;
    Ok(Command::RenameTable { table_name, new_table_name })
}

fn parse_rename_column<'a, I>(mut token: I, table_name: SqlValue) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    let column_name = parse_column_name(&mut token)?;
    match token.next() {
        Some(Token::To) => {
            let new_column_name = parse_column_name(&mut token)?;
            Ok(Command::RenameTableColumn {
                table_name,
                column_name,
                new_column_name,
            })
        },
        None => Err(ParserError::RenameColumnToMissing),
        Some(token) => Err(ParserError::RenameColumnToExpected(token)),
    }
}

fn parse_add_column<'a, I>(mut token: I, table_name: SqlValue) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    let column_definition = parse_column_definition(&mut token)?;

    Ok(Command::AddTableColumn { table_name, column_definition })
}

fn parse_drop_entity<'a, I>(mut token: I, table_name: SqlValue) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::Column) => {
            let column_name = parse_column_name(token)?;
            Ok(Command::DropTableColumn { table_name, column_name })
        },
        None => Err(ParserError::DropTypeMissing),
        Some(token) => Err(ParserError::DropTypeUnknown(token, "COLUMN")),
    }
}
