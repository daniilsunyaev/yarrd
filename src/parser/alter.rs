use crate::command::Command;
use crate::lexer::Token;
use crate::table::Constraint;
use crate::parser::error::ParserError;
use crate::parser::shared::{parse_table_name, parse_column_name, parse_column_definition, parse_constraint_tokens};
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
        Some(Token::Add) => parse_add_entity(token, table_name),
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

fn parse_add_entity<'a, I>(mut token: I, table_name: SqlValue) -> Result<Command, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::Column) => {
            let (column_definition, _) = parse_column_definition(&mut token)?;
            Ok(Command::AddTableColumn { table_name, column_definition })
        },
        Some(Token::Constraint) => {
            let (column_name, constraint) = parse_column_constraint(&mut token)?;
            Ok(Command::AddColumnConstraint { table_name, column_name, constraint })
        },
        None => Err(ParserError::AddTypeMissing),
        Some(token) => Err(ParserError::AddTypeUnknown(token, "COLUMN")),
    }
}

fn parse_column_constraint<'a, I>(mut token: I) -> Result<(SqlValue, Constraint), ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    let mut constraint_tokens = vec![];
    loop {
        match token.next() {
            Some(Token::LeftParenthesis) => break,
            Some(token) => constraint_tokens.push(token),
            None => return Err(ParserError::LeftParenthesisMissing("column name")),
        }
    }

    let column_name = parse_column_name(&mut token)?;
    match token.next() {
        Some(Token::RightParenthesis) => {},
        Some(token) => return Err(ParserError::RightParenthesisExpected(token, "column name")),
        None => return Err(ParserError::RightParenthesisMissing("column name")),
    }

    let mut constraints = parse_constraint_tokens(constraint_tokens)?;
    match constraints.len() {
        0 => return Err(ParserError::NoConstraintsGiven),
        1 => {},
        _ => return Err(ParserError::MultipleConstraintsGiven),
    }
    let constraint = constraints.pop().unwrap();


    Ok((column_name, constraint))
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
        Some(Token::Constraint) => {
            let (column_name, constraint) = parse_column_constraint(&mut token)?;
            Ok(Command::DropColumnConstraint { table_name, column_name, constraint })
        }
        None => Err(ParserError::DropTypeMissing),
        Some(token) => Err(ParserError::DropTypeUnknown(token, "COLUMN")),
    }
}
