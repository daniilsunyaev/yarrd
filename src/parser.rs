use crate::command::{Command, MetaCommand};
use crate::lexer::Token;
use create::parse_create_statement;
use drop::parse_drop_statement;

mod create;
mod drop;

pub fn parse_statement<'a, I>(mut token: I) -> Result<Command, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let command = match token.next() {
        Some(Token::Create) => parse_create_statement(&mut token)?,
        Some(Token::Drop) => parse_drop_statement(&mut token)?,
        _ => return Err("cannot parse statement".to_string()),
    };

    println!("{:?}", command);
    let remainder = token.collect::<Vec<&Token>>();
    if !remainder.is_empty() {
        Err(format!("parsed correct statement, but some excess tokens are present in the input: {:?}", remainder))
    } else {
        Ok(command)
    }
}

pub fn parse_meta_command(input: &str) -> Result<Option<MetaCommand>, String> {
    if input.starts_with('.') {
        match input {
            ".exit" | ".quit" => Ok(Some(MetaCommand::Exit)),
            _ => Err(format!("unrecognized meta_command '{}'", input)),
        }
    } else {
        Ok(None)
    }
}
