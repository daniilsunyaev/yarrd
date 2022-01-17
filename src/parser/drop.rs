use crate::command::Command;
use crate::lexer::Token;

pub fn parse_drop_statement<'a, I>(mut token: I) -> Result<Command, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    match token.next() {
        Some(Token::Table) => parse_drop_table_clause(token),
        None => Err("drop type is not provided".to_string()),
        _ => Err("unknown drop type".to_string()),
    }
}

fn parse_drop_table_clause<'a, I>(mut token: I) -> Result<Command, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{

    let table_name = match token.next() {
        Some(Token::Identificator(name)) => name.clone(),
        _ => return Err(format!("expected identificator as a table name, got {:?}", token)),
    };

    Ok(Command::DropTable { table_name })
}
