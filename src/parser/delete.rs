use crate::command::Command;
use crate::lexer::Token;
use crate::parser::where_clause::parse_where_clause;

pub fn parse_delete_statement<'a, I>(mut token: I) -> Result<Command, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    match token.next() {
        Some(Token::From) => { },
        _ => return Err("expected FROM keyword".to_string()),
    };

    let table_name = match token.next() {
        Some(Token::Value(name)) => name.clone(),
        Some(token) => return Err(format!("expected a table name, got {:?}", token)),
        None => return Err("no table name is provided".to_string()),
    };

    match token.next() {
        Some(Token::Where) => {
            let where_clause = parse_where_clause(token)?;
            Ok(Command::Delete { table_name, where_clause: Some(where_clause) })
        },
        Some(token) => Err(format!("expected WHERE or end of statement, got {:?}", token)),
        None => Ok(Command::Delete { table_name, where_clause: None })
    }
}
