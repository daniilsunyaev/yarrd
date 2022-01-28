use crate::where_clause::{WhereClause, CmpOperator};
use crate::lexer::Token;

pub fn parse_where_clause<'a, I>(mut token: I) -> Result<WhereClause, String>
where
    I: Iterator<Item = &'a Token> + std::fmt::Debug,
{
    let left_value = match token.next() {
        Some(Token::Value(sql_value)) => sql_value.clone(),
        Some(token) => return Err(format!("expected value or identifier, got {:?}", token)),
        None => return Err("where left value is not provided".to_string()),
    };

    let operator = match token.next() {
        Some(Token::Equals) => CmpOperator::Equals,
        Some(Token::NotEquals) => CmpOperator::NotEquals,
        Some(Token::Less) => CmpOperator::Less,
        Some(Token::Greater) => CmpOperator::Greater,
        Some(Token::LessEquals) => CmpOperator::LessEquals,
        Some(Token::GreaterEquals) => CmpOperator::GreaterEquals,
        Some(token) => return Err(format!("expected operator, got {:?}", token)),
        None => return Err("expected operator".to_string()),
    };

    let right_value = match token.next() {
        Some(Token::Value(sql_value)) => sql_value.clone(),
        Some(token) => return Err(format!("expected value or identifier, got {:?}", token)),
        None => return Err("where right value is not provided".to_string()),
    };

    Ok(WhereClause { left_value, right_value, operator })
}
