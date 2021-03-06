use crate::where_clause::WhereClause;
use crate::cmp_operator::CmpOperator;
use crate::lexer::Token;
use crate::parser::error::ParserError;
use crate::lexer::SqlValue;

pub fn parse_where_clause<'a, I>(mut token: I) -> Result<WhereClause, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    let left_value = match token.next() {
        Some(Token::Value(sql_value)) => sql_value.clone(),
        Some(token) => return Err(ParserError::LvalueInvalid(token)),
        None => return Err(ParserError::LvalueMissing),
    };

    let operator = match token.next() {
        Some(Token::Equals) => CmpOperator::Equals,
        Some(Token::NotEquals) => CmpOperator::NotEquals,
        Some(Token::Less) => CmpOperator::Less,
        Some(Token::Greater) => CmpOperator::Greater,
        Some(Token::LessEquals) => CmpOperator::LessEquals,
        Some(Token::GreaterEquals) => CmpOperator::GreaterEquals,
        Some(Token::Is) => CmpOperator::IsNull,
        Some(token) => return Err(ParserError::OperatorInvalid(token)),
        None => return Err(ParserError::OperatorMissing),
    };

    let next_token = token.next();
    let right_value = match next_token {
        Some(Token::Value(SqlValue::Null)) => SqlValue::Null,
        Some(Token::Value(sql_value)) => {
            match operator {
                CmpOperator::IsNull => return Err(ParserError::RvalueNotNull(next_token.unwrap())),
                _ => sql_value.clone(),
            }
        },
        Some(token) => return Err(ParserError::RvalueInvalid(token)),
        None => return Err(ParserError::RvalueMissing),
    };

    Ok(WhereClause { left_value, right_value, operator })
}
