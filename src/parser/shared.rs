use crate::parser::error::ParserError;
use crate::lexer::{SqlValue, Token};
use crate::command::ColumnDefinition;
use crate::table::ColumnType;
use crate::table::Constraint;
use crate::cmp_operator::CmpOperator;
use crate::binary_condition::BinaryCondition;

pub fn parse_table_name<'a, I>(mut token: I) -> Result<SqlValue, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::Value(name)) => Ok(name.clone()),
        Some(token) => Err(ParserError::TableNameInvalid(token)),
        None => Err(ParserError::TableNameMissing),
    }
}

pub fn parse_column_name<'a, I>(mut token: I) -> Result<SqlValue, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::Value(name)) => Ok(name.clone()),
        Some(token) => Err(ParserError::ColumnNameInvalid(token)),
        None => Err(ParserError::ColumnNameMissing),
    }
}

pub fn parse_column_type<'a, I>(mut token: I) -> Result<ColumnType, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
        match token.next() {
            Some(Token::IntegerType) => Ok(ColumnType::Integer),
            Some(Token::FloatType) => Ok(ColumnType::Float),
            Some(Token::StringType) => Ok(ColumnType::String),
            Some(token) => Err(ParserError::ColumnTypeInvalid(token)),
            None => Err(ParserError::ColumnTypeMissing),
        }
}

pub fn parse_column_value<'a, I>(mut token: I) -> Result<SqlValue, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::Value(value)) => Ok(value.clone()),
        Some(token) => Err(ParserError::ColumnValueInvalid(token)),
        None => Err(ParserError::ColumnValueMissing),
    }
}

pub fn parse_left_parenthesis<'a, I>(mut token: I, entity: &'static str) -> Result<(), ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::LeftParenthesis) => Ok(()),
        Some(token) => Err(ParserError::LeftParenthesisExpected(token, entity)),
        None => Err(ParserError::LeftParenthesisMissing(entity)),
    }
}

pub fn parse_right_parenthesis<'a, I>(mut token: I, entity: &'static str) -> Result<(), ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::RightParenthesis) => Ok(()),
        Some(token) => return Err(ParserError::RightParenthesisExpected(token, entity)),
        None => return Err(ParserError::RightParenthesisMissing(entity)),
    }
}

pub fn parse_csl_right_parenthesis<'a, I>(mut token: I, entity: &'static str) -> Result<bool, ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    match token.next() {
        Some(Token::RightParenthesis) => Ok(true),
        Some(Token::Comma) => Ok(false),
        Some(token) => return Err(ParserError::RightParenthesisExpected(token, entity)),
        None => return Err(ParserError::RightParenthesisMissing(entity)),
    }
}

pub fn parse_column_definition<'a, I>(mut token: I) -> Result<(ColumnDefinition, Option<Token>), ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
        let name = parse_column_name(&mut token)?;
        let kind = parse_column_type(&mut token)?;
        let (constraint_tokens, last_token) = collect_constraint_tokens(&mut token)?;
        let column_constraints = parse_constraint_tokens(constraint_tokens)?;

        Ok((ColumnDefinition { name, kind, column_constraints }, last_token))
}

fn collect_constraint_tokens<'a, I>(mut token: I) -> Result<(Vec<&'a Token>, Option<Token>), ParserError<'a>>
where
    I: Iterator<Item = &'a Token>
{
    let mut tokens: Vec<&Token> = vec![];
    let mut parenthesis_depth = 0i32;
    loop {
        match token.next() {
            Some(Token::LeftParenthesis) => {
                parenthesis_depth += 1;
                tokens.push(&Token::LeftParenthesis);
            },
            Some(Token::RightParenthesis) => {
                parenthesis_depth -= 1;
                if parenthesis_depth < 0 {
                    return Ok((tokens, Some(Token::RightParenthesis)));
                } else {
                    tokens.push(&Token::RightParenthesis);
                }
            },
            Some(Token::Comma) => return Ok((tokens, Some(Token::Comma))),
            Some(token) => tokens.push(token),
            None => return Ok((tokens, None)),
        }
    }
}

pub fn parse_constraint_tokens(tokens: Vec<&Token>) -> Result<Vec<Constraint>, ParserError> {
    let mut iter = tokens.clone().into_iter();
    let mut result = vec![];
    println!("toketns: {:?}", tokens);

    loop {
        match iter.next() {
            Some(Token::Not) => {
                match iter.next() {
                    Some(Token::Value(SqlValue::Null)) => result.push(Constraint::NotNull),
                    _ => return Err(ParserError::InvalidConstraint(tokens)),
                }
            },
            Some(Token::Default) => {
                match iter.next() {
                    Some(Token::Value(value)) => result.push(Constraint::Default(value.clone())),
                    _ => return Err(ParserError::InvalidConstraint(tokens)),
                }
            },
            Some(Token::Check) => {
                parse_left_parenthesis(&mut iter, "check constraint definition")?;
                let condition = parse_binary_condition(&mut iter)?;
                result.push(Constraint::Check(condition));
                parse_right_parenthesis(&mut iter, "check constraint definition")?;
            }
            None => break,
            _ => return Err(ParserError::InvalidConstraint(tokens)),
        }
    }

    Ok(result)
}

pub fn parse_binary_condition<'a, I>(mut token: I) -> Result<BinaryCondition, ParserError<'a>>
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

    Ok(BinaryCondition { left_value, right_value, operator })
}
