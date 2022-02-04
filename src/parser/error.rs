use std::error::Error;
use std::fmt;

use crate::parser::Token;

#[derive(Debug)]
pub enum ParserError<'a> {
    UnknownCommand(&'a Token),
    UnknownMetaCommand(&'a str),
    ExcessTokens(Vec<&'a Token>),
    CreateTypeMissing,
    CreateTypeUnknown(&'a Token),
    DropTypeMissing,
    DropTypeUnknown(&'a Token),
    InsertInvalid(&'a Token),
    IntoMissing,
    TableNameInvalid(&'a Token),
    TableNameMissing,
    LeftParenthesisExpected(&'a Token, &'static str),
    LeftParenthesisMissing(&'static str),
    RightParenthesisExpected(&'a Token, &'static str),
    RightParenthesisMissing(&'static str),
    ColumnNameInvalid(&'a Token),
    ColumnNameMissing,
    ColumnTypeInvalid(&'a Token),
    ColumnTypeMissing,
    ValuesKeywordMissing(&'a Token),
    InsertValuesMissing,
    ColumnValueMissing,
    ColumnValueInvalid(&'a Token),
    WhereExpected(&'a Token),
    SelectColumnNamesInvalid(&'a Token),
    SelectColumnNamesNotFinished,
    LvalueMissing,
    LvalueInvalid(&'a Token),
    OperatorMissing,
    OperatorInvalid(&'a Token),
    RvalueMissing,
    RvalueInvalid(&'a Token),
    UpdateSetMissing,
    UpdateSetExpected(&'a Token),
    EqualsExpected(&'a Token),
    EqualsMissing,
    AssignmentsInvalid(&'a Token),
    FromExpected(&'a Token),
    FromMissing,
}

impl<'a> fmt::Display for ParserError<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            Self::UnknownCommand(command) => format!("unknown command '{}'", command),
            Self::UnknownMetaCommand(command) => format!("unknown meta command '{}'", command),
            Self::ExcessTokens(tokens) =>
                format!("statement is correct, but contains excess tokens {:?}",
                        tokens.into_iter().map(|t| t.to_string()).collect::<Vec<String>>()),
            Self::CreateTypeMissing => "CREATE type is not provided".to_string(),
            Self::CreateTypeUnknown(create_type) =>
                format!("unknown CREATE type '{}', consider using CREATE TABLE", create_type),
            Self::DropTypeMissing => "DROP type is not provided".to_string(),
            Self::DropTypeUnknown(drop_type) =>
                format!("unknown DROP type '{}', consider using DROP TABLE", drop_type),
            Self::InsertInvalid(token) => format!("expected INSERT INTO, got INSERT {}", token),
            Self::IntoMissing => "expected INSERT INTO, got INSERT".to_string(),
            Self::TableNameInvalid(table_name) => format!("'{}' is not a valid table name", table_name),
            Self::TableNameMissing => "table name is not provided".to_string(),
            Self::LeftParenthesisExpected(token, entity) =>
                format!("{} expected to be inside parenthesis, but instead of '(' got '{}'",
                        entity, token),
            Self::LeftParenthesisMissing(entity) =>
                format!("{} expected to be described inside parenthesis, got nothing", entity),
            Self::RightParenthesisExpected(token, entity) =>
                format!("{} description is not finished, instead of ')' got '{}'", entity, token),
            Self::RightParenthesisMissing(entity) =>
                format!("{} description is not finished, ')' is missing", entity),
            Self::ColumnNameInvalid(name) => format!("{} is not a valid column name", name),
            Self::ColumnNameMissing => "column name is not provided".to_string(),
            Self::ColumnTypeInvalid(name) => format!("{} is not a valid column type", name),
            Self::ColumnTypeMissing => "column type is not provided".to_string(),
            Self::ValuesKeywordMissing(token) => format!("expected VALUES keyword, got '{}'", token),
            Self::InsertValuesMissing => "expected VALUES (...) to insert, got nothing".to_string(),
            Self::ColumnValueMissing => "column value is not provided".to_string(),
            Self::ColumnValueInvalid(token) => format!("expected column value, got {}", token),
            Self::WhereExpected(token) => format!("expected WHERE or end of statement, got {}", token),
            Self::SelectColumnNamesInvalid(token) => format!("column names list is not finished, expected ',' or 'FROM', got {}", token),
            Self::SelectColumnNamesNotFinished => "column names list is not finished, expected ',' or 'FROM'".to_string(),
            Self::LvalueInvalid(token) => format!("expected where left value or identifier, got {}", token),
            Self::LvalueMissing => "where left value is not provided".to_string(),
            Self::OperatorMissing => "no operator provided".to_string(),
            Self::OperatorInvalid(token) => format!("expected operator, got {}", token),
            Self::RvalueInvalid(token) => format!("expected where right value or identifier, got {}", token),
            Self::RvalueMissing => "where right value is not provided".to_string(),
            Self::UpdateSetMissing => "expected SET keyword, got nothing".to_string(),
            Self::UpdateSetExpected(token) => format!("expected SET keyword, got {}", token),
            Self::EqualsMissing => "expected '=' keyword, got nothing".to_string(),
            Self::EqualsExpected(token) => format!("expected assignment '=' keyword, got {}", token),
            Self::AssignmentsInvalid(token) => format!("field assignment list is not finished, expected ',' or 'WHERE', got {:?}", token),
            Self::FromExpected(token) => format!("expected FROM keyword, got {}", token),
            Self::FromMissing => "expected FROM keyword, got nothing".to_string()
        };

        write!(f, "{}", message)
    }
}

impl<'a> Error for ParserError<'a> {}