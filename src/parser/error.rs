use std::error::Error;
use std::fmt;

use crate::parser::Token;
use crate::lexer::LexerError;

#[derive(Debug)]
pub enum ParserError<'a> {
    UnknownCommand(&'a Token),
    DatabasePathMissing,
    CouldNotParseDbFilename(&'a str),
    ExcessTokens(Vec<&'a Token>),
    CreateTypeMissing,
    CreateTypeUnknown(&'a Token),
    DropTypeMissing,
    DropTypeUnknown(&'a Token, &'static str),
    AddTypeMissing,
    AddTypeUnknown(&'a Token, &'static str),
    AlterTypeMissing,
    AlterTypeUnknown(&'a Token),
    AlterTableActionMissing,
    AlterTableActionUnknown(&'a Token),
    RenameTypeMissing,
    RenameTypeUnknown(&'a Token),
    RenameColumnToExpected(&'a Token),
    RenameColumnToMissing,
    InsertInvalid(&'a Token),
    IntoMissing,
    CreateIndexInvalid(&'a Token),
    CreateIndexOnMissing,
    TableNameInvalid(&'a Token),
    TableNameMissing,
    RowCountMissing,
    RowCountInvalid(String),
    LeftParenthesisExpected(&'a Token, &'static str),
    LeftParenthesisMissing(&'static str),
    RightParenthesisExpected(&'a Token, &'static str),
    RightParenthesisMissing(&'static str),
    CommaExpected(&'static str),
    ColumnNameInvalid(&'a Token),
    ColumnNameMissing,
    ColumnTypeInvalid(&'a Token),
    ColumnTypeMissing,
    IndexNameInvalid(&'a Token),
    IndexNameMissing,
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
    RvalueNotNull(&'a Token),
    UpdateSetMissing,
    UpdateSetExpected(&'a Token),
    EqualsExpected(&'a Token),
    EqualsMissing,
    AssignmentsInvalid(&'a Token),
    FromExpected(&'a Token),
    FromMissing,
    IntegerExpected(&'a Token),
    IntegerMissing,
    LexerError(LexerError),
    InvalidConstraint(Vec<&'a Token>),
    NoConstraintsGiven,
    MultipleConstraintsGiven,
    InvalidSchemaDefinition(String),
}

impl<'a> fmt::Display for ParserError<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            Self::UnknownCommand(command) => format!("unknown command '{}'", command),
            Self::DatabasePathMissing => "database name or path is not provided".to_string(),
            Self::CouldNotParseDbFilename(full_path_buf) =>
                format!("could not extract database filename from {}", full_path_buf),
            Self::ExcessTokens(tokens) =>
                format!("statement is correct, but contains excess tokens {:?}",
                        tokens.iter().map(|t| t.to_string()).collect::<Vec<String>>()),
            Self::CreateTypeMissing => "CREATE type is not provided".to_string(),
            Self::CreateTypeUnknown(create_type) =>
                format!("unknown CREATE type '{}', consider using CREATE TABLE", create_type),
            Self::DropTypeMissing => "DROP type is not provided".to_string(),
            Self::DropTypeUnknown(drop_type, considered) =>
                format!("unknown DROP type '{}', consider using DROP {}", drop_type, considered),
            Self::AddTypeMissing => "ADD type is not provided".to_string(),
            Self::AddTypeUnknown(drop_type, considered) =>
                format!("unknown ADD type '{}', consider using DROP {}", drop_type, considered),
            Self::AlterTypeMissing => "ALTER type is not provided".to_string(),
            Self::AlterTypeUnknown(alter_type) =>
                format!("unknown ALTER type '{}', consider using ALTER TABLE", alter_type),
            Self::AlterTableActionMissing => "ALTER TABLE action is not provided".to_string(),
            Self::AlterTableActionUnknown(action_type) =>
                format!("unknown ALTER TABLE action '{}', consider using ALTER TABLE <table_name> RENAME TO", action_type),
            Self::RenameTypeMissing => "RENAME type is not provided".to_string(),
            Self::RenameTypeUnknown(rename_type) =>
                format!("unknown RENAME type '{}', consider using RENAME TO", rename_type),
            Self::RenameColumnToMissing => "wrong RENAME syntax, expected 'TO', got nothing".to_string(),
            Self::RenameColumnToExpected(token) =>
                format!("wrong RENAME syntax, expected TO, got {}", token),
            Self::InsertInvalid(token) => format!("expected INSERT INTO, got INSERT {}", token),
            Self::IntoMissing => "expected INSERT INTO, got INSERT".to_string(),
            Self::CreateIndexInvalid(token) =>
                format!("expected CREATE INDEX index_name ON column_name, got CREATE INDEX index_name {}", token),
            Self::CreateIndexOnMissing => "expected CREATE INDEX index_name ON column_name, got CREATE INDEX".to_string(),
            Self::TableNameInvalid(table_name) => format!("'{}' is not a valid table name", table_name),
            Self::TableNameMissing => "table name is not provided".to_string(),
            Self::RowCountMissing => "row count is not provided".to_string(),
            Self::RowCountInvalid(token) => format!("row count is expected to be a positive number, '{}' provided instead", token),
            Self::LeftParenthesisExpected(token, entity) =>
                format!("{} expected to be inside parenthesis, but instead of '(' got '{}'",
                        entity, token),
            Self::LeftParenthesisMissing(entity) =>
                format!("{} expected to be described inside parenthesis, got nothing", entity),
            Self::RightParenthesisExpected(token, entity) =>
                format!("{} description is not finished, instead of ')' got '{}'", entity, token),
            Self::RightParenthesisMissing(entity) =>
                format!("{} description is not finished, ')' is missing", entity),
            Self::CommaExpected(entity) => format!("{} description is not finished, expected ',' or end of line", entity),
            Self::ColumnNameInvalid(name) => format!("{} is not a valid column name", name),
            Self::ColumnNameMissing => "column name is not provided".to_string(),
            Self::IndexNameInvalid(name) => format!("{} is not a valid index name", name),
            Self::IndexNameMissing => "index name is not provided".to_string(),
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
            Self::RvalueNotNull(token) => format!("expected IS NULL, got {}", token),
            Self::RvalueMissing => "where right value is not provided".to_string(),
            Self::UpdateSetMissing => "expected SET keyword, got nothing".to_string(),
            Self::UpdateSetExpected(token) => format!("expected SET keyword, got {}", token),
            Self::EqualsMissing => "expected '=' keyword, got nothing".to_string(),
            Self::EqualsExpected(token) => format!("expected assignment '=' keyword, got {}", token),
            Self::AssignmentsInvalid(token) => format!("field assignment list is not finished, expected ',' or 'WHERE', got {:?}", token),
            Self::FromExpected(token) => format!("expected FROM keyword, got {}", token),
            Self::FromMissing => "expected FROM keyword, got nothing".to_string(),
            Self::IntegerExpected(token) => format!("expected positive integer number, got {}", token),
            Self::IntegerMissing => "expected positive integer number, got nothing".to_string(),
            Self::LexerError(lexer_error) => format!("{}", lexer_error),
            Self::MultipleConstraintsGiven => "only one constraint is allowed, but several were given".to_string(),
            Self::NoConstraintsGiven => "no constraints were given".to_string(),
            Self::InvalidConstraint(tokens) =>
                format!("cannot treat constraint sequence '{:?}'",
                        tokens.iter().map(|t| t.to_string()).collect::<Vec<String>>()),
            Self::InvalidSchemaDefinition(message) => format!("cannot parse schema definition: {}", message),
        };

        write!(f, "{}", message)
    }
}

impl<'a> Error for ParserError<'a> {}
