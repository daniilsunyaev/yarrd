use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Debug, PartialEq)]
pub enum Token {
    LeftParenthesis,
    RightParenthesis,
    Comma,
    Semicolon,
    Less,
    Greater,
    Equals,
    NotEquals,
    LessEquals,
    GreaterEquals,
    Insert,
    Into,
    On,
    Select,
    AllColumns,
    From,
    Where,
    Update,
    Set,
    Delete,
    Create,
    Drop,
    Alter,
    Rename,
    To,
    Add,
    Column,
    Table,
    Index,
    Values,
    Is,
    Not,
    Constraint,
    Default,
    Check,
    Vacuum,
    IntegerType, // TODO: maybe extract types to separate enum
    StringType,
    FloatType,
    Value(SqlValue),
    Unknown(String),
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let str_token = match self {
            Self::LeftParenthesis => "(",
            Self::RightParenthesis => ")",
            Self::Comma => ",",
            Self::Semicolon => ";",
            Self::Less => "<",
            Self::Greater => ">",
            Self::Equals => "=",
            Self::NotEquals => "<>",
            Self::LessEquals => "<=",
            Self::GreaterEquals => ">=",
            Self::Insert => "INSERT",
            Self::Into => "INTO",
            Self::On => "ON",
            Self::Select => "SELECT",
            Self::AllColumns => "*",
            Self::From => "FROM",
            Self::Where => "WHERE",
            Self::Update => "UPDATE",
            Self::Set => "SET",
            Self::Delete => "DELETE",
            Self::Create => "CREATE",
            Self::Drop => "DROP",
            Self::Alter => "ALTER",
            Self::Rename => "RENAME",
            Self::To => "TO",
            Self::Add => "ADD",
            Self::Column => "COLUMN",
            Self::Table => "TABLE",
            Self::Index => "INDEX",
            Self::Values => "VALUES",
            Self::Is => "IS",
            Self::Not => "NOT",
            Self::Vacuum => "VACUUM",
            Self::Constraint => "CONSTRAINT",
            Self::Default => "DEFAULT",
            Self::Check => "CHECK",
            Self::IntegerType => "int",
            Self::StringType => "string",
            Self::FloatType => "float",
            Self::Value(sql_value) => return write!(f, "{}", sql_value),
            Self::Unknown(string) => return write!(f, "{}", string),
        };
        write!(f, "{}", str_token)
    }
}

#[derive(Debug)]
pub enum LexerError {
    IncompleteString,
    UnknownToken(String),
}

impl fmt::Display for LexerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            LexerError::IncompleteString => "statement contain unclosed quotes".to_string(),
            LexerError::UnknownToken(token) => format!("token '{}' contains unallowed chars and cannot be recognized", token),
        };

        write!(f, "{}", message)
    }
}

impl Error for LexerError {}

#[derive(Debug, PartialEq, Clone)]
pub enum SqlValue {
    String(String),
    Integer(i64),
    Float(f64),
    Identificator(String),
    Null,
}

impl fmt::Display for SqlValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::String(string) | Self::Identificator(string) => write!(f, "{}", string),
            Self::Integer(integer) => write!(f, "{}", integer),
            Self::Float(float) => write!(f, "{:e}", float),
            Self::Null => write!(f, "NULL"),
        }
    }
}

impl Hash for SqlValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Float(_) => return,
            Self::String(string) => string.hash(state),
            Self::Integer(int) => int.hash(state),
            Self::Identificator(string) => string.hash(state),
            Self::Null => Self::Null.hash(state),
        }
    }
}

impl Token {
    pub fn is_junk(&self) -> bool {
        matches!(self, Token::Unknown(_))
    }
}

pub fn to_tokens(input: &str) -> Result<Vec<Token>, LexerError> {
    if input.matches('"').count() % 2 != 0 { return Err(LexerError::IncompleteString) };
    let input_chars_length = input.chars().count();

    let tokens: Vec<Token> = input.chars().enumerate()
        .fold((vec![0], false), |(mut separate_at, mut inside_string), (i, c)| {
            match c {
                '"' => {
                    if inside_string {
                        separate_at.push(i+1);
                    } else {
                        separate_at.push(i);
                    }
                    inside_string = !inside_string;
                },
                '(' | ')' | ',' | ' ' | ';' | '*' => {
                    if !inside_string {
                        separate_at.push(i);
                        separate_at.push(i+1);
                    }
                },
                _ => { },
            }

            if i == input_chars_length - 1 && separate_at[separate_at.len() - 1] != input_chars_length {
                separate_at.push(input_chars_length);
            }

            (separate_at, inside_string)
        }).0
        .windows(2)
        .map(|separator_indices| &input[separator_indices[0]..separator_indices[1]])
        .map(str::trim)
        .filter(|string| !string.is_empty())
        .map(parse_token)
        .collect();

    if let Some(Token::Unknown(input)) = tokens.iter().find(|el| el.is_junk()) {
        Err(LexerError::UnknownToken(input.to_string()))
    } else {
        Ok(tokens)
    }

}

fn parse_token(str_token: &str) -> Token {
    if str_token.starts_with('"') && str_token.ends_with('"') {
        return Token::Value(SqlValue::String(str_token[1..str_token.len()-1].to_string()))
    };

    match str_token.to_lowercase().as_str() {
        "=" => Token::Equals,
        "<>" => Token::NotEquals,
        ">" => Token::Greater,
        "<" => Token::Less,
        ">=" => Token::GreaterEquals,
        "<=" => Token::LessEquals,
        "(" => Token::LeftParenthesis,
        ")" => Token::RightParenthesis,
        "," => Token::Comma,
        ";" => Token::Semicolon,
        "*" => Token::AllColumns,
        "insert" => Token::Insert,
        "into" => Token::Into,
        "on" => Token::On,
        "select" => Token::Select,
        "from" => Token::From,
        "where" => Token::Where,
        "update" => Token::Update,
        "set" => Token::Set,
        "delete" => Token::Delete,
        "create" => Token::Create,
        "drop" => Token::Drop,
        "alter" => Token::Alter,
        "rename" => Token::Rename,
        "to" => Token::To,
        "add" => Token::Add,
        "column" => Token::Column,
        "table" => Token::Table,
        "index" => Token::Index,
        "values" => Token::Values,
        "is" => Token::Is,
        "not" => Token::Not,
        "vacuum" => Token::Vacuum,
        "constraint" => Token::Constraint,
        "default" => Token::Default,
        "check" => Token::Check,
        "int" => Token::IntegerType,
        "float" => Token::FloatType,
        "string" => Token::StringType,
        "null" => Token::Value(SqlValue::Null),
        _ => parse_sql_value(str_token).map(Token::Value)
            .unwrap_or_else(|| Token::Unknown(str_token.to_string())),
    }
}

fn parse_sql_value(str_token: &str) -> Option<SqlValue> {
    if let Ok(integer) = str_token.parse::<i64>() {
        Some(SqlValue::Integer(integer))
    } else if let Ok(float) = str_token.parse::<f64>() {
        Some(SqlValue::Float(float))
    } else if str_token.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.') {
        Some(SqlValue::Identificator(str_token.to_string()))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_parse() {
        let valid_input = "create TABLE,table_name RENAME Column not NULL Default add (row columnn type int float to string (,) ";
        let another_valid_input = "token*from alter CHECK foo ( < 2) Index";
        let invalid_input = "create (row \"column, type\" int string\" yy ";
        let another_invalid_input = ";123abc:";

        assert!(to_tokens(valid_input).is_ok());
        assert!(to_tokens(another_valid_input).is_ok());
        assert_eq!(
            to_tokens(valid_input).unwrap(),
            vec![
                Token::Create, Token::Table, Token::Comma, Token::Value(SqlValue::Identificator("table_name".into())),
                Token::Rename, Token::Column, Token::Not, Token::Value(SqlValue::Null), Token::Default,
                Token::Add, Token::LeftParenthesis, Token::Value(SqlValue::Identificator("row".into())),
                Token::Value(SqlValue::Identificator("columnn".into())), Token::Value(SqlValue::Identificator("type".into())),
                Token::IntegerType, Token::FloatType, Token::To, Token::StringType, Token::LeftParenthesis,
                Token::Comma, Token::RightParenthesis
            ]
        );
        assert_eq!(to_tokens(another_valid_input).unwrap(),
            vec![
                Token::Value(SqlValue::Identificator("token".to_string())), Token::AllColumns, Token::From, Token::Alter,
                Token::Check, Token::Value(SqlValue::Identificator("foo".into())), Token::LeftParenthesis,
                Token::Less, Token::Value(SqlValue::Integer(2)), Token::RightParenthesis, Token::Index
            ]);

        assert!(matches!(to_tokens(invalid_input), Err(LexerError::IncompleteString)));
        assert!(matches!(
                to_tokens(another_invalid_input),
                Err(LexerError::UnknownToken(_))
        ));
    }

    #[test]
    fn token_qoutes_parse() {
        let valid_input = "CrEAte vacuum (row NULL \"column, type\" constraint int -421 string 43.2552 \" ; \" on";

        assert!(to_tokens(valid_input).is_ok());
        assert_eq!(
            to_tokens(valid_input).unwrap(),
            vec![
                Token::Create, Token::Vacuum, Token::LeftParenthesis, Token::Value(SqlValue::Identificator("row".into())),
                Token::Value(SqlValue::Null), Token::Value(SqlValue::String("column, type".to_string())),
                Token::Constraint, Token::IntegerType, Token::Value(SqlValue::Integer(-421)), Token::StringType,
                Token::Value(SqlValue::Float(43.2552)), Token::Value(SqlValue::String(" ; ".into())), Token::On
            ]
        )
    }
}
