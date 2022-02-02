use std::error::Error;
use std::fmt;

#[derive(Debug, PartialEq)] // TODO: impl display to display in error messages
pub enum Token {
    LeftParenthesis,
    RightParenthesis,
    Comma,
    Less,
    Greater,
    Equals,
    NotEquals,
    LessEquals,
    GreaterEquals,
    Insert,
    Into,
    Select,
    AllColumns,
    From,
    Where,
    Update,
    Set,
    Delete,
    Create,
    Drop,
    Table,
    Values,
    IntegerType, // TODO: maybe extract types to separate enum
    StringType,
    Value(SqlValue),
    Unknown(String),
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let str_token = match self {
            Self::LeftParenthesis => "(",
            Self::RightParenthesis => ")",
            Self::Comma => ",",
            Self::Less => "<",
            Self::Greater => ">=",
            Self::Equals => "=",
            Self::NotEquals => "<>",
            Self::LessEquals => "<=",
            Self::GreaterEquals => ">=",
            Self::Insert => "INSERT",
            Self::Into => "INTO",
            Self::Select => "SELECT",
            Self::AllColumns => "*",
            Self::From => "FROM",
            Self::Where => "WHERE",
            Self::Update => "UPDATE",
            Self::Set => "SET",
            Self::Delete => "DELETE",
            Self::Create => "CREATE",
            Self::Drop => "DROP",
            Self::Table => "TABLE",
            Self::Values => "VALUES",
            Self::IntegerType => "int",
            Self::StringType => "string",
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

#[derive(Debug, PartialEq, Clone)] // TODO: display instead of debug in error messages
pub enum SqlValue {
    String(String),
    Integer(i64),
    Identificator(String),
    Null,
}

impl fmt::Display for SqlValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::String(string) | Self::Identificator(string) => write!(f, "{}", string),
            Self::Integer(integer) => write!(f, "{}", integer),
            Self::Null => write!(f, "NULL"),
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

    match str_token {
        "=" => Token::Equals,
        "<>" => Token::NotEquals,
        ">" => Token::Greater,
        "<" => Token::Less,
        ">=" => Token::GreaterEquals,
        "<=" => Token::LessEquals,
        "(" => Token::LeftParenthesis,
        ")" => Token::RightParenthesis,
        "," => Token::Comma,
        "*" => Token::AllColumns,
        "insert" => Token::Insert,
        "into" => Token::Into,
        "select" => Token::Select,
        "from" => Token::From,
        "where" => Token::Where,
        "update" => Token::Update,
        "set" => Token::Set,
        "delete" => Token::Delete,
        "create" => Token::Create,
        "drop" => Token::Drop,
        "table" => Token::Table,
        "values" => Token::Values,
        "int" => Token::IntegerType,
        "string" => Token::StringType,
        "NULL" => Token::Value(SqlValue::Null),
        _ => parse_sql_value(str_token).map(Token::Value)
            .unwrap_or_else(|| Token::Unknown(str_token.to_string())),
    }
}

fn parse_sql_value(str_token: &str) -> Option<SqlValue> {
    if let Ok(number) = str_token.parse::<i64>() {
        Some(SqlValue::Integer(number))
    } else if str_token.chars().all(|c| c.is_alphanumeric() || c == '_') {
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
        let valid_input = "create table,table_name (row column type int string (,) ";
        let another_valid_input = "token*from";
        let invalid_input = "create (row \"column, type\" int string\" yy ";
        let another_invalid_input = ";123abc";

        assert!(to_tokens(valid_input).is_ok());
        assert!(to_tokens(another_valid_input).is_ok());
        assert_eq!(
            to_tokens(valid_input).unwrap(),
            vec![
                Token::Create, Token::Table, Token::Comma, Token::Value(SqlValue::Identificator("table_name".into())),
                Token::LeftParenthesis, Token::Value(SqlValue::Identificator("row".into())),
                Token::Value(SqlValue::Identificator("column".into())), Token::Value(SqlValue::Identificator("type".into())),
                Token::IntegerType, Token::StringType, Token::LeftParenthesis,
                Token::Comma, Token::RightParenthesis
            ]
        );
        assert_eq!(to_tokens(another_valid_input).unwrap(),
            vec![Token::Value(SqlValue::Identificator("token".to_string())), Token::AllColumns, Token::From]);

        assert!(matches!(to_tokens(invalid_input), Err(LexerError::IncompleteString)));
        assert!(matches!(
                to_tokens(another_invalid_input),
                Err(LexerError::UnknownToken(_))
        ));
    }

    #[test]
    fn token_qoutes_parse() {
        let valid_input = "create (row \"column, type\" int -421 string\" ; \"";

        assert!(to_tokens(valid_input).is_ok());
        assert_eq!(
            to_tokens(valid_input).unwrap(),
            vec![
                Token::Create, Token::LeftParenthesis, Token::Value(SqlValue::Identificator("row".into())),
                Token::Value(SqlValue::String("column, type".to_string())), Token::IntegerType,
                Token::Value(SqlValue::Integer(-421)), Token::StringType, Token::Value(SqlValue::String(" ; ".into()))
            ]
        )
    }
}
