use crate::command::Command;

#[derive(Debug, PartialEq)] // TODO: impl display to display in error messages
pub enum Token {
    Exit,
    LeftParenthesis,
    RightParenthesis,
    Comma,
    Insert,
    Select,
    Update,
    Delete,
    Create,
    Drop,
    Table,
    Integer,
    String,
    Identificator(String),
}

impl Token {
    pub fn is_type(&self) -> bool {
        match self {
            Token::Integer | Token::String => true,
            _ => false,
        }
    }

    pub fn is_identificator(&self) -> bool {
        match self {
            Token::Identificator(_) => true,
            _ => false,
        }
    }

    pub fn is_metacommand(&self) -> bool {
        match self {
            Token::Exit => true,
            _ => false,
        }
    }

    pub fn is_keyword(&self) -> bool {
        match self {
            Token::Insert | Token::Select | Token::Update | Token::Delete |
                Token::Create | Token::Drop | Token::Table => true,
            _ => false,
        }
    }
}

// TODO: change it to result
pub fn to_tokens(input: &str) -> Vec<Token> {
    input.chars().enumerate()
        .fold(vec![0], |mut separate_at, (i, c)| {
            match c {
                '(' | ')' | ',' | ' ' | ';' => {
                    separate_at.push(i);
                    separate_at.push(i+1);
                },
                _ => { },
            }
            separate_at
        })
        .windows(2)
        .map(|separator_indices| &input[separator_indices[0]..separator_indices[1]])
        .map(str::trim)
        .filter(|string| !string.is_empty())
        .map(parse_token)
        .collect()
}

fn parse_token(str_token: &str) -> Token {
    match str_token {
        ".exit" | ".quit" => Token::Exit,
        "(" => Token::LeftParenthesis,
        ")" => Token::RightParenthesis,
        "," => Token::Comma,
        "insert" => Token::Insert,
        "select" => Token::Select,
        "update" => Token::Update,
        "delete" => Token::Delete,
        "create" => Token::Create,
        "drop" => Token::Drop,
        "table" => Token::Table,
        "int" => Token::Integer,
        "string" => Token::String,
        _ => Token::Identificator(str_token.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_parse() {
        let input = "create table,table_name (row column type int string (,); ";

        assert_eq!(
            to_tokens(input),
            vec![
                Token::Create, Token::Table, Token::Comma, Token::Identificator("table_name".into()),
                Token::LeftParenthesis, Token::Identificator("row".into()),
                Token::Identificator("column".into()), Token::Identificator("type".into()),
                Token::Integer, Token::String, Token::LeftParenthesis,
                Token::Comma, Token::RightParenthesis, Token::Identificator(";".into())
            ]
        )
    }
}
