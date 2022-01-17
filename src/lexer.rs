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
    IntegerType,
    StringType,
    StringValue(String),
    IntegerValue(i64),
    Identificator(String),
    JunkIdentificator(String),
}

impl Token {
    pub fn is_junk(&self) -> bool {
        match self {
            Token::JunkIdentificator(_) => true,
            _ => false,
        }
    }
}

pub fn to_tokens(input: &str) -> Result<Vec<Token>, String> {
    if input.matches('"').count() % 2 != 0 { return Err("statement contain unclosed quotes".to_string()) };
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
                '(' | ')' | ',' | ' ' | ';' => {
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

    if let Some(token) = tokens.iter().find(|el| el.is_junk()) {
        Err(format!("name '{:?}' contains non alphanumertic chars", token))
    } else {
        Ok(tokens)
    }

}

fn parse_token(str_token: &str) -> Token {
    if str_token.starts_with('"') && str_token.ends_with('"') {
        return Token::StringValue(str_token[1..str_token.len()-1].to_string())
    };

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
        "int" => Token::IntegerType,
        "string" => Token::StringType,
        _ => {
            if let Ok(number) = str_token.parse::<i64>() {
                Token::IntegerValue(number)
            } else {
                if str_token.chars().nth(0).unwrap().is_alphabetic() &&
                    str_token.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    Token::Identificator(str_token.to_string())
                } else {
                    Token::JunkIdentificator(str_token.to_string())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_parse() {
        let valid_input = "create table,table_name (row column type int string (,) ";
        let another_valid_input = "token";
        let invalid_input = "create (row \"column, type\" int string\" yy ";
        let another_invalid_input = "123abc";

        assert!(to_tokens(valid_input).is_ok());
        assert!(to_tokens(another_valid_input).is_ok());
        assert_eq!(
            to_tokens(valid_input).unwrap(),
            vec![
                Token::Create, Token::Table, Token::Comma, Token::Identificator("table_name".into()),
                Token::LeftParenthesis, Token::Identificator("row".into()),
                Token::Identificator("column".into()), Token::Identificator("type".into()),
                Token::IntegerType, Token::StringType, Token::LeftParenthesis,
                Token::Comma, Token::RightParenthesis
            ]
        );
        assert_eq!(to_tokens(another_valid_input).unwrap(), vec![Token::Identificator("token".to_string())]);

        println!("{:?}", to_tokens(another_invalid_input));
        assert!(to_tokens(invalid_input).is_err());
        assert!(to_tokens(another_invalid_input).is_err());
    }

    #[test]
    fn token_qoutes_parse() {
        let valid_input = "create (row \"column, type\" int -421 string\" ; \"";

        assert!(to_tokens(valid_input).is_ok());
        assert_eq!(
            to_tokens(valid_input).unwrap(),
            vec![
                Token::Create, Token::LeftParenthesis, Token::Identificator("row".into()),
                Token::StringValue("column, type".to_string()), Token::IntegerType,
                Token::IntegerValue(-421), Token::StringType, Token::StringValue(" ; ".into())
            ]
        )
    }
}
