use std::io;
use std::fmt;
use std::io::Write;

use crate::command::MetaCommand;
use crate::database::Database;

mod table;
mod lexer;
mod command;
mod parser;
mod database;
mod row; // TODO: maybe put it inside database or table?
mod where_clause;

enum CliError {
    IoError(io::Error),
    //ParseError(String),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(io_error) => write!(f, "io error: '{:?}'", io_error),
            //Self::ParseError(error_text) => write!(f, "parse error: '{}'", error_text),
        }
    }
}

const PROMPT: &str = "yarrd> ";

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {}", error);
    }
}

fn run() -> Result<(), CliError> {
    let mut buffer = String::new();
    let stdin = io::stdin();
    let mut database = Database::new();

    // let mut table = Table::db_open("./my_database.db").unwrap();

    loop {
        buffer.clear();
        print_prompt();

        stdin.read_line(&mut buffer).map_err(CliError::IoError)?;
        let input = buffer.trim();

        match parser::parse_meta_command(input) {
            Ok(Some(MetaCommand::Exit)) => break,
            Ok(None) => { },
            Err(message) => {
                println!("error parsing meta command: {}", message);
                continue
            },
        }

        let tokens = match lexer::to_tokens(input) {
            Ok(tokens) => tokens,
            Err(message) => {
                println!("cannot parse statement: {}", message);
                continue
            }
        };

        match parser::parse_statement(tokens.iter()) {
            Err(error) => println!("error parsing statement: {}", error),
            Ok(command) => {
                match database.execute(command) {
                    Ok(result) => {
                        println!("statement executed successfully");
                        println!("{:?}", result);
                    }
                    Err(message) => println!("cannot execute statement: {}", message),
                }
            },
        }
    };
    //table.db_close().unwrap();
    Ok(())
}

fn print_prompt() {
    print!("{}", PROMPT);
    io::stdout().flush().expect("error flushing the prompt");
}

// fn execute_meta_command(input: &str) -> Result<(), String> {
//     Ok(())
// }
