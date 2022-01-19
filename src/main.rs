use std::io;
use std::fmt;
use std::io::Write;

use crate::command::MetaCommand;

mod table;
mod lexer;
mod command;
mod parser;
mod database;

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
            Ok(command) => {
                println!("successfully parsed command {:?}", command);
            },
            Err(error) => println!("error parsing statement: {}", error),
        }

        //} else {
        //    match Statement::parse(input, &mut table) {
        //        Ok(statement) => {
        //            match statement.execute() {
        //                Ok(_) => println!("statement executed successfully"),
        //                Err(message) => println!("cannot execute statement: {}", message),
        //            }
        //        },
        //        Err(error) => println!("error parseing statement: {}", error),
        //    }
        //}
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
