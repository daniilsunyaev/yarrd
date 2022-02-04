use std::io;
use std::fmt;
use std::io::Write;
use std::error::Error;

use crate::command::MetaCommand;
use crate::database::Database;
use crate::execution_error::ExecutionError;

mod table;
mod lexer;
mod command;
mod parser;
mod database;
mod row; // TODO: maybe put it inside database or table?
mod where_clause;
mod execution_error;
mod serialize;

const PROMPT: &str = "yarrd> ";

fn main() {
    if let Err(error) = run() {
        eprintln!("critical error: {}", error);
    }
}

fn run() -> Result<(), io::Error> {
    let mut buffer = String::new();
    let stdin = io::stdin();
    let mut database = Database::new();

    // let mut table = Table::db_open("./my_database.db").unwrap();

    loop {
        buffer.clear();
        print_prompt();

        stdin.read_line(&mut buffer)?;
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
