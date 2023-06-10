use std::io::{self, Write};

use crate::meta_command::MetaCommandResult;
use crate::database::Database;
use crate::meta_command_error::MetaCommandError;
use crate::connection::Connection;

mod table;
mod lexer;
mod command;
mod meta_command;
mod parser;
mod database;
mod row; // TODO: maybe put it inside database or table?
mod query_result;
mod binary_condition;
mod row_check;
mod connection;
mod execution_error;
mod meta_command_error;
mod serialize;
mod pager;
mod cmp_operator;
mod helpers;

#[cfg(test)]
mod temp_file;

const PROMPT: &str = "yarrd> ";

fn main() {
    if let Err(error) = run() {
        eprintln!("critical error: {}", error);
    }
}

fn run() -> Result<(), MetaCommandError> {
    let mut buffer = String::new();
    let stdin = io::stdin();
    let mut connection = Connection::blank();

    loop {
        buffer.clear();
        print_prompt();

        stdin.read_line(&mut buffer)?;
        let input = buffer.trim();

        match parser::parse_meta_command(input).execute(&mut connection) {
            MetaCommandResult::Exit => break,
            MetaCommandResult::Ok => {
                println!("OK");
                continue
            },
            MetaCommandResult::Err(error) => {
                println!("error executing meta command: {}", error);
                continue
            },
            MetaCommandResult::None => {
                match connection.get_mut_database() {
                    Some(database) => parse_and_execute_sql_statement(input, database),
                    None => println!("cannot exectute statement: no database connected"),
                }
            },
        };
    };

    connection.close();
    Ok(())
}

fn parse_and_execute_sql_statement(input: &str, database: &mut Database) {
    let tokens = match lexer::to_tokens(input) {
        Ok(tokens) => tokens,
        Err(message) => {
            println!("cannot parse statement: {}", message);
            return
        },
    };

    match parser::parse_statement(tokens.iter()) {
        Err(error) => println!("error parsing statement: {}", error),
        Ok(command) => {
            match database.execute(command) {
                Ok(result) => println!("{:?}", result),
                Err(message) => println!("cannot execute statement: {}", message),
            }
        },
    }
}

fn print_prompt() {
    print!("{}", PROMPT);
    io::stdout().flush().expect("error flushing the prompt");
}

// fn execute_meta_command(input: &str) -> Result<(), String> {
//     Ok(())
// }
