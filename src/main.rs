use std::io::{self, Write};
use std::path::Path;

use crate::meta_command::MetaCommandResult;
use crate::database::Database;
use crate::meta_command_error::MetaCommandError;

mod table;
mod lexer;
mod command;
mod meta_command;
mod parser;
mod database;
mod row; // TODO: maybe put it inside database or table?
mod query_result;
mod where_clause;
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
    let mut connected_database: Option<Database> = None;

    loop {
        buffer.clear();
        print_prompt();

        stdin.read_line(&mut buffer)?;
        let input = buffer.trim();

        match parser::parse_meta_command(input).execute() {
            MetaCommandResult::Exit => break,
            MetaCommandResult::Ok => {
                println!("OK");
                continue
            },
            MetaCommandResult::Connection(database) => {
                println!("connected to {}", database.name());
                if let Some(database) = connected_database { // TODO: extract to connection
                    database.close();
                    connected_database = None;
                }
                connected_database = Some(database);
                continue
            },
            MetaCommandResult::CloseConnectionDirective => {
                if let Some(database) = connected_database {
                    database.close();
                    connected_database = None;
                }
                continue
            }
            MetaCommandResult::Err(error) => {
                println!("error executing meta command: {}", error);
                continue
            },
            MetaCommandResult::None => {
                match connected_database.as_mut() {
                    Some(database) => parse_and_execute_sql_statement(input, database),
                    None => println!("cannot exectute statement: no database connected"),
                }
            },
        };
    };

    match connected_database {
        Some(database) => database.close(),
        None => {},
    }
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
