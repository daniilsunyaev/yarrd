use std::io;
use std::fmt;
use std::io::Write;

mod table;
mod lexer;
mod command;
mod parser;

// enum MetaCommand {
//     Exit,
// }

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
    match run() {
        Err(error) => eprintln!("error: {}", error),
        Ok(_) => { },
    }
}

fn run() -> Result<(), CliError> {
    let mut buffer = String::new();
    let stdin = io::stdin();

    // let mut table = Table::db_open("./my_database.db").unwrap();

    loop {
        buffer.clear();
        print_prompt();

        stdin.read_line(&mut buffer).map_err(|io_error| CliError::IoError(io_error))?;
        let input = buffer.trim();

        let tokens = lexer::to_tokens(input);

        match parser::parse_statement(tokens.iter()) {
            Ok(command) => {
                println!("successfully parsed command {:?}", command);
            },
            Err(error) => println!("error parsing statement: {}", error),
        }

        //if input.starts_with('.') {
        //    match parse_meta_command(input) {
        //        Ok(MetaCommand::Exit) => break,
        //        // Ok(command) => execute_meta_command(command)?,
        //        Err(error) => println!("error parsing meta command: {}", error),
        //    }

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
    //Ok(())
}

fn print_prompt() {
    print!("{}", PROMPT);
    io::stdout().flush().expect("error flushing the prompt");
}

//fn parse_meta_command(input: &str) -> Result<MetaCommand, String> {
//    if input.starts_with(".exit") || input.starts_with(".quit") {
//        Ok(MetaCommand::Exit)
//    } else {
//        Err(format!("unrecognized command '{}'", input))
//    }
//}


// fn execute_meta_command(input: &str) -> Result<(), String> {
//     Ok(())
// }
