use crate::database::Database;
use crate::meta_command_error::MetaCommandError;

use std::path::PathBuf;

pub enum MetaCommand {
    Void,
    Unknown(String),
    MetacommandWithWrongArgs(MetaCommandError),
    Exit,
    Createdb { db_path: PathBuf, tables_dir_path: PathBuf },
    Dropdb(PathBuf),
    Connect(PathBuf),
    CloseConnection,
}

impl MetaCommand {
    pub fn execute(self) -> MetaCommandResult {
        match self {
            Self::Void => MetaCommandResult::None,
            Self::Exit => MetaCommandResult::Exit,
            Self::MetacommandWithWrongArgs(error) => MetaCommandResult::Err(error),
            Self::Unknown(input) => MetaCommandResult::Err(MetaCommandError::UnknownCommand(input)),
            Self::Createdb { db_path, tables_dir_path } => {
                match Database::create(&db_path, &tables_dir_path) {
                    Ok(()) => MetaCommandResult::Ok,
                    Err(error) => MetaCommandResult::Err(error),
                }
            },
            Self::Dropdb(db_path) => {
                match Database::drop(&db_path) {
                    Ok(()) => MetaCommandResult::Ok,
                    Err(error) => MetaCommandResult::Err(error),
                }
            },
            Self::Connect(db_path) => {
                // TODO: implement
                MetaCommandResult::Ok
            },
            Self::CloseConnection => {
                // TODO: implement
                MetaCommandResult::Ok
            }
        }
    }
}

pub enum MetaCommandResult {
    Ok,
    None,
    Exit,
    Err(MetaCommandError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temp_file::TempFile;

    #[test]
    fn create_drop_database() {
        let (temp_dir, _temp_file) = create_temp_dir();

        let create_database = MetaCommand::Createdb {
            db_path: PathBuf::from(format!("{}/new_db", temp_dir.to_str().unwrap())),
            tables_dir_path: PathBuf::from(format!("{}/some_tables", temp_dir.to_str().unwrap())),
        };

        assert!(matches!(create_database.execute(), MetaCommandResult::Ok));

        let create_database = MetaCommand::Createdb {
            db_path: PathBuf::from(format!("{}/another_new_db", temp_dir.to_str().unwrap())),
            tables_dir_path: temp_dir.clone(),
        };

        assert!(matches!(create_database.execute(), MetaCommandResult::Ok));

        let drop_database = MetaCommand::Dropdb(PathBuf::from(format!("{}/nonexistent_db", temp_dir.clone().to_str().unwrap())));
        assert!(matches!(drop_database.execute(), MetaCommandResult::Err(_)));

        let drop_database = MetaCommand::Dropdb(PathBuf::from(format!("{}/another_new_db", temp_dir.to_str().unwrap())));
        assert!(matches!(drop_database.execute(), MetaCommandResult::Ok));
    }

    // TODO: add connect/close tests

    fn create_temp_dir() -> (PathBuf, TempFile) {
        let db_file = TempFile::new("dummy").unwrap();
        let temp_dir_path = db_file.temp_dir_path.clone();
        // we need to return tempfile because it will be dropped and removed otherwise
        (temp_dir_path, db_file)
    }
}
