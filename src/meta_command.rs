use crate::database::Database;
use crate::meta_command_error::MetaCommandError;
use crate::connection::Connection;

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
    pub fn execute(self, connection: &mut Connection) -> MetaCommandResult {
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
                if connection.is_active() {
                    return MetaCommandResult::Err(MetaCommandError::ConnectionPresent);
                }

                match Database::drop(&db_path) {
                    Ok(()) => MetaCommandResult::Ok,
                    Err(error) => MetaCommandResult::Err(error),
                }
            },
            Self::Connect(db_path) => {
                match connection.from(&db_path) {
                    Ok(_) => MetaCommandResult::Ok,
                    Err(error) => MetaCommandResult::Err(error),
                }
            },
            Self::CloseConnection => {
                connection.close();
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
        let mut connection = Connection::blank();

        let create_database = MetaCommand::Createdb {
            db_path: PathBuf::from(format!("{}/new_db", temp_dir.to_str().unwrap())),
            tables_dir_path: PathBuf::from(format!("{}/some_tables", temp_dir.to_str().unwrap())),
        };

        assert!(matches!(create_database.execute(&mut connection), MetaCommandResult::Ok));
        assert_eq!(connection.is_active(), false);

        let create_database = MetaCommand::Createdb {
            db_path: PathBuf::from(format!("{}/another_new_db", temp_dir.to_str().unwrap())),
            tables_dir_path: temp_dir.clone(),
        };

        assert!(matches!(create_database.execute(&mut connection), MetaCommandResult::Ok));
        assert_eq!(connection.is_active(), false);

        let drop_database = MetaCommand::Dropdb(PathBuf::from(format!("{}/nonexistent_db", temp_dir.clone().to_str().unwrap())));
        assert!(matches!(drop_database.execute(&mut connection), MetaCommandResult::Err(_)));
        assert_eq!(connection.is_active(), false);

        let drop_database = MetaCommand::Dropdb(PathBuf::from(format!("{}/another_new_db", temp_dir.to_str().unwrap())));
        assert!(matches!(drop_database.execute(&mut connection), MetaCommandResult::Ok));
        assert_eq!(connection.is_active(), false);
    }

    #[test]
    fn create_connect_close_database() {
        let (temp_dir, _temp_file) = create_temp_dir();

        let db_path = PathBuf::from(format!("{}/new_db", temp_dir.to_str().unwrap()));
        let mut connection = Connection::blank();

        MetaCommand::Createdb {
            db_path: db_path.clone(),
            tables_dir_path: PathBuf::from(format!("{}/some_tables", temp_dir.to_str().unwrap())),
        }.execute(&mut connection);

        let connect = MetaCommand::Connect(db_path).execute(&mut connection);

        assert!(matches!(connect, MetaCommandResult::Ok));
        assert_eq!(connection.is_active(), true);

        let disconnect = MetaCommand::CloseConnection.execute(&mut connection);

        assert!(matches!(disconnect, MetaCommandResult::Ok));
        assert_eq!(connection.is_active(), false);
    }

    fn create_temp_dir() -> (PathBuf, TempFile) {
        let db_file = TempFile::new("dummy").unwrap();
        let temp_dir_path = db_file.temp_dir_path.clone();
        // we need to return tempfile because it will be dropped and removed otherwise
        (temp_dir_path, db_file)
    }
}
