use crate::database::Database;
use crate::MetaCommandError;

use std::path::Path;

pub struct Connection {
    database: Option<Database>
}

impl Connection {
    pub fn blank() -> Self {
        Self { database: None }
    }

    pub fn from(&mut self, database_filepath: &Path) -> Result<(), MetaCommandError> {
        let database = Database::from(database_filepath)?;
        self.close();
        self.database = Some(database);
        Ok(())
    }

    pub fn close(&mut self) {
        if self.database.is_some() {
            let db = std::mem::take(&mut self.database);
            db.unwrap().close();
        }
    }

    pub fn is_active(&self) -> bool {
        self.database.is_some()
    }

    pub fn get_mut_database(&mut self) -> Option<&mut Database> {
        self.database.as_mut()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.close();
    }
}
