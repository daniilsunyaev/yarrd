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
        if let Some(_) = &self.database {
            let db = std::mem::take(&mut self.database);
            db.unwrap().close();
        }
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.database.is_none()
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
