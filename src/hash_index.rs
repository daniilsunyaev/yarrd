use crate::lexer::SqlValue;
use crate::hash_index::error::HashIndexError;
use crate::serialize::SerDeError;

use std::path::{PathBuf, Path};
use std::fs::{OpenOptions, File};
use std::io::{Seek, SeekFrom, Write, Read};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const ROW_SIZE: usize = 1 + 8 + 8; // is deleted flag + hashed value + disk row number
const BUCKET_SIZE: usize = 512;
const ROWS_IN_BUCKET: usize = BUCKET_SIZE / ROW_SIZE;

pub mod error;

#[derive(Debug)]
pub struct HashBucket {
    hash_index_file: File,
    bytes: [u8; BUCKET_SIZE],
}

impl HashBucket {
    pub fn new(filepath: &Path, bucket_number: u64) -> Result<HashBucket, HashIndexError> {
        let mut hash_index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(filepath)?;

        hash_index_file.seek(SeekFrom::Start(BUCKET_SIZE as u64 * bucket_number))?;
        let mut bytes = [0u8; BUCKET_SIZE];
        hash_index_file.read_exact(&mut bytes)?;

        Ok(Self { hash_index_file, bytes })
    }

    pub fn find_rows(self, hashed_value: u64) -> impl Iterator<Item = Result<u64, HashIndexError>> {
        // TODO: what if there are 100 values maped to the same hash value, how it will fit in a
        // single bucket?

        (0..ROWS_IN_BUCKET)
            .map(move |row_number| {
                let mut u64_blob: [u8; 8] = [0; 8];
                let row_starts_at = row_number * ROW_SIZE;

                let presence_flag = &self.bytes[row_starts_at];

                (&self.bytes[(row_starts_at + 1)..(row_starts_at + 9)])
                    .read(&mut u64_blob)
                    .map_err(SerDeError::CannotReadStringLenError)?;
                let current_hashed_value = u64::from_le_bytes(u64_blob);

                (&self.bytes[(row_starts_at + 9)..(row_starts_at + 17)])
                    .read(&mut u64_blob)
                    .map_err(SerDeError::CannotReadStringLenError)?;
                let potential_row_id = u64::from_le_bytes(u64_blob);

                Ok((*presence_flag, current_hashed_value, potential_row_id))
            })
            .filter(move |result| {
                    result.is_err() ||
                        matches!(result, Ok(tuple) if tuple.0 == 1 && tuple.1 == hashed_value)
            })
            .map(|result| result.map(|r| r.2))
    }

    // pub fn insert_row(&mut self, hashed_value: u64, row_id: u64) -> Result<(),()> {
    //     for row_number in 0..ROWS_IN_BUCKET {
    //         let row_starts_at = row_number * ROW_SIZE;
    //         match self.bytes[row_starts_at] {
    //             1 => continue,
    //             0 => {
    //                 self.bytes[row_starts_at] = 1;
    //                 let hashed_value_blob = hashed_value.to_le_bytes();
    //                 let row_id_blob = row_id.to_le_bytes();
    //                 (&mut self.bytes[(row_starts_at + 1)..]).write(&hashed_value_blob);
    //                 (&mut self.bytes[(row_starts_at + 9)..]).write(&row_id_blob);

    //                 return Ok(())
    //             },
    //             _ => continue,
    //         }
    //     }

    //     Err(())
    // }
}

#[derive(Debug, Clone)]
pub struct HashIndex {
    filepath: PathBuf,
    buckets_count: u64,
}

impl HashIndex {
    pub fn new(table_filepath: &Path, table_name: &str, column_index: usize) -> HashIndex {
        Self {
            filepath: Self::build_hash_index_filepath(table_filepath, table_name, column_index),
            buckets_count: 1,
        }
    }

    pub fn find_row_ids(&self, column_value: SqlValue) -> Result<impl Iterator<Item = Result<u64, HashIndexError>>, HashIndexError> {
        let hashed_value = Self::hash_sql_value(column_value);

        Ok(self.get_bucket(hashed_value)?.find_rows(hashed_value))
    }

    fn get_bucket(&self, hashed_value: u64) -> Result<HashBucket, HashIndexError> {
        let bucket_number = hashed_value % self.buckets_count;

        HashBucket::new(self.filepath.as_path(), bucket_number)
    }

    fn hash_sql_value(value: SqlValue) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    fn build_hash_index_filepath(table_filepath: &Path, table_name: &str, column_index: usize) -> PathBuf {
        let mut filepath = table_filepath.to_path_buf();
        filepath.pop();
        filepath.push(format!("{}-{}.hash", table_name, column_index));
        filepath
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temp_file::TempFile;

    fn calculate_hash<T: Hash>(t: &T) -> u64 {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }

    #[test]
    fn create_index_does_not_panic() {
        let table_file = TempFile::new("users.table").unwrap();
        HashIndex::new(table_file.path(), "users", 8);
    }

    #[test]
    fn find_row_ids() {
        let mut index_file = TempFile::new("users-2.hash").unwrap();
        let table_file_name = "users.table";
        let mut table_file_path = index_file.path().to_path_buf();
        table_file_path.pop();
        table_file_path.push(table_file_name);

        let hash_1 = calculate_hash(&1i64).to_le_bytes();
        let hash_john = calculate_hash(&"john").to_le_bytes();
        let hash_3 = calculate_hash(&3i64).to_le_bytes();
        let mut contents: Vec<u8> = vec![];

        contents.push(1);
        contents.extend_from_slice(&hash_1); // hashed value (1)
        contents.extend_from_slice(&3u64.to_le_bytes()); // row_id (3)

        contents.push(1);
        contents.extend_from_slice(&hash_john); // hashed value ("john")
        contents.extend_from_slice(&1u64.to_le_bytes()); // row_id (1)

        contents.push(0); // this row is removed
        contents.extend_from_slice(&hash_3); // hashed value (3)
        contents.extend_from_slice(&1u64.to_le_bytes()); // row_id (1)

        contents.resize(512, 0);

        index_file.write_bytes(&contents).unwrap();

        let index = HashIndex::new(table_file_path.as_path(), "users", 2);

        assert_eq!(index.find_row_ids(SqlValue::Integer(1)).unwrap().next().unwrap().unwrap(), 3u64);
        assert_eq!(index.find_row_ids(SqlValue::String("john".to_string())).unwrap().next().unwrap().unwrap(), 1u64);
        assert_eq!(index.find_row_ids(SqlValue::Integer(3)).unwrap().next().is_none(), true);
    }
}
