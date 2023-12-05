use crate::lexer::SqlValue;
use crate::hash_index::error::HashIndexError;
use crate::hash_index::hash_bucket::{HashBucket, HashRow};
use crate::serialize::SerDeError;

use std::path::{PathBuf, Path};
use std::fs::{self, OpenOptions, File};
use std::io::{Seek, SeekFrom, Write, Read};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub mod error;
mod hash_bucket;

#[derive(Debug)]
pub struct HashIndex {
    pub name: String,
    hash_index_filepath: PathBuf,
    hash_index_file: File,
    swap_hash_index_filepath: PathBuf, // this is used to rebuild index and swap it with original
    base_buckets_count: usize,
}

impl HashIndex {
    pub fn new(tables_dir: &Path, table_name: &str, name: String) -> Result<HashIndex, HashIndexError> {
        let hash_index_filepath = Self::build_hash_index_filepath(tables_dir, table_name, name.as_str());
        let swap_filepath = Self::build_swap_hash_index_filepath(tables_dir, table_name, name.as_str());

        let hash_index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(hash_index_filepath.as_path())?;

        let base_buckets_count = HashBucket::new(&hash_index_file, 0)?.primary_buckets_count()? as usize;

        if hash_index_file.metadata()?.len() < (base_buckets_count * hash_bucket::BUCKET_SIZE) as u64 {
            hash_index_file.set_len((base_buckets_count * hash_bucket::BUCKET_SIZE) as u64)?;
        }

        Ok(Self {
            hash_index_file,
            hash_index_filepath,
            base_buckets_count,
            name,
            swap_hash_index_filepath: swap_filepath,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn find_row_ids(&self, column_value: &SqlValue) -> impl Iterator<Item = Result<u64, HashIndexError>> + '_ {
        let hashed_value = Self::hash_sql_value(column_value);

        Self::matching_buckets(&self.hash_index_file, self.base_buckets_count as u64, hashed_value)
            .flat_map(move |bucket| bucket.find_database_rows(hashed_value))
    }

    pub fn insert_row(&mut self, column_value: &SqlValue, row_id: u64, total_row_count: usize) -> Result<(), HashIndexError> {
        if total_row_count > hash_bucket::ROWS_IN_BUCKET * self.base_buckets_count / 2 {
            self.increase_buckets_count()?;
        }
        let hashed_value = Self::hash_sql_value(column_value);

        if self
            .find_row_ids(column_value)
            .any(|found_row_ids_result| {
                found_row_ids_result.is_ok() && found_row_ids_result.as_ref().unwrap() == &row_id
            }) {
                Err(HashIndexError::RowAlreadyExists(column_value.clone(), row_id))
            } else {
                Self::insert_row_to_file(&self.hash_index_file, hashed_value, row_id, self.base_buckets_count)
            }
    }

    pub fn update_row(&self, row_id: u64, old_column_value: &SqlValue, new_column_value: &SqlValue) -> Result<(), HashIndexError> {
        let hashed_old_value = Self::hash_sql_value(old_column_value);
        let hashed_new_value = Self::hash_sql_value(new_column_value);

        let row_id = self.delete_row_from_file(hashed_old_value, row_id)?;
        Self::insert_row_to_file(&self.hash_index_file, hashed_new_value, row_id, self.base_buckets_count)
    }

    pub fn delete_row(&self, row_id: u64, column_value: &SqlValue) -> Result<(), HashIndexError> {
        let hashed_value = Self::hash_sql_value(column_value);

        self.delete_row_from_file(hashed_value, row_id)?;
        Ok(())
    }

    pub fn destroy(self) -> Result<(), HashIndexError> {
        self.drop_swap_file_if_present()?;
        fs::remove_file(self.hash_index_filepath)?;
        Ok(())
    }

    pub fn adjust_filepaths(&mut self, new_table_name: &str, tables_dir: &Path) -> Result<(), HashIndexError> {
        self.drop_swap_file_if_present()?;

        let new_hash_index_filepath = Self::build_hash_index_filepath(tables_dir, new_table_name, &self.name);
        let new_swap_filepath = Self::build_swap_hash_index_filepath(tables_dir, new_table_name, &self.name);

        // TODO: this should be rollbackable via cascade file manager
        fs::rename(self.hash_index_filepath.as_path(), new_hash_index_filepath.as_path())?;

        self.hash_index_filepath = new_hash_index_filepath;
        self.swap_hash_index_filepath = new_swap_filepath;

        Ok(())
    }

    fn drop_swap_file_if_present(&self) -> Result<(), HashIndexError> {
        match fs::remove_file(self.swap_hash_index_filepath.as_path()) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => return Err(e.into()),
        }
    }

    fn insert_row_to_file(file: &File, hashed_value: u64, row_id: u64, base_buckets_count: usize) -> Result<(), HashIndexError> {
        let bucket_with_new_row =
            Self::matching_buckets(file, base_buckets_count as u64, hashed_value)
            .map(|mut bucket| {
                match bucket.insert_row(hashed_value, row_id) {
                    Err(HashIndexError::BucketIsFull)  => Ok(false), // this bucket is full, need to continue iteration
                    Ok(_) => Ok(true), // insertion successful no need to continue iteration
                    Err(other_error)  => Err(other_error), // serialization error, can't insert
                }
            })
            .find(|insertion_result| insertion_result.is_err() || insertion_result.as_ref().unwrap() == &true);

        match bucket_with_new_row {
            Some(Ok(_)) => Ok(()),
            Some(Err(error)) => Err(error),
            None => {
                Self::matching_buckets(file, base_buckets_count as u64, hashed_value)
                    .last()
                    .unwrap() // matching buckets is guaranteed to return at least one bucket
                    .spawn_overflow_bucket()?
                    .insert_row(hashed_value, row_id)
            }
        }
    }

    fn delete_row_from_file(&self, hashed_old_value: u64, row_id: u64) -> Result<u64, HashIndexError> {
        let last_deleted_row =
            Self::matching_buckets(&self.hash_index_file, self.base_buckets_count as u64, hashed_old_value)
            .map(|mut bucket| bucket.delete_row(row_id))
            .find(|deletion_result| deletion_result.is_err() || deletion_result.as_ref().unwrap().is_some());

        match last_deleted_row {
            Some(Ok(_)) => Ok(row_id),
            Some(Err(error)) => Err(error),
            None => Err(HashIndexError::RowDoesNotExists(row_id)),
        }
    }

    pub fn clear(&mut self) -> Result<(), HashIndexError> {
        self.hash_index_file.set_len(0)?;
        self.hash_index_file.rewind()?;
        Ok(())
    }

    pub fn increase_buckets_count(&mut self) -> Result<(), HashIndexError> {
        let mut swap_hash_index_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(self.swap_hash_index_filepath.as_path())?;

        swap_hash_index_file.set_len(self.base_buckets_count as u64 * 2 * hash_bucket::BUCKET_SIZE_U64)?;

        for hash_row_result in self.each_row() {
            let hash_row = hash_row_result.as_ref().unwrap();
            Self::insert_row_to_file(&swap_hash_index_file, hash_row.hashed_value, hash_row.row_id, self.base_buckets_count * 2)?
        }

        swap_hash_index_file.seek(SeekFrom::Start(hash_bucket::TOTAL_BUCKETS_ADDRESS as u64))?;
        swap_hash_index_file.write_all(&(self.base_buckets_count * 2).to_le_bytes())?;

        let total_buckets = swap_hash_index_file.metadata()?.len() / hash_bucket::BUCKET_SIZE_U64;
        self.hash_index_file.set_len(0)?;
        swap_hash_index_file.rewind()?;

        for bucket_number in 0..total_buckets {
            let mut bytes = [0u8; hash_bucket::BUCKET_SIZE];
            swap_hash_index_file.read_exact(&mut bytes)?;

            self.hash_index_file.seek(SeekFrom::Start(hash_bucket::BUCKET_SIZE_U64 * bucket_number))?;
            self.hash_index_file.write_all(&bytes[..])?;
        }

        self.base_buckets_count *= 2;

        Ok(())
    }

    fn each_row(&self) -> impl Iterator<Item = Result<HashRow, HashIndexError>> + '_ {
        self.each_bucket()
            .flat_map(|bucket| bucket.unwrap().all_index_rows())
    }

    fn each_bucket(&self) -> impl Iterator<Item = Result<HashBucket, HashIndexError>> + '_ {
        let total_buckets = self.hash_index_file.metadata().unwrap().len() / hash_bucket::BUCKET_SIZE_U64;
        (0..total_buckets)
            .map(|bucket_number| HashBucket::new(&self.hash_index_file, bucket_number))
    }

    fn matching_buckets(hash_index_file: &File, base_buckets_count: u64, hashed_value: u64) -> impl Iterator<Item = HashBucket> + '_ {
        let primary_bucket_number = hashed_value % base_buckets_count;
        HashBucket::bucket_iter_with_overflow_buckets(primary_bucket_number, hash_index_file)
    }

    fn hash_sql_value(value: &SqlValue) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    fn build_hash_index_filepath(tables_dir: &Path, table_name: &str, index_name: &str) -> PathBuf {
        let mut filepath = tables_dir.to_path_buf();
        filepath.push(format!("{}-{}.hash", table_name, index_name));
        filepath
    }

    fn build_swap_hash_index_filepath(tables_dir: &Path, table_name: &str, index_name: &str) -> PathBuf {
        let mut filepath = tables_dir.to_path_buf();
        filepath.push(format!("{}-{}-swap.hash", table_name, index_name));
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

    fn create_index_file(table_name: &str, index_name: &str) -> (TempFile, PathBuf) {
        let index_file = TempFile::new(format!("{}-{}.hash", table_name, index_name).as_str()).unwrap();
        let table_file_name = "users.table";
        let mut tables_dir_path = index_file.path().to_path_buf();
        tables_dir_path.pop();

        (index_file, tables_dir_path)
    }

    #[test]
    fn create_index_does_not_panic() {
        let (_index_file, tables_dir_path) = create_index_file("users", "u8");
        HashIndex::new(&tables_dir_path, "users", "name".to_string()).expect("cannot create index from file");
    }

    #[test]
    fn find_row_ids() {
        let (index_file, tables_dir_path) = create_index_file("users", "u_index_2");

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
        contents[496] = 1; // total buckets count

        index_file.write_bytes(&contents)
            .expect("seed contents should be writable to index file");

        let index = HashIndex::new(tables_dir_path.as_path(), "users", "u_index_2".to_string())
            .expect("hash index should be creatable from seed file");

        assert_eq!(index.find_row_ids(&SqlValue::Integer(1)).next().unwrap().unwrap(), 3u64);
        assert_eq!(index.find_row_ids(&SqlValue::String("john".to_string())).next().unwrap().unwrap(), 1u64);
        assert_eq!(index.find_row_ids(&SqlValue::Integer(3)).next().is_none(), true);
    }

    #[test]
    fn insert_row_causing_overflow() {
        let (index_file, tables_dir_path) = create_index_file("users", "i_name");

        let hash_5 = calculate_hash(&5i64).to_le_bytes(); // in case of 4 buckets, hash of 5 falls to first bucket
        let mut contents: Vec<u8> = vec![];

        for row_id in 0..28u64 { // we leave 1 free row in first bucket
            contents.push(1); // presence flag
            contents.extend_from_slice(&hash_5); // hashed value (5)
            contents.extend_from_slice(&row_id.to_le_bytes()); // row pointer
        }

        contents.resize(512 * 4, 0); // we reserve 4 bucket file to avoid reindexing in this test
        contents[496] = 4; // total buckets count

        index_file.write_bytes(&contents)
            .expect("seed contents should be writable to index file");

        let mut index = HashIndex::new(tables_dir_path.as_path(), "users", "i_name".to_string())
            .expect("hash index should be creatable from seed file");

        assert_eq!(index.insert_row(&SqlValue::Integer(5), 999, 28).is_ok(), true);
        assert_eq!(index.find_row_ids(&SqlValue::Integer(5)).last().unwrap().unwrap(), 999u64);

        // inserting to overflow bucket
        assert_eq!(index.insert_row(&SqlValue::Integer(5), 1000, 29).is_ok(), true);
        assert_eq!(index.find_row_ids(&SqlValue::Integer(5)).last().unwrap().unwrap(), 1000u64);

        assert_eq!(index_file.file_path.metadata().unwrap().len(), 512 * 5);

        let overflow_blob = index_file.read_u64(504).expect("cannot read overflow bucket number blob");
        let overflow_pointer = u64::from_le_bytes(overflow_blob);
        assert_eq!(overflow_pointer, 4); // 5th bucket is overflow bucket of a first bucket
    }

    #[test]
    fn insert_row_causing_index_recreation() {
        let (index_file, tables_dir_path) = create_index_file("users", "u2");

        let hash_1 = calculate_hash(&1i64).to_le_bytes();
        let mut contents: Vec<u8> = vec![];

        for row_id in 0..20u64 { // more than half of bucket is filled
            contents.push(1);
            contents.extend_from_slice(&hash_1);
            contents.extend_from_slice(&row_id.to_le_bytes());
        }

        contents.resize(512, 0);
        contents[496] = 1; // total buckets count

        index_file.write_bytes(&contents)
            .expect("seed contents should be writable to index file");

        let mut index = HashIndex::new(tables_dir_path.as_path(), "users", "u2".to_string())
            .expect("hash index should be creatable from seed file");

        assert_eq!(index.insert_row(&SqlValue::Integer(1), 999, 28).is_ok(), true);
        assert_eq!(index.find_row_ids(&SqlValue::Integer(1)).last().unwrap().unwrap(), 999u64);

        assert_eq!(index_file.file_path.metadata().unwrap().len(), 512 * 2);

        let overflow_blob = index_file.read_u64(504).expect("cannot read overflow bucket number blob");
        let overflow_pointer = u64::from_le_bytes(overflow_blob);
        assert_eq!(overflow_pointer, 0); // no overflow
    }

    #[test]
    fn update_and_delete_row() {
        let (index_file, tables_dir_path) = create_index_file("users", "ui1");

        let hash_1 = calculate_hash(&1i64).to_le_bytes();
        let mut contents: Vec<u8> = vec![];

        for row_id in 0..2u64 {
            contents.push(1);
            contents.extend_from_slice(&hash_1);
            contents.extend_from_slice(&row_id.to_le_bytes());
        }

        contents.resize(512, 0);
        contents[496] = 1; // total buckets count

        index_file.write_bytes(&contents)
            .expect("seed contents should be writable to index file");

        let index = HashIndex::new(tables_dir_path.as_path(), "users", "ui1".to_string())
            .expect("hash index should be creatable from seed file");

        assert_eq!(index.update_row(1, &SqlValue::Integer(1), &SqlValue::Integer(3)).is_ok(), true);
        assert_eq!(index.find_row_ids(&SqlValue::Integer(3)).last().unwrap().unwrap(), 1u64);

        let mut ids_with_3 = index.find_row_ids(&SqlValue::Integer(3));
        assert_eq!(ids_with_3.next().is_some(), true);
        assert_eq!(ids_with_3.next().is_none(), true);

        let mut ids_with_1 = index.find_row_ids(&SqlValue::Integer(1));
        assert_eq!(ids_with_1.next().is_some(), true);
        assert_eq!(ids_with_1.next().is_none(), true);

        assert_eq!(index.update_row(8, &SqlValue::Integer(1), &SqlValue::Integer(3)).is_err(), true);

        assert_eq!(index.delete_row(0, &SqlValue::Integer(1)).is_ok(), true);

        let mut ids_with_1 = index.find_row_ids(&SqlValue::Integer(1));
        assert_eq!(ids_with_1.next().is_none(), true);
    }
}
