use crate::lexer::SqlValue;
use crate::hash_index::error::HashIndexError;
use crate::hash_index::hash_bucket::{HashBucket, HashRow};
use crate::serialize::SerDeError;

use std::path::{PathBuf, Path};
use std::fs::{OpenOptions, File};
use std::io::{Seek, SeekFrom, Write, Read};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub mod error;
mod hash_bucket;

#[derive(Debug)]
pub struct HashIndex {
    hash_index_file: File,
    swap_hash_index_filepath: PathBuf, // this is used to rebuild index and swap it with original
    base_buckets_count: usize,
}

impl HashIndex {
    pub fn new(table_filepath: &Path, table_name: &str, base_buckets_count: usize, column_index: usize) -> Result<HashIndex, HashIndexError> {
        let filepath = Self::build_hash_index_filepath(table_filepath, table_name, column_index);
        let swap_filepath = Self::build_swap_hash_index_filepath(table_filepath, table_name, column_index);

        let hash_index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filepath)?;

        Ok(Self {
            hash_index_file,
            base_buckets_count,
            swap_hash_index_filepath: swap_filepath,
        })
    }

    pub fn find_row_ids(&mut self, column_value: &SqlValue) -> impl Iterator<Item = Result<u64, HashIndexError>> + '_ {
        let hashed_value = Self::hash_sql_value(column_value);

        Self::matching_buckets(&mut self.hash_index_file, self.base_buckets_count as u64, hashed_value)
            .map(move |bucket| bucket.find_database_rows(hashed_value))
            .flatten()
    }

    pub fn insert_row(&mut self, column_value: &SqlValue, row_id: u64, total_row_count: usize) -> Result<(), HashIndexError> {
        if total_row_count > hash_bucket::ROWS_IN_BUCKET * self.base_buckets_count / 2 {
            self.increase_buckets_count()?;
        }
        let hashed_value = Self::hash_sql_value(column_value);

        Self::insert_row_to_file(&mut self.hash_index_file, hashed_value, row_id, self.base_buckets_count)
    }

    fn insert_row_to_file(file: &mut File, hashed_value: u64, row_id: u64, base_buckets_count: usize) -> Result<(), HashIndexError> {
        let bucket_with_new_row =
            Self::matching_buckets(file, base_buckets_count as u64, hashed_value)
            .map(|mut bucket: HashBucket| {
                match bucket.insert_row(hashed_value, row_id) {
                    Ok(_) => true, // insert successful, finish iteration
                    Err(_) => false, // keep searching for a free bucket
                }
            })
            .skip_while(|&insertion_result| insertion_result == false)
            .next();


        match bucket_with_new_row {
            Some(_) => Ok(()),
            None => {
                Self::matching_buckets(file, base_buckets_count as u64, hashed_value)
                    .last()
                    .unwrap() // matching buckets is guaranteed to return at least one bucket
                    .spawn_overflow_bucket()?
                    .insert_row(hashed_value, row_id)
            }
        }
    }

    pub fn increase_buckets_count(&mut self) -> Result<(), HashIndexError> {
        let mut swap_hash_index_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(self.swap_hash_index_filepath.as_path())?;

        swap_hash_index_file.set_len(self.base_buckets_count as u64* 2 * hash_bucket::BUCKET_SIZE_U64)?;

        for hash_row_result in self.each_row() {
            let hash_row = hash_row_result.as_ref().unwrap();
            Self::insert_row_to_file(&mut swap_hash_index_file, hash_row.hashed_value, hash_row.row_id, self.base_buckets_count * 2)?
        }

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
            .map(|bucket| bucket.unwrap().all_index_rows())
            .flatten()
    }

    fn each_bucket(&self) -> impl Iterator<Item = Result<HashBucket, HashIndexError>> + '_ {
        let total_buckets = self.hash_index_file.metadata().unwrap().len() / hash_bucket::BUCKET_SIZE_U64;
        (0..total_buckets)
            .map(|bucket_number| HashBucket::new(&self.hash_index_file, bucket_number))
    }

    fn matching_buckets(hash_index_file: &mut File, base_buckets_count: u64, hashed_value: u64) -> impl Iterator<Item = HashBucket> + '_ {
        let primary_bucket_number = hashed_value % base_buckets_count;
        HashBucket::bucket_iter_with_overflow_buckets(primary_bucket_number, hash_index_file)
    }

    fn hash_sql_value(value: &SqlValue) -> u64 {
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

    fn build_swap_hash_index_filepath(table_filepath: &Path, table_name: &str, column_index: usize) -> PathBuf {
        let mut filepath = table_filepath.to_path_buf();
        filepath.pop();
        filepath.push(format!("{}-{}-swap.hash", table_name, column_index));
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

    fn create_index_file() -> (TempFile, PathBuf) {
        let index_file = TempFile::new("users-2.hash").unwrap();
        let table_file_name = "users.table";
        let mut table_file_path = index_file.path().to_path_buf();
        table_file_path.pop();
        table_file_path.push(table_file_name);

        (index_file, table_file_path)
    }

    #[test]
    fn create_index_does_not_panic() {
        let table_file = TempFile::new("users.table").unwrap();
        HashIndex::new(table_file.path(), "users", 1, 8).expect("cannot create index from file");
    }

    #[test]
    fn find_row_ids() {
        let (index_file, table_file_path) = create_index_file();

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

        index_file.write_bytes(&contents)
            .expect("seed contents should be writable to index file");

        let mut index = HashIndex::new(table_file_path.as_path(), "users", 1, 2)
            .expect("hash index should be creatable from seed file");

        assert_eq!(index.find_row_ids(&SqlValue::Integer(1)).next().unwrap().unwrap(), 3u64);
        assert_eq!(index.find_row_ids(&SqlValue::String("john".to_string())).next().unwrap().unwrap(), 1u64);
        assert_eq!(index.find_row_ids(&SqlValue::Integer(3)).next().is_none(), true);
    }

    #[test]
    fn insert_row_causing_overflow() {
        let (index_file, table_file_path) = create_index_file();

        let hash_5 = calculate_hash(&5i64).to_le_bytes(); // in case of 4 buckets, hash of 5 falls to first bucket
        let mut contents: Vec<u8> = vec![];

        for row_id in 0..28u64 { // we leave 1 free row in first bucket
            contents.push(1); // presence flag
            contents.extend_from_slice(&hash_5); // hashed value (5)
            contents.extend_from_slice(&row_id.to_le_bytes()); // row pointer
        }

        contents.resize(512 * 4, 0); // we reserve 4 bucket file to avoid reindexing in this test

        index_file.write_bytes(&contents)
            .expect("seed contents should be writable to index file");

        let mut index = HashIndex::new(table_file_path.as_path(), "users", 4, 2)
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
        let (index_file, table_file_path) = create_index_file();

        let hash_1 = calculate_hash(&1i64).to_le_bytes();
        let mut contents: Vec<u8> = vec![];

        for row_id in 0..20u64 { // more than half of bucket is filled
            contents.push(1);
            contents.extend_from_slice(&hash_1);
            contents.extend_from_slice(&row_id.to_le_bytes());
        }

        contents.resize(512, 0);

        index_file.write_bytes(&contents)
            .expect("seed contents should be writable to index file");

        let mut index = HashIndex::new(table_file_path.as_path(), "users", 1, 2)
            .expect("hash index should be creatable from seed file");

        assert_eq!(index.insert_row(&SqlValue::Integer(1), 999, 28).is_ok(), true);
        assert_eq!(index.find_row_ids(&SqlValue::Integer(1)).last().unwrap().unwrap(), 999u64);

        assert_eq!(index_file.file_path.metadata().unwrap().len(), 512 * 2);

        let overflow_blob = index_file.read_u64(504).expect("cannot read overflow bucket number blob");
        let overflow_pointer = u64::from_le_bytes(overflow_blob);
        assert_eq!(overflow_pointer, 0); // no overflow
    }
}
