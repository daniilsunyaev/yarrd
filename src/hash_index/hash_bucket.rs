use crate::hash_index::error::HashIndexError;
use crate::serialize::SerDeError;

use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write, Read};

const ROW_SIZE: usize = 1 + 8 + 8; // is deleted flag + hashed value + disk row number
pub const BUCKET_SIZE: usize = 512;
pub const BUCKET_SIZE_U64: u64 = BUCKET_SIZE as u64;
pub const ROWS_IN_BUCKET: usize = BUCKET_SIZE / ROW_SIZE - 1; // leave some space for overflow pointer
const OVERFLOW_BUCKET_ADDRESS: usize = BUCKET_SIZE - 8; // rows end at 493th byte, and we use 8 bytes
                                                        // for a pointer to overflow bucket at the end of page

#[derive(Debug)]
pub struct HashBucket {
    hash_index_file: File,
    bucket_number: u64,
    modified: bool,
    bytes: [u8; BUCKET_SIZE],
}

pub struct HashRow {
    pub presence_flag: u8,
    pub hashed_value: u64,
    pub row_id: u64,
}

impl HashBucket {
    pub fn new(file: &File, bucket_number: u64) -> Result<HashBucket, HashIndexError> {
        let mut hash_index_file = file.try_clone()?;
        let file_len = hash_index_file.metadata()?.len();
        let bucket_starts_at = BUCKET_SIZE as u64 * bucket_number;

        if bucket_starts_at == file_len {
            hash_index_file.set_len(file_len + BUCKET_SIZE as u64)?;
        } else if bucket_starts_at > file_len {
            return Err(HashIndexError::UnexpectedBucketNumber(bucket_number))
        };

        hash_index_file.seek(SeekFrom::Start(bucket_number * BUCKET_SIZE as u64))?;
        let mut bytes = [0u8; BUCKET_SIZE];
        hash_index_file.read_exact(&mut bytes)?;

        Ok(Self { hash_index_file, bucket_number, bytes, modified: false })
    }

    pub fn all_index_rows(self) -> impl Iterator<Item = Result<HashRow, HashIndexError>> {
        (0..ROWS_IN_BUCKET)
            .map(move |row_number| {
                let mut u64_blob: [u8; 8] = [0; 8];
                let row_starts_at = row_number * ROW_SIZE;

                let presence_flag = &self.bytes[row_starts_at];

                (&self.bytes[(row_starts_at + 1)..(row_starts_at + 9)])
                    .read(&mut u64_blob)
                    .map_err(SerDeError::CannotReadIntegerBytesError)?;
                let current_hashed_value = u64::from_le_bytes(u64_blob);

                (&self.bytes[(row_starts_at + 9)..(row_starts_at + 17)])
                    .read(&mut u64_blob)
                    .map_err(SerDeError::CannotReadIntegerBytesError)?;
                let potential_row_id = u64::from_le_bytes(u64_blob);

                Ok(HashRow { presence_flag: *presence_flag, hashed_value: current_hashed_value, row_id: potential_row_id })
            })
            .filter(|hash_row| hash_row.as_ref().unwrap().presence_flag == 1)
    }


    pub fn find_database_rows(self, hashed_value: u64) -> impl Iterator<Item = Result<u64, HashIndexError>> {
        self.all_index_rows()
            .filter(move |result| {
                    result.is_err() ||
                        matches!(result, Ok(index_row) if index_row.presence_flag == 1 && index_row.hashed_value == hashed_value)
            })
            .map(|result| result.map(|index_row| index_row.row_id))
    }

    pub fn insert_row(&mut self, hashed_value: u64, row_id: u64) -> Result<(), HashIndexError> {
        for row_number in 0..ROWS_IN_BUCKET {
            let row_starts_at = row_number * ROW_SIZE;
            match self.bytes[row_starts_at] {
                0 => {
                    self.bytes[row_starts_at] = 1;
                    let hashed_value_blob = hashed_value.to_le_bytes();
                    let row_id_blob = row_id.to_le_bytes();
                    (&mut self.bytes[(row_starts_at + 1)..]).write(&hashed_value_blob)?;
                    (&mut self.bytes[(row_starts_at + 9)..]).write(&row_id_blob)?;
                    self.modified = true;

                    return Ok(())
                },
                _ => continue
            }
        }

        Err(HashIndexError::BucketIsFull)
    }

    pub fn spawn_overflow_bucket(mut self) -> Result<HashBucket, HashIndexError> {
        let bucket_starts_at = self.hash_index_file.metadata()?.len();
        let overflow_bucket_number = bucket_starts_at / BUCKET_SIZE as u64;
        self.set_overflow_bucket_pointer(overflow_bucket_number)?;
        Self::new(&self.hash_index_file, overflow_bucket_number)
    }

    pub fn overflow_bucket_number(&self) -> Result<Option<u64>, HashIndexError> {
        let mut u64_blob: [u8; 8] = [0; 8];

        (&self.bytes[OVERFLOW_BUCKET_ADDRESS..])
            .read(&mut u64_blob)
            .map_err(SerDeError::CannotReadIntegerBytesError)?;

        match u64::from_le_bytes(u64_blob) {
            0 => Ok(None),
            number => Ok(Some(number)),
        }
    }

    pub fn set_overflow_bucket_pointer(&mut self, overflow_bucket_number: u64) -> Result<(), HashIndexError> {
        let number_blob = overflow_bucket_number.to_le_bytes();
        (&mut self.bytes[OVERFLOW_BUCKET_ADDRESS..]).write(&number_blob)?;
        self.modified = true;
        Ok(())
    }

    pub fn bucket_iter_with_overflow_buckets(bucket_number: u64, file: &mut File) -> impl Iterator<Item = HashBucket> + '_ {
        HashBucketChainIter { file, next_bucket_number: Some(bucket_number) }
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        if !self.modified { return Ok(()) }

        self.hash_index_file.seek(SeekFrom::Start(BUCKET_SIZE as u64 * self.bucket_number))?;
        self.hash_index_file.write_all(&self.bytes[..])?;

        Ok(())
    }
}

impl Drop for HashBucket {
    fn drop(&mut self) {
        // TODO: flush err should be handled somehow (probably via WAL or error logs)
        self.flush().unwrap();
    }
}

struct HashBucketChainIter<'a> {
    next_bucket_number: Option<u64>,
    file: &'a File,
}

impl<'a> Iterator for HashBucketChainIter<'a> {
    type Item = HashBucket;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_bucket_number.is_none() {
            None
        } else {
            match HashBucket::new(self.file, self.next_bucket_number.unwrap()) {
                Ok(bucket) => {
                    // we assume that hash index data is valid, so bucket number won't throw an error
                    self.next_bucket_number = bucket.overflow_bucket_number().unwrap();
                    Some(bucket)
                },
                _ => None,
            }
        }
    }
}
