use std::error::Error;
use std::fmt;
use std::io::{self, Seek, SeekFrom, Write, Read};
use std::fs::{OpenOptions, File};

use lru::{Lru, LruError};
use crate::row::Row;

mod lru;

const PAGE_SIZE: usize = 4096;
const PAGE_CACHE_SIZE: usize = 10;

#[derive(Debug)]
pub enum PagerError {
    IoError(io::Error),
    LruError(LruError),
}

impl fmt::Display for PagerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(io_error) => write!(f, "{}", io_error),
            Self::LruError(lru_error) => write!(f, "{}", lru_error),
        }
    }
}

impl From<io::Error> for PagerError {
    fn from(error: io::Error) -> Self {
        Self::IoError(error)
    }
}

impl From<LruError> for PagerError {
    fn from(error: LruError) -> Self {
        Self::LruError(error)
    }
}

impl Error for PagerError { }

type Page = [u8; PAGE_SIZE];

pub struct Pager {
    page_cache: Lru<u64, Page>,
    row_size: usize,
    table_file: File,
}

impl Pager {
    pub fn new(table_filepath: &str, row_size: usize) -> Result<Pager, PagerError> {
        let table_file = OpenOptions::new()
            .read(true)
            .open(table_filepath)?;
        let page_cache = Lru::new(PAGE_CACHE_SIZE)?;

        Ok(Pager { page_cache, row_size, table_file })
    }

    pub fn get(&mut self, row_id: u64) -> Result<Row, PagerError> {
        let page_id = self.page_id(row_id);
        let page =
            match self.page_cache.get(&page_id) {
                Some(page) => page,
                None => {
                    let page = Self::load_page(&mut self.table_file, page_id)?;
                    let dropped = self.page_cache.set(page_id, page);
                    Self::flush(&mut self.table_file, dropped)?;
                    self.page_cache.get(&page_id).unwrap()
                },
            };
        let page_offset = Self::page_offset(row_id, self.row_size);
        Ok(Row::from_bytes(page[page_offset..page_offset + self.row_size].to_vec()))
    }

    fn load_page(file: &mut File, page_id: u64) -> Result<Page, PagerError> {
        file.seek(SeekFrom::Start(PAGE_SIZE as u64 * page_id))?;
        let mut page = [0u8; PAGE_SIZE];
        file.read_exact(&mut page)?;
        Ok(page)
    }

    fn flush(file: &mut File, page_data: Option<(u64, Page)>) -> Result<(), PagerError> {
        if let Some((page_id, page)) = page_data {
            file.seek(SeekFrom::Start(PAGE_SIZE as u64 * page_id))?;
            file.write_all(&page[..])?;
        }
        Ok(())
    }

    // this should use primary index later on
    fn page_id(&self, row_id: u64) -> u64 {
        row_id / Self::rows_per_page(self.row_size) as u64
    }

    fn page_offset(row_id: u64, row_size: usize) -> usize {
        (row_id % Self::rows_per_page(row_size) as u64) as usize * row_size
    }

    fn rows_per_page(row_size: usize) -> usize {
        PAGE_SIZE / row_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temp_file::TempFile;

    #[test]
    fn create_pager_does_not_panic() {
        let table_file = TempFile::new("useisdares.table").unwrap();
        assert!(Pager::new(table_file.path(), 8).is_ok());
    }

    #[test]
    fn pager_gets_row() {
        let table_file = TempFile::new("users.table").unwrap();
        let contents: Vec<u8> = (0..(PAGE_SIZE * 2)).map(|n| (n % 256) as u8).collect();
        table_file.write_bytes(&contents).unwrap();
        let mut pager = Pager::new(table_file.path(), 8).unwrap();

        assert_eq!(pager.get(1).unwrap().as_bytes(), [8, 9, 10, 11, 12, 13, 14, 15]);
        assert_eq!(pager.get(512).unwrap().as_bytes(), [0, 1, 2, 3, 4, 5, 6, 7]);
    }
}
