use std::error::Error;
use std::fmt;
use std::io::{self, Seek, SeekFrom, Write, Read};
use std::fs::{OpenOptions, File};
use std::path::Path;

use lru::{Lru, LruError};
use page::{Page, PAGE_SIZE};
use crate::row::Row;

mod lru;
pub mod page;

const PAGE_CACHE_SIZE: usize = 10;
const MAX_ROW_SIZE: usize = PAGE_SIZE - 1; // wee need at least 1 byte for deleted row flag on the page

#[derive(Debug)]
pub enum PagerError {
    IoError(io::Error),
    LruError(LruError),
    PageIsFull,
    RowIsTooBig(usize),
}

impl fmt::Display for PagerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(io_error) => write!(f, "{}", io_error),
            Self::LruError(lru_error) => write!(f, "{}", lru_error),
            Self::PageIsFull => write!(f, "cannot append row to page: page is full"),
            Self::RowIsTooBig(row_size) =>
                write!(f, "tried to build a row which size is {} bytes, but max row size is {}", row_size, MAX_ROW_SIZE),
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

#[derive(Debug)]
pub struct Pager {
    page_cache: Lru<u64, Page>,
    row_size: usize,
    table_file: File,
}

impl Pager {
    pub fn new(table_filepath: &Path, row_size: usize) -> Result<Pager, PagerError> {
        if row_size > MAX_ROW_SIZE {
            return Err(PagerError::RowIsTooBig(row_size))
        }

        let table_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(table_filepath)?;
        let page_cache = Lru::new(PAGE_CACHE_SIZE)?;

        Ok(Pager { page_cache, row_size, table_file })
    }

    pub fn get_row(&mut self, row_id: u64) -> Result<Option<Row>, PagerError> {
        let row_number = self.page_row_number(row_id);
        let page = self.get_page_by_row_id(row_id)?;

        Ok(page.get_row(row_number))
    }

    pub fn delete_row(&mut self, row_id: u64) -> Result<(), PagerError> {
        let row_number = self.page_row_number(row_id);
        let page = self.get_page_by_row_id(row_id)?;

        page.delete_row(row_number);
        Ok(())
    }

    pub fn insert_row(&mut self, row: Row) -> Result<(), PagerError> {
        self.get_last_page()?
            .insert_row(&row)
            .or_else(|_err| {
                let page_id = self.allocate_new_page()?;
                self.get_page(page_id)?.insert_row(&row)
            })
    }

    pub fn update_row(&mut self, row_id: u64, row: &Row) -> Result<(), PagerError> {
        let row_number = self.page_row_number(row_id);
        let page = self.get_page_by_row_id(row_id)?;

        page.update_row(row_number, row);
        Ok(())
    }

    fn get_page_by_row_id(&mut self, row_id: u64) -> Result<&mut Page, PagerError> {
        let page_id = self.page_id(row_id);
        self.get_page(page_id)
    }

    fn get_page(&mut self, page_id: u64) -> Result<&mut Page, PagerError> {
        match self.page_cache.contains_key(&page_id) {
            true => Ok(self.page_cache.get_mut(&page_id).unwrap()),
            false => {
                let bytes = Self::load_page_bytes(&mut self.table_file, page_id)?;
                let page = Page::new(self.row_size, bytes);
                let dropped = self.page_cache.set(page_id, page);
                Self::flush(&mut self.table_file, dropped)?;
                let page = self.page_cache.get_mut(&page_id).unwrap();
                Ok(page)
            }
        }
    }

    pub fn max_rows(&self) -> u64 {
        match self.last_page_id().unwrap() { // TODO: check if it is successful
            None => 0,
            Some(last_page_id) => (last_page_id + 1) * Page::calculate_row_count(self.row_size) as u64,
        }
    }

    fn get_last_page(&mut self) -> Result<&mut Page, PagerError> {
        let page_id = match self.last_page_id()? {
            None => self.allocate_new_page()?,
            Some(page_id) => page_id,
        };

        self.get_page(page_id)
    }

    fn last_page_id(&self) -> io::Result<Option<u64>> {
        let table_file_size = self.table_file.metadata()?.len();
        match table_file_size {
            0 => Ok(None),
            _ => Ok(Some((table_file_size - 1) / PAGE_SIZE as u64)),
        }
    }

    fn allocate_new_page(&mut self) -> io::Result<u64> {
        let table_file_size = self.table_file.metadata()?.len();
        self.table_file.set_len(table_file_size + PAGE_SIZE as u64)?;
        Ok(self.last_page_id()?.unwrap())
    }

    fn load_page_bytes(file: &mut File, page_id: u64) -> Result<[u8; PAGE_SIZE], PagerError> {
        file.seek(SeekFrom::Start(PAGE_SIZE as u64 * page_id))?;
        let mut bytes = [0u8; PAGE_SIZE];
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn flush_all(&mut self) -> Result<(), io::Error> {
        let page_cache = std::mem::take(&mut self.page_cache);
        for page_data in page_cache {
            Self::flush(&mut self.table_file, page_data)?
        }
        Ok(())
    }

    fn flush(file: &mut File, page_data: Option<(u64, Page)>) -> Result<(), io::Error> {
        if let Some((page_id, page)) = page_data {
            if !page.modified { return Ok(()) }
            file.seek(SeekFrom::Start(PAGE_SIZE as u64 * page_id))?;
            file.write_all(page.as_bytes())?;
        }
        Ok(())
    }

    // this should use primary index later on
    fn page_id(&self, row_id: u64) -> u64 {
        row_id / Page::calculate_row_count(self.row_size) as u64
    }

    fn page_row_number(&self, row_id: u64) -> usize {
        (row_id % Page::calculate_row_count(self.row_size) as u64) as usize
    }
}

impl Drop for Pager {
    fn drop(&mut self) {
        // TODO: flush_all err should be handled somehow (probably via WAL or error logs)
        self.flush_all().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temp_file::TempFile;

    #[test]
    fn create_pager_does_not_panic() {
        let table_file = TempFile::new("users.table").unwrap();
        assert!(Pager::new(table_file.path(), 8).is_ok());
    }

    #[test]
    fn create_pager_returns_err_for_big_row() {
        let table_file = TempFile::new("users.table").unwrap();
        assert!(Pager::new(table_file.path(), 4096).is_err());
    }

    #[test]
    fn pager_gets_row() {
        let table_file = TempFile::new("users.table").unwrap();
        let mut contents: Vec<u8> = (0..(PAGE_SIZE * 2)).map(|n| (n % 256) as u8).collect();
        let row_bitmask_size = 63;
        // 4096 byte page can contain 63 * 8 = 504 rows (if row contain 8 bytes),
        // 505 rows wont fit 4096 byte page, so row bitask size is 63
        for i in 0..row_bitmask_size {
            contents[i] = 255; // make sure all rows are present on first page
        }
        contents[0] = 0b11111011; // delete row 3 on first page

        for i in PAGE_SIZE..(PAGE_SIZE + row_bitmask_size) {
            contents[i] = 255; // make sure all rows are present on second page
        }

        table_file.write_bytes(&contents).unwrap();
        let mut pager = Pager::new(table_file.path(), 8).unwrap();

        assert_eq!(pager.get_row(1).unwrap().unwrap().as_bytes(), [71, 72, 73, 74, 75, 76, 77, 78]);
        assert!(pager.get_row(2).unwrap().is_none());
        assert_eq!(pager.get_row(504).unwrap().unwrap().as_bytes(), [63, 64, 65, 66, 67, 68, 69, 70]);
    }

    #[test]
    fn page_flags_modifications() {
        let table_file = TempFile::new("users.table").unwrap();
        let contents = vec![0u8; PAGE_SIZE * 2];
        table_file.write_bytes(&contents).unwrap();
        let mut pager = Pager::new(table_file.path(), 8).unwrap();

        assert_eq!(pager.get_page_by_row_id(0).unwrap().modified, false);
        assert_eq!(pager.get_page_by_row_id(505).unwrap().modified, false); // 505th row is on the second page

        pager.delete_row(5).unwrap(); // 5th row is on the 0th page

        assert_eq!(pager.get_page_by_row_id(0).unwrap().modified, true);
        assert_eq!(pager.get_page_by_row_id(505).unwrap().modified, false);
    }
}
