use crate::row::Row;
use crate::pager::PagerError;

pub const PAGE_SIZE: usize = 4096;

#[derive(Debug)]
pub struct Page {
    bytes: [u8; PAGE_SIZE],
    row_size: usize,
    free_row_bitmask_size: usize,
    pub modified: bool,
}

impl Page {
    pub fn new(row_size: usize, bytes: [u8; PAGE_SIZE]) -> Page {
        let row_count = Self::calculate_row_count(row_size);
        let free_row_bitmask_size = Self::free_row_bitmask_size(row_count);
        Self { row_size, free_row_bitmask_size, bytes, modified: false }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..]
    }

    pub fn get_row(&self, page_row_number: usize) -> Option<Row> {
        let row_offset = self.row_offset(page_row_number);
        match self.row_is_blank(page_row_number) {
            true => None,
            false => Some(Row::from_bytes(self.bytes[row_offset..row_offset + self.row_size].to_vec())),
        }
    }

    pub fn delete_row(&mut self, page_row_number: usize) {
        self.flag_row_presence_status(page_row_number, false);
        self.modified = true;
    }

    pub fn insert_row(&mut self, row: &Row) -> Result<(), PagerError> {
        match self.free_row_number() {
            Some(free_row_number) => {
                self.update_row(free_row_number, row);
                Ok(())
            },
            None => Err(PagerError::PageIsFull),
        }
    }

    pub fn update_row(&mut self, page_row_number: usize, row: &Row) {
        self.mut_row_bytes(page_row_number).copy_from_slice(row.as_bytes());
        self.flag_row_presence_status(page_row_number, true);
        self.modified = true;
    }

    fn flag_row_presence_status(&mut self, page_row_number: usize, new_status: bool) {
        match new_status {
            true => self.bytes[page_row_number / 8] |= 1 << (page_row_number % 8),
            false => self.bytes[page_row_number / 8] &= !(1 << (page_row_number % 8)),
        }
    }

    fn mut_row_bytes(&mut self, page_row_number: usize) -> &mut [u8] {
        let row_offset = self.row_offset(page_row_number);
        &mut self.bytes[row_offset..row_offset + self.row_size]
    }

    fn row_is_blank(&self, page_row_number: usize) -> bool {
        self.bytes[page_row_number / 8] & (1 << (page_row_number % 8)) == 0
    }

    fn row_offset(&self, page_row_number: usize) -> usize {
        self.free_row_bitmask_size + page_row_number * self.row_size
    }

    fn free_row_number(&self) -> Option<usize> {
        for (byte_i, byte) in self.free_row_bitmask().iter().enumerate() {
            let mod_8_row_number = byte.trailing_ones();
            if mod_8_row_number < 8 {
                let bit_based_row_number = byte_i * 8 + mod_8_row_number as usize;
                if bit_based_row_number >= self.row_count() {
                    return None
                } else {
                    return Some(bit_based_row_number)
                }
            }
        }
        None
    }

    fn free_row_bitmask(&self) -> &[u8] {
        &self.bytes[0..self.free_row_bitmask_size]
    }

    fn free_row_bitmask_size(row_count: usize) -> usize {
        (row_count + 7) / 8
    }

    fn row_count(&self) -> usize {
        Self::calculate_row_count(self.row_size)
    }

    pub fn calculate_row_count(row_size: usize) -> usize {
        PAGE_SIZE * 8 / (row_size * 8 + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new() {
        let bytes = [0u8; PAGE_SIZE];
        let page = Page::new(100, bytes.clone());

        assert_eq!(page.modified, false);
        assert_eq!(page.as_bytes(), &bytes);
    }

    #[test]
    fn delete_row() {
        let mut bytes = [0u8; PAGE_SIZE];
        bytes[0] = 255;
        let mut page = Page::new(100, bytes.clone());
        page.delete_row(1);

        assert_eq!(page.as_bytes()[0], 0b11111101);
        assert_eq!(page.modified, true);
    }

    #[test]
    fn row_count() {
        assert_eq!(Page::calculate_row_count(1), 3640);
        assert_eq!(Page::calculate_row_count(10), 404);
        assert_eq!(Page::calculate_row_count(50), 81);
        assert_eq!(Page::calculate_row_count(100), 40);
        assert_eq!(Page::calculate_row_count(1000), 4);
    }
}
