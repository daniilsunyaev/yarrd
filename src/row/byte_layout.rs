use std::cmp::Ordering;

#[derive(Debug)]
pub struct ByteLayout {
    pub columns_offsets: Vec<usize>,
    pub row_size: usize,
}

impl ByteLayout {
    pub fn null_bitmask_byte_size(&self) -> usize {
        match self.columns_offsets.len() {
            0 => 0,
            _ => self.columns_offsets[0],
        }
    }

    pub fn column_size(&self, index: usize) -> usize {
        let max_index = self.columns_offsets.len() - 1;
        match index.cmp(&max_index) {
            Ordering::Equal => self.row_size - self.columns_offsets[max_index],
            Ordering::Less => self.columns_offsets[index + 1] - self.columns_offsets[index],
            Ordering::Greater => {
                panic!("index is out of bounds, cannot get column with index {} when there are only {} columns",
                       index, self.columns_offsets.len())
            }
        }
    }
}
