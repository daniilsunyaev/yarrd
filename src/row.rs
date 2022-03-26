use crate::table::ColumnType;
use crate::serialize::{deserialize, serialize_into, SerDeError};
use crate::execution_error::ExecutionError;
use crate::lexer::SqlValue;
use byte_layout::ByteLayout;

mod byte_layout;

pub const INTEGER_SIZE: usize = 8;
pub const STRING_SIZE: usize = 256;

/// Struct for manipulating with row's bytes, and spawning its interpretation.
/// Is it simple, so it does not check if provided bytes match column types,
/// and that source has correct byte size to read - this all table's responsibility.

#[derive(Debug)]
pub struct Row {
    bytes: Vec<u8>,
}

impl Row {
    pub fn new(column_types: &[ColumnType]) -> Row {
        let layout = Self::generate_byte_layout(column_types);
        Self::from_layout(&layout)
    }

    fn from_layout(layout: &ByteLayout) -> Row {
        let mut row_data = vec![255u8; layout.null_bitmask_byte_size()];
        row_data.resize(layout.row_size, 0u8);
        Row::from_bytes(row_data)
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Row {
        Row { bytes }
    }

    pub fn from_sql_values(values: Vec<SqlValue>, column_types: &[ColumnType]) -> Result<Row, SerDeError> {
        let layout = Self::generate_byte_layout(column_types);
        let mut row = Self::from_layout(&layout);

        for (column_index, value) in values.iter().enumerate() {
            row.set_cell(column_types, column_index, value)?;
        }
        Ok(row)
    }

    pub fn set_cell(&mut self, column_types: &[ColumnType], column_index: usize, value: &SqlValue) -> Result<(), SerDeError> {
        let layout = Self::generate_byte_layout(column_types);
        let column_offset = layout.columns_offsets[column_index];

        serialize_into(&mut self.bytes[column_offset..], column_types[column_index], value)?;
        if *value == SqlValue::Null {
            self.nullify_cell(column_index);
        } else {
            self.denullify_cell(column_index);
        }
        Ok(())
    }

    pub fn set_cell_bytes(&mut self, column_types: &[ColumnType], column_index: usize, bytes: &[u8], is_null: bool) -> Result<(), SerDeError> {
        let layout = Self::generate_byte_layout(column_types);
        self.set_cell_bytes_with_layout(column_index, bytes, is_null, &layout)
    }

    fn set_cell_bytes_with_layout(&mut self, column_index: usize, bytes: &[u8], is_null: bool, layout: &ByteLayout) -> Result<(), SerDeError> {
        let offset = layout.columns_offsets[column_index];
        let cell_size = layout.column_size(column_index);
        self.bytes[offset..offset + cell_size].clone_from_slice(&bytes[..cell_size]);

        if is_null {
            self.nullify_cell(column_index);
        } else {
            self.denullify_cell(column_index);
        }
        Ok(())
    }

    pub fn get_cell_bytes(&self, column_types: &[ColumnType], column_index: usize) -> &[u8] {
        let layout = Self::generate_byte_layout(column_types);
        let offset = layout.columns_offsets[column_index];
        let cell_size = layout.column_size(column_index);
        &self.bytes[offset..(offset + cell_size)]
    }

    pub fn get_cell_sql_value(&self, column_types: &[ColumnType], column_index: usize) -> Result<SqlValue, ExecutionError> {
        if self.cell_is_null(column_index) {
            Ok(SqlValue::Null)
        } else {
            let cell_bytes = self.get_cell_bytes(column_types, column_index);
            let column_type = column_types[column_index];
            deserialize(cell_bytes, column_type).map_err(|e| e.into())
        }
    }

    fn generate_byte_layout(column_types: &[ColumnType]) -> ByteLayout {
        let mut columns_offsets = vec![];
        for i in 0..column_types.len() {
            let offset = match i {
                0 => Self::calculate_null_bitmask_size(column_types.len()),
                _ => columns_offsets[i - 1] + Self::column_size(column_types[i - 1]),
            };

            columns_offsets.push(offset);
        }
        let row_size = columns_offsets.last().unwrap() + Self::column_size(*column_types.last().unwrap());

        ByteLayout { columns_offsets, row_size }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..]
    }

    fn column_size(column_type: ColumnType) -> usize {
        match column_type {
            ColumnType::Integer => INTEGER_SIZE,
            ColumnType::String => STRING_SIZE,
        }
    }

    pub fn cell_is_null(&self, column_index: usize) -> bool {
        self.bytes[column_index / 8] & (1 << (column_index % 8)) != 0
    }

    pub fn calculate_row_size(column_types: &[ColumnType]) -> usize {
        Self::generate_byte_layout(column_types).row_size
    }

    fn calculate_null_bitmask_size(columns_len: usize) -> usize {
        (columns_len + 7) / 8
    }

    fn nullify_cell(&mut self, column_index: usize) {
        self.bytes[column_index / 8] |= 1 << (column_index % 8);
    }

    fn denullify_cell(&mut self, column_index: usize) {
        self.bytes[column_index / 8] &= !(1 << (column_index % 8));
    }

    //pub fn get<'a, T: From<&'a SqlValue>>(&self, index: usize) -> Result<T, String> {
    //    let value = self.column_values.get(index)
    //        .ok_or(format!("row does not contain data with offset {}", index))?;

    //    Ok(value.into())
    //}

    // pub fn get(&self, index: usize) -> Result<SqlValue, String> {
    //     let value_ref = self.column_values.get(index)
    //         .ok_or(format!("row does not contain data with offset {}", index))?;

    //     Ok(value_ref.clone())
    // }
}
