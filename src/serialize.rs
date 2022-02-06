use std::io::{Read, Write};

use crate::table::ColumnType;
use crate::lexer::SqlValue;

pub fn serialize_into<W: Write>(mut destination: W, column_type: ColumnType, value: &SqlValue) -> Result<(), String> {
    match column_type {
        ColumnType::String => {
            let blob = serialize_string(value)?;
            destination.write(&blob).map_err(|e| e.to_string())?;
        }
        ColumnType::Integer => {
            let blob = serialize_int(value)?;
            destination.write(&blob).map_err(|e| e.to_string())?;
        }
    }

    Ok(())
}

pub fn deserialize<R: Read>(mut source: R, column_type: ColumnType) -> Result<SqlValue, String> {
    match column_type {
        ColumnType::String => {
            let mut len_blob = [0u8];
            source.read(&mut len_blob).map_err(|e| e.to_string())?;
            let len = len_blob[0] as usize;
            let mut str_blob = vec![0u8; len];
            source.read(&mut str_blob).map_err(|e| e.to_string())?;
            let string = std::str::from_utf8(&str_blob).map_err(|e| e.to_string())?;
            Ok(SqlValue::String(string.to_owned()))
        },
        ColumnType::Integer => {
            let mut blob = [0u8; 8];
            source.read(&mut blob).map_err(|e| e.to_string())?;

            let int = i64::from_le_bytes(blob);
            Ok(SqlValue::Integer(int))
        }
    }
}

fn serialize_int(value: &SqlValue) -> Result<[u8; 8], String> {
    match value {
        SqlValue::Integer(int) => Ok(int.to_le_bytes()),
        SqlValue::String(string) | SqlValue::Identificator(string) =>
            Err(format!("string '{}' cannot be used as integer value", string)),
        SqlValue::Null => Ok([0; 8]),
    }
}

fn serialize_string(value: &SqlValue) -> Result<[u8; 256], String> {
    match value {
        SqlValue::Integer(int) => {
            let string = int.to_string();
            serialize_native_string(&string)
        },
        SqlValue::String(string) | SqlValue::Identificator(string) => serialize_native_string(string),
        SqlValue::Null => Ok([0; 256]),
    }
}

fn serialize_native_string(string: &str) -> Result<[u8; 256], String> {
    let len = string.len();
    let mut result = [0u8; 256];
    result[0] = len as u8;
    for (i, byte) in string.bytes().enumerate() {
        result[i+1] = byte;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_integer() {
        let mut dest = [0u8; 4];
        let int = 2;
        let result = serialize_into(&mut dest[..], ColumnType::Integer, &SqlValue::Integer(int));
        assert!(result.is_ok());
        assert_eq!(dest, [2u8, 0, 0, 0]);

        let mut dest = [0u8; 4];
        let int = "2".to_string();
        let result = serialize_into(&mut dest[..], ColumnType::Integer, &SqlValue::String(int));
        assert!(result.is_err());
        assert_eq!(dest, [0u8; 4]);
    }

    #[test]
    fn serialize_string() {
        let mut dest = vec![0u8; 256];
        let text = "abc d".to_string();
        let result = serialize_into(&mut dest[..], ColumnType::String, &SqlValue::String(text));
        assert!(result.is_ok());
        assert_eq!(dest[0..8], [5u8, 97, 98, 99, 32, 100, 0, 0]);

        let mut dest = vec![0u8; 256];
        let text = "abc".to_string();
        let result = serialize_into(&mut dest[..], ColumnType::String, &SqlValue::Identificator(text));
        assert!(result.is_ok());
        assert_eq!(dest[0..6], [3u8, 97, 98, 99, 0, 0]);
    }

    #[test]
    fn deserialize_integer() {
        let source = [3u8, 0, 0];
        let result = deserialize(&source[..], ColumnType::Integer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::Integer(3));
    }

    #[test]
    fn deserialize_string() {
        let source = [4u8, 97, 98, 99, 97, 0];
        let result = deserialize(&source[..], ColumnType::String);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::String("abca".to_owned()));
    }
}
