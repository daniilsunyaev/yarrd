use std::io::{self, Read, Write};
use std::error::Error;
use std::fmt;

use crate::table::ColumnType;
use crate::lexer::SqlValue;

#[derive(Debug)]
pub enum SerDeError {
    WriteError(io::Error),
    CannotReadStringLenError(io::Error),
    CannotReadStringBytesError(io::Error),
    CannotReadIntegerBytesError(io::Error),
    CannotSerializeStringAsInt(String),
    CannotConvertBytesToString(std::str::Utf8Error),
}

impl fmt::Display for SerDeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            Self::WriteError(_io_error) => "error writing to target".to_string(),
            Self::CannotReadStringLenError(_io_error) => "error reading string length from source".to_string(),
            Self::CannotReadStringBytesError(_io_error) => "error reading string bytes from source".to_string(),
            Self::CannotReadIntegerBytesError(_io_error) => "error reading integer bytes from source".to_string(),
            Self::CannotSerializeStringAsInt(string) => format!("string '{}' cannot be used as integer value", string),
            Self::CannotConvertBytesToString(_utf8_error) => "cannot convert provided bytes to a utf8 string".to_string(),
        };
        write!(f, "{}", message)
    }
}

impl Error for SerDeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::WriteError(io_error) => Some(io_error),
            Self::CannotReadStringLenError(io_error) => Some(io_error),
            Self::CannotReadStringBytesError(io_error) => Some(io_error),
            Self::CannotReadIntegerBytesError(io_error) => Some(io_error),
            Self::CannotSerializeStringAsInt(_) => None,
            Self::CannotConvertBytesToString(utf8_error) => Some(utf8_error),
        }
    }
}


pub fn serialize_into<W: Write>(mut destination: W, column_type: ColumnType, value: &SqlValue) -> Result<(), SerDeError> {
    match column_type {
        ColumnType::String => {
            let blob = serialize_string(value);
            destination.write(&blob).map_err(SerDeError::WriteError)?;
        },
        ColumnType::Integer => {
            let blob = serialize_int(value)?;
            destination.write(&blob).map_err(SerDeError::WriteError)?;
        }
    }

    Ok(())
}

pub fn deserialize<R: Read>(mut source: R, column_type: ColumnType) -> Result<SqlValue, SerDeError> {
    match column_type {
        ColumnType::String => {
            let mut len_blob = [0u8];
            source.read(&mut len_blob).map_err(SerDeError::CannotReadStringLenError)?;
            let len = len_blob[0] as usize;
            let mut str_blob = vec![0u8; len];
            source.read(&mut str_blob).map_err(SerDeError::CannotReadStringBytesError)?;
            let string = std::str::from_utf8(&str_blob).map_err(SerDeError::CannotConvertBytesToString)?;
            Ok(SqlValue::String(string.to_owned()))
        },
        ColumnType::Integer => {
            let mut blob = [0u8; 8];
            source.read(&mut blob).map_err(SerDeError::CannotReadIntegerBytesError)?;

            let int = i64::from_le_bytes(blob);
            Ok(SqlValue::Integer(int))
        }
    }
}

fn serialize_int(value: &SqlValue) -> Result<[u8; 8], SerDeError> {
    match value {
        SqlValue::Integer(int) => Ok(int.to_le_bytes()),
        SqlValue::String(string) | SqlValue::Identificator(string) =>
            Err(SerDeError::CannotSerializeStringAsInt(string.clone())),
        SqlValue::Null => Ok([0; 8]),
    }
}

fn serialize_string(value: &SqlValue) -> [u8; 256] {
    match value {
        SqlValue::Integer(int) => {
            let string = int.to_string();
            serialize_native_string(&string)
        },
        SqlValue::String(string) | SqlValue::Identificator(string) => serialize_native_string(string),
        SqlValue::Null => [0; 256],
    }
}

fn serialize_native_string(string: &str) -> [u8; 256] {
    let len = string.len();
    let mut result = [0u8; 256];
    result[0] = len as u8;
    for (i, byte) in string.bytes().enumerate() {
        result[i+1] = byte;
    }
    result
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
