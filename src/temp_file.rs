use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::env;

use crate::helpers::get_timestamp;

pub struct TempFile {
    pub temp_dir_path: PathBuf,
    pub file_path: PathBuf,
}

impl TempFile {
    pub fn new(name: &str) -> io::Result<TempFile> {
        let mut path = env::temp_dir();
        path.push(Self::generate_temdir_name());
        let temp_dir_path = path.clone();
        fs::create_dir(temp_dir_path.to_str().unwrap())?;
        path.push(name);
        fs::File::create(path.to_str().unwrap())?;
        Ok(TempFile { temp_dir_path, file_path: path })
    }

    pub fn path(&self) -> &Path {
        self.file_path.as_path()
    }

    pub fn read_bytes(&self, start_at: u64, size: usize) -> io::Result<Vec<u8>> {
         let mut hash_file = OpenOptions::new()
             .read(true)
             .open(self.file_path.to_str().unwrap())?;

         hash_file.seek(SeekFrom::Start(start_at))?;
         let mut blob = vec![0u8; size];
         hash_file.read_exact(&mut blob)?;
         Ok(blob)
    }

    pub fn read_u64(&self, start_at: u64) -> io::Result<[u8; 8]> {
        let bytes: [u8; 8] = self.read_bytes(start_at, 8)?.try_into().unwrap();

        Ok(bytes)
    }

    pub fn write_bytes(&self, contents: &[u8]) -> io::Result<()> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(self.file_path.to_str().unwrap())?;
        file.write_all(contents)
    }

    pub fn writeln_str(&self, contents: &str) -> io::Result<()> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(self.file_path.to_str().unwrap())?;
        writeln!(file, "{}", contents)
    }

    fn generate_temdir_name() -> String {
        format!("yarrd-test-{}", get_timestamp())
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        fs::remove_dir_all(self.temp_dir_path.to_str().unwrap()).unwrap();
    }
}
