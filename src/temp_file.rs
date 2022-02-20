use std::time;
use std::fs;
use std::path::PathBuf;
use std::io::{self, Write};
use std::env;

pub struct TempFile {
    temp_dir_path: PathBuf,
    file_path: PathBuf,
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

    pub fn path(&self) -> &str {
        self.file_path.to_str().unwrap()
    }

    pub fn write_bytes(&self, contents: &[u8]) -> io::Result<()> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(self.file_path.to_str().unwrap())?;
        file.write_all(contents)
    }

    fn generate_temdir_name() -> String {
        format!("yarrd-test-{}", Self::get_timestamp())
    }

    fn get_timestamp() -> u128 {
        time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

}

impl Drop for TempFile {
    fn drop(&mut self) {
        fs::remove_dir_all(self.temp_dir_path.to_str().unwrap()).unwrap();
    }
}
