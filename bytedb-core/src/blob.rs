use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use uuid::Uuid;

const BLOB_MAGIC: [u8; 4] = *b"BDBL";
const BLOB_VERSION: u8 = 1;
const TAG_FILE_REF: u8 = 22;

const MAX_INLINE_BYTES: usize = 65536;

pub struct BlobStore {
    blobs_dir: PathBuf,
}

#[derive(Clone)]
pub struct BlobRef {
    pub uuid: Uuid,
    pub size: u64,
    pub compressed: bool,
    pub algo: u8,
}

impl BlobStore {
    pub fn new(root: &PathBuf) -> std::io::Result<Self> {
        let blobs_dir = root.join("blobs");
        fs::create_dir_all(&blobs_dir)?;
        Ok(BlobStore { blobs_dir })
    }

    pub fn store(&self, data: &[u8]) -> std::io::Result<BlobRef> {
        if data.len() <= MAX_INLINE_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("data too small for external blob: {} bytes", data.len()),
            ));
        }

        let uuid = Uuid::new_v4();
        let path = self.blob_path(&uuid);

        let (algo, compressed) = self.write_blob_file(&path, data)?;

        Ok(BlobRef {
            uuid,
            size: data.len() as u64,
            compressed,
            algo,
        })
    }

    fn write_blob_file(&self, path: &PathBuf, data: &[u8]) -> std::io::Result<(u8, bool)> {
        let (algo, encoded) = match crate::compress::compress(data) {
            Some((enc, _)) => {
                let algo = enc[0];
                (algo, enc)
            }
            None => (5, {
                let mut enc = Vec::with_capacity(13 + data.len());
                enc.push(5);
                enc.extend_from_slice(&(data.len() as u64).to_le_bytes());
                enc.extend_from_slice(&(data.len() as u32).to_le_bytes());
                enc.extend_from_slice(data);
                enc
            }),
        };

        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)?;

        file.write_all(&BLOB_MAGIC)?;
        file.write_all(&[BLOB_VERSION])?;
        file.write_all(&encoded)?;
        file.flush()?;
        drop(file);

        Ok((algo, encoded[0] != 5))
    }

    pub fn load(&self, ref_: &BlobRef) -> std::io::Result<Vec<u8>> {
        let path = self.blob_path(&ref_.uuid);
        let mut file = File::open(&path)?;

        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != &BLOB_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid blob magic",
            ));
        }

        let mut version = [0u8; 1];
        file.read_exact(&mut version)?;
        if version[0] != BLOB_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unsupported blob version",
            ));
        }

        let mut header = [0u8; 13];
        file.read_exact(&mut header)?;
        let algo = header[0];
        let _orig_size = u64::from_le_bytes(header[1..9].try_into().unwrap());
        let comp_size = u32::from_le_bytes(header[9..13].try_into().unwrap()) as usize;

        let mut encoded = vec![0u8; 13 + comp_size];
        encoded[..13].copy_from_slice(&header);
        file.read_exact(&mut encoded[13..])?;

        if algo == 12 || algo == 13 {
            crate::compress::decompress(&encoded)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "decompression failed"))
        } else {
            Ok(encoded[13..].to_vec())
        }
    }

    pub fn delete(&self, ref_: &BlobRef) -> std::io::Result<()> {
        let path = self.blob_path(&ref_.uuid);
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    fn blob_path(&self, uuid: &Uuid) -> PathBuf {
        self.blobs_dir.join(format!("{}.blob", uuid))
    }

    pub fn blobs_dir(&self) -> &PathBuf {
        &self.blobs_dir
    }
}

pub const TAG_BLOB_FILE_REF: u8 = TAG_FILE_REF;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_blob_store_round_trip() {
        let tmp = TempDir::new().unwrap();
        let store = BlobStore::new(&tmp.path().to_path_buf()).unwrap();

        let data: Vec<u8> = (0..100_000usize).map(|i| i as u8).collect();
        let ref_ = store.store(&data).unwrap();
        assert_eq!(ref_.size, 100_000);

        let loaded = store.load(&ref_).unwrap();
        assert_eq!(loaded, data);

        store.delete(&ref_).unwrap();
        assert!(!store.blob_path(&ref_.uuid).exists());
    }

    #[test]
    fn test_blob_store_too_small() {
        let tmp = TempDir::new().unwrap();
        let store = BlobStore::new(&tmp.path().to_path_buf()).unwrap();
        let data = vec![0u8; 1000];
        let result = store.store(&data);
        assert!(result.is_err());
    }
}
