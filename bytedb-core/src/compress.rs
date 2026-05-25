const MIN_COMPRESS_SIZE: usize = 1024;

#[derive(Debug)]
pub enum Algo {
    Lz4,
    Zstd,
}

pub fn compress(data: &[u8]) -> Option<(Vec<u8>, Algo)> {
    if data.len() < MIN_COMPRESS_SIZE {
        return None;
    }

    let lz4_buf = lz4_flex::compress(data);
    if lz4_buf.len() < data.len() {
        let mut out = Vec::with_capacity(13 + lz4_buf.len());
        out.push(12);
        out.extend_from_slice(&(data.len() as u64).to_le_bytes());
        out.extend_from_slice(&(lz4_buf.len() as u32).to_le_bytes());
        out.extend_from_slice(&lz4_buf);
        return Some((out, Algo::Lz4));
    }

    let zstd_buf = zstd::bulk::compress(data, 0).ok()?;
    if zstd_buf.len() < data.len() {
        let mut out = Vec::with_capacity(13 + zstd_buf.len());
        out.push(13);
        out.extend_from_slice(&(data.len() as u64).to_le_bytes());
        out.extend_from_slice(&(zstd_buf.len() as u32).to_le_bytes());
        out.extend_from_slice(&zstd_buf);
        return Some((out, Algo::Zstd));
    }

    None
}

pub fn decompress(data: &[u8]) -> Option<Vec<u8>> {
    if data.len() < 13 {
        return None;
    }
    let tag = data[0];
    let orig_size = u64::from_le_bytes(data[1..9].try_into().ok()?) as usize;
    let comp_size = u32::from_le_bytes(data[9..13].try_into().ok()?) as usize;
    if data.len() < 13 + comp_size as usize {
        return None;
    }
    let compressed = &data[13..13 + comp_size as usize];
    match tag {
        12 => {
            let out = lz4_flex::decompress(compressed, orig_size).ok()?;
            if out.len() == orig_size { Some(out) } else { None }
        }
        13 => {
            let out = zstd::stream::decode_all(compressed).ok()?;
            if out.len() == orig_size { Some(out) } else { None }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lz4_flex_round_trip() {
        let data: Vec<u8> = (0..2000usize).map(|i| i as u8).collect();
        let compressed = lz4_flex::compress(&data);
        let decompressed = lz4_flex::decompress(&compressed, 2000);
        assert!(decompressed.is_ok());
        assert_eq!(decompressed.unwrap(), data);
    }

    #[test]
    fn test_compress_decompress_directly() {
        let data: Vec<u8> = (0..2000usize).map(|i| i as u8).collect();
        let result = compress(&data);
        assert!(result.is_some());
        let (encoded, algo) = result.unwrap();

        let decoded = decompress(&encoded);
        assert!(decoded.is_some(), "decompress failed for algo {:?}", algo);
        assert_eq!(decoded.unwrap(), data);
    }
}
