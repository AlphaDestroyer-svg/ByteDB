use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{ServerError, Result};

pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

pub async fn read_frame<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > MAX_FRAME_SIZE {
        return Err(ServerError::Protocol(format!("Frame too large: {} bytes", len)));
    }

    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await?;
    Ok(data)
}

pub async fn write_frame<S: AsyncWrite + Unpin>(stream: &mut S, data: &[u8]) -> Result<()> {
    let len = data.len() as u32;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(data).await?;
    stream.flush().await?;
    Ok(())
}
