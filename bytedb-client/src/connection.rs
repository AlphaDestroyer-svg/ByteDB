use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::protocol::{Request, Response};

pub struct ClientConnection {
    stream: TcpStream,
    #[allow(dead_code)]
    pub session_id: u64,
    pub active_txn: Option<u64>,
}

impl ClientConnection {
    pub async fn connect(addr: &str, username: &str, password: &str) -> Result<Self, String> {
        let mut stream = TcpStream::connect(addr).await
            .map_err(|e| format!("Connection failed: {}", e))?;

        let auth_req = Request::Authenticate {
            username: username.to_string(),
            password: password.to_string(),
        };

        Self::send_raw(&mut stream, &auth_req.serialize()).await
            .map_err(|e| format!("Send failed: {}", e))?;

        let response_data = Self::recv_raw(&mut stream).await
            .map_err(|e| format!("Recv failed: {}", e))?;

        let response = Response::deserialize(&response_data)
            .ok_or_else(|| "Invalid response".to_string())?;

        match response {
            Response::AuthOk { session_id } => {
                Ok(ClientConnection {
                    stream,
                    session_id,
                    active_txn: None,
                })
            }
            Response::AuthFail { reason } => Err(format!("Authentication failed: {}", reason)),
            _ => Err("Unexpected response".to_string()),
        }
    }

    pub async fn query(&mut self, sql: &str) -> Result<Response, String> {
        let req = Request::Query {
            sql: sql.to_string(),
            txn_id: self.active_txn,
        };

        Self::send_raw(&mut self.stream, &req.serialize()).await
            .map_err(|e| format!("Send failed: {}", e))?;

        let response_data = Self::recv_raw(&mut self.stream).await
            .map_err(|e| format!("Recv failed: {}", e))?;

        let response = Response::deserialize(&response_data)
            .ok_or_else(|| "Invalid response".to_string())?;

        if let Response::Ok { ref message } = response {
            if message.starts_with("Transaction ") && message.ends_with(" started") {
                let parts: Vec<&str> = message.split_whitespace().collect();
                if let Some(id_str) = parts.get(1) {
                    if let Ok(id) = id_str.parse::<u64>() {
                        self.active_txn = Some(id);
                    }
                }
            } else if message == "COMMIT" || message == "ROLLBACK" {
                self.active_txn = None;
            }
        }

        Ok(response)
    }

    pub async fn ping(&mut self) -> Result<(), String> {
        let req = Request::Ping;
        Self::send_raw(&mut self.stream, &req.serialize()).await
            .map_err(|e| format!("Send failed: {}", e))?;

        let response_data = Self::recv_raw(&mut self.stream).await
            .map_err(|e| format!("Recv failed: {}", e))?;

        let response = Response::deserialize(&response_data)
            .ok_or_else(|| "Invalid response".to_string())?;

        match response {
            Response::Pong => Ok(()),
            _ => Err("Unexpected response to ping".to_string()),
        }
    }

    pub async fn disconnect(&mut self) -> Result<(), String> {
        let req = Request::Disconnect;
        Self::send_raw(&mut self.stream, &req.serialize()).await
            .map_err(|e| format!("Send failed: {}", e))?;
        Ok(())
    }

    async fn send_raw(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
        let len = data.len() as u32;
        stream.write_all(&len.to_le_bytes()).await?;
        stream.write_all(data).await?;
        stream.flush().await?;
        Ok(())
    }

    async fn recv_raw(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut data = vec![0u8; len];
        stream.read_exact(&mut data).await?;
        Ok(data)
    }
}
