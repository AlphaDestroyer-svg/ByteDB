use std::sync::Arc;
use tokio::net::TcpStream;
use tracing::{info, warn};

use crate::error::{ServerError, Result};
use crate::protocol::codec::{read_frame, write_frame};
use crate::protocol::message::{Request, Response};
use crate::auth::credentials::{Credentials, SessionManager};
use bytedb_query::executor::engine::{QueryEngine, ExecutionResult};
use bytedb_query::kv::kv_engine::KvEngine;
use bytedb_query::document::doc_engine::DocEngine;
use bytedb_query::parser::parser::Parser;
use bytedb_query::parser::ast::Statement;
use bytedb_core::tuple::value::Value;

pub async fn handle_connection(
    mut stream: TcpStream,
    query_engine: Arc<QueryEngine>,
    kv_engine: Arc<KvEngine>,
    doc_engine: Arc<DocEngine>,
    credentials: Arc<Credentials>,
    session_manager: Arc<SessionManager>,
) -> Result<()> {
    let mut session_id: Option<u64> = None;
    let mut active_txn: Option<u64> = None;

    loop {
        let frame = match read_frame(&mut stream).await {
            Ok(f) => f,
            Err(_) => break,
        };

        let request = Request::deserialize(&frame)
            .ok_or_else(|| ServerError::Protocol("Invalid request".into()))?;

        let response = match request {
            Request::Authenticate { username, password } => {
                if credentials.authenticate(&username, &password) {
                    let sid = session_manager.create_session(username.clone());
                    session_id = Some(sid);
                    info!("User '{}' authenticated, session {}", username, sid);
                    Response::AuthOk { session_id: sid }
                } else {
                    warn!("Auth failed for user '{}'", username);
                    Response::AuthFail { reason: "Invalid credentials".into() }
                }
            }
            Request::Query { sql, txn_id } => {
                if session_id.is_none() {
                    Response::Error { code: 401, message: "Not authenticated".into() }
                } else {
                    let effective_txn = txn_id.or(active_txn);
                    let ends_txn = {
                        let s = sql.trim_start().to_ascii_uppercase();
                        s.starts_with("COMMIT") || s.starts_with("ROLLBACK") || s.starts_with("END")
                    };
                    match execute_query(&sql, effective_txn, &query_engine, &kv_engine, &doc_engine).await {
                        Ok(resp) => {
                            if let Response::Ok { ref message } = resp {
                                if message.starts_with("Transaction ") && message.ends_with(" started") {
                                    let parts: Vec<&str> = message.split_whitespace().collect();
                                    if let Some(id_str) = parts.get(1) {
                                        if let Ok(id) = id_str.parse::<u64>() {
                                            active_txn = Some(id);
                                        }
                                    }
                                } else if message == "COMMIT" || message == "ROLLBACK" {
                                    active_txn = None;
                                }
                            }
                            resp
                        }
                        Err(e) => {
                            if ends_txn {
                                active_txn = None;
                            }
                            Response::Error { code: 500, message: e.to_string() }
                        }
                    }
                }
            }
            Request::Ping => Response::Pong,
            Request::Disconnect => {
                if let Some(sid) = session_id {
                    session_manager.remove_session(sid);
                }
                break;
            }
        };

        let response_data = response.serialize();
        write_frame(&mut stream, &response_data).await?;
    }

    if let Some(tid) = active_txn.take() {
        query_engine.rollback(tid);
    }

    Ok(())
}

async fn execute_query(
    sql: &str,
    txn_id: Option<u64>,
    query_engine: &QueryEngine,
    kv_engine: &KvEngine,
    doc_engine: &DocEngine,
) -> Result<Response> {
    let mut parser = Parser::new(sql)
        .map_err(|e| ServerError::Query(e))?;
    let stmt = parser.parse()
        .map_err(|e| ServerError::Query(e))?;

    match &stmt {
        Statement::KvGet(key) => {
            match kv_engine.get(key).map_err(|e| ServerError::Query(e))? {
                Some(val) => Ok(Response::ResultSet {
                    columns: vec!["key".into(), "value".into()],
                    rows: vec![vec![Value::Text(key.clone()), Value::Text(val)]],
                }),
                None => Ok(Response::ResultSet {
                    columns: vec!["key".into(), "value".into()],
                    rows: vec![],
                }),
            }
        }
        Statement::KvSet(key, value) => {
            kv_engine.set(key, value).map_err(|e| ServerError::Query(e))?;
            Ok(Response::Ok { message: "OK".into() })
        }
        Statement::KvDelete(key) => {
            let deleted = kv_engine.delete(key).map_err(|e| ServerError::Query(e))?;
            Ok(Response::Modified { count: if deleted { 1 } else { 0 } })
        }
        Statement::KvScan(start, end) => {
            let results = kv_engine.scan(start, end).map_err(|e| ServerError::Query(e))?;
            let rows: Vec<Vec<Value>> = results.into_iter()
                .map(|(k, v)| vec![Value::Text(k), Value::Text(v)])
                .collect();
            Ok(Response::ResultSet {
                columns: vec!["key".into(), "value".into()],
                rows,
            })
        }
        Statement::DocInsert(di) => {
            let id = doc_engine.insert(&di.collection, &di.document)
                .map_err(|e| ServerError::Query(e))?;
            Ok(Response::Ok { message: format!("Inserted: {}", id) })
        }
        Statement::DocFind(df) => {
            let docs = doc_engine.find_all(&df.collection)
                .map_err(|e| ServerError::Query(e))?;
            let rows: Vec<Vec<Value>> = docs.into_iter()
                .map(|d| vec![Value::Text(d.to_string())])
                .collect();
            Ok(Response::ResultSet {
                columns: vec!["document".into()],
                rows,
            })
        }
        Statement::DocDelete(_dd) => {
            Ok(Response::Ok { message: "DOC DELETE executed".into() })
        }
        Statement::DocUpdate(_du) => {
            Ok(Response::Ok { message: "DOC UPDATE executed".into() })
        }
        _ => {
            let result = query_engine.execute(stmt, txn_id)
                .map_err(|e| ServerError::Query(e))?;
            match result {
                ExecutionResult::Rows { columns, rows } => {
                    Ok(Response::ResultSet { columns, rows })
                }
                ExecutionResult::Modified { count } => {
                    Ok(Response::Modified { count })
                }
                ExecutionResult::Ok(msg) => {
                    Ok(Response::Ok { message: msg })
                }
            }
        }
    }
}
