use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::RwLock;

pub struct Credentials {
    users: RwLock<HashMap<String, String>>,
}

impl Credentials {
    pub fn new() -> Self {
        let mut users = HashMap::new();
        users.insert("admin".to_string(), "admin".to_string());
        Credentials {
            users: RwLock::new(users),
        }
    }

    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        if let Some(stored) = self.users.read().get(username) {
            stored == password
        } else {
            false
        }
    }

    #[allow(dead_code)]
    pub fn add_user(&self, username: String, password: String) {
        self.users.write().insert(username, password);
    }

    #[allow(dead_code)]
    pub fn remove_user(&self, username: &str) -> bool {
        self.users.write().remove(username).is_some()
    }
}

impl Default for Credentials {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SessionManager {
    next_id: AtomicU64,
    sessions: RwLock<HashMap<u64, Session>>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Session {
    pub id: u64,
    pub username: String,
    pub active_txn: Option<u64>,
}

impl SessionManager {
    pub fn new() -> Self {
        SessionManager {
            next_id: AtomicU64::new(1),
            sessions: RwLock::new(HashMap::new()),
        }
    }

    pub fn create_session(&self, username: String) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let session = Session {
            id,
            username,
            active_txn: None,
        };
        self.sessions.write().insert(id, session);
        id
    }

    #[allow(dead_code)]
    pub fn get_session(&self, id: u64) -> Option<Session> {
        self.sessions.read().get(&id).cloned()
    }

    #[allow(dead_code)]
    pub fn set_txn(&self, session_id: u64, txn_id: Option<u64>) {
        if let Some(session) = self.sessions.write().get_mut(&session_id) {
            session.active_txn = txn_id;
        }
    }

    pub fn remove_session(&self, id: u64) {
        self.sessions.write().remove(&id);
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}
