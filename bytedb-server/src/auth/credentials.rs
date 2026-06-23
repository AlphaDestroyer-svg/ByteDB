use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::RwLock;

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use rand_core::OsRng;
use argon2::Argon2;

pub struct Credentials {
    users: RwLock<HashMap<String, String>>,
}

fn hash_password(password: &str) -> Option<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .ok()
        .map(|h| h.to_string())
}

impl Credentials {
    pub fn new() -> Self {
        let mut users = HashMap::new();
        if let Some(h) = hash_password("admin") {
            users.insert("admin".to_string(), h);
        }
        Credentials {
            users: RwLock::new(users),
        }
    }

    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        let stored = match self.users.read().get(username) {
            Some(h) => h.clone(),
            None => return false,
        };
        let parsed = match PasswordHash::new(&stored) {
            Ok(p) => p,
            Err(_) => return false,
        };
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
    }

    #[allow(dead_code)]
    pub fn add_user(&self, username: String, password: String) -> bool {
        match hash_password(&password) {
            Some(h) => {
                self.users.write().insert(username, h);
                true
            }
            None => false,
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_admin_authenticates() {
        let c = Credentials::new();
        assert!(c.authenticate("admin", "admin"));
        assert!(!c.authenticate("admin", "wrong"));
        assert!(!c.authenticate("nobody", "admin"));
    }

    #[test]
    fn passwords_are_hashed_not_plaintext() {
        let c = Credentials::new();
        let stored = c.users.read().get("admin").cloned().unwrap();
        assert_ne!(stored, "admin", "password must not be stored in plaintext");
        assert!(stored.starts_with("$argon2"), "must be an argon2 PHC hash, got {stored}");
    }

    #[test]
    fn added_user_authenticates_and_distinct_salts() {
        let c = Credentials::new();
        assert!(c.add_user("alice".into(), "s3cret".into()));
        assert!(c.authenticate("alice", "s3cret"));
        assert!(!c.authenticate("alice", "S3cret"));

        // same password for two users must yield different hashes (random salt)
        assert!(c.add_user("bob".into(), "s3cret".into()));
        let ha = c.users.read().get("alice").cloned().unwrap();
        let hb = c.users.read().get("bob").cloned().unwrap();
        assert_ne!(ha, hb, "identical passwords must hash differently (per-user salt)");
    }

    #[test]
    fn removed_user_cannot_authenticate() {
        let c = Credentials::new();
        c.add_user("temp".into(), "pw".into());
        assert!(c.authenticate("temp", "pw"));
        assert!(c.remove_user("temp"));
        assert!(!c.authenticate("temp", "pw"));
    }
}
