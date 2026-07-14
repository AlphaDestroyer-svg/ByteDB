use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

pub fn build_acceptor(cert_path: &Path, key_path: &Path) -> Result<TlsAcceptor, String> {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();

    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("invalid certificate/key pair: {e}"))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let file = File::open(path).map_err(|e| format!("open cert {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("parse certs {}: {e}", path.display()))?;
    if certs.is_empty() {
        return Err(format!("no certificates found in {}", path.display()));
    }
    Ok(certs)
}

fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    let file = File::open(path).map_err(|e| format!("open key {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| format!("parse key {}: {e}", path.display()))?
        .ok_or_else(|| format!("no PKCS#8/SEC1 private key found in {}", path.display()))
}
