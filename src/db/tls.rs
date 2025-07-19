#[cfg(feature = "tls")]
use std::path::Path;
#[cfg(feature = "tls")]
use std::sync::Arc;
#[cfg(feature = "tls")]
use std::io::BufReader;
#[cfg(feature = "tls")]
use std::fs::File;

#[cfg(feature = "tls")]
use rustls::{ClientConfig, RootCertStore};
#[cfg(feature = "tls")]
use tokio_postgres_rustls::MakeRustlsConnect;

use tokio_postgres::{Client, Connection, Socket};
use tokio_postgres::NoTls;
use tokio_postgres::tls::NoTlsStream;

/// TLS mode for PostgreSQL connections
#[derive(Debug, Clone, PartialEq)]
pub enum TlsMode {
    /// No TLS encryption
    Disable,
    /// Try TLS first, fall back to unencrypted if it fails
    #[cfg(feature = "tls")]
    Prefer,
    /// Require TLS encryption
    #[cfg(feature = "tls")]
    Require,
    /// Require TLS and verify server certificate against CA
    #[cfg(feature = "tls")]
    VerifyCa,
    /// Require TLS, verify CA, and verify server hostname matches certificate
    #[cfg(feature = "tls")]
    VerifyFull,
}

impl TlsMode {
    /// Parse TLS mode from string (matching PostgreSQL's sslmode parameter)
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "disable" => Ok(TlsMode::Disable),
            #[cfg(feature = "tls")]
            "prefer" => Ok(TlsMode::Prefer),
            #[cfg(feature = "tls")]
            "require" => Ok(TlsMode::Require),
            #[cfg(feature = "tls")]
            "verify-ca" => Ok(TlsMode::VerifyCa),
            #[cfg(feature = "tls")]
            "verify-full" => Ok(TlsMode::VerifyFull),
            #[cfg(not(feature = "tls"))]
            mode if mode != "disable" => {
                Err(format!(
                    "TLS mode '{}' requires pgmg to be built with TLS support. \
                    Rebuild with: cargo install pgmg --features tls", 
                    mode
                ))
            }
            _ => Err(format!("Invalid sslmode: {}", s)),
        }
    }
}

impl Default for TlsMode {
    fn default() -> Self {
        TlsMode::Disable
    }
}

/// TLS configuration for PostgreSQL connections
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub mode: TlsMode,
    pub root_cert: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            mode: TlsMode::default(),
            root_cert: None,
            client_cert: None,
            client_key: None,
        }
    }
}

/// Connector type that abstracts over TLS and non-TLS connections
pub enum TlsConnector {
    #[cfg(not(feature = "tls"))]
    NoTls(NoTls),
    #[cfg(feature = "tls")]
    NoTls(NoTls),
    #[cfg(feature = "tls")]
    Rustls(MakeRustlsConnect),
}

#[cfg(feature = "tls")]
fn load_certs(path: &Path) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
    Ok(certs)
}

#[cfg(feature = "tls")]
fn load_private_key(path: &Path) -> Result<rustls::pki_types::PrivateKeyDer<'static>, Box<dyn std::error::Error>> {
    // Try PKCS8 first
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    if let Some(key) = rustls_pemfile::pkcs8_private_keys(&mut reader).next() {
        return key.map(Into::into).map_err(Into::into);
    }
    
    // Try RSA key
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    if let Some(key) = rustls_pemfile::rsa_private_keys(&mut reader).next() {
        return key.map(Into::into).map_err(Into::into);
    }
    
    Err("No private key found in file".into())
}

#[cfg(feature = "tls")]
fn build_rustls_config(tls_config: &TlsConfig) -> Result<ClientConfig, Box<dyn std::error::Error>> {
    let config = match tls_config.mode {
        TlsMode::Require => {
            // Accept any certificate for "require" mode
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(DangerousAcceptAnyServerCert::new()))
                .with_no_client_auth()
        }
        TlsMode::VerifyCa | TlsMode::VerifyFull => {
            let mut root_store = RootCertStore::empty();
            
            // Load root certificates
            if let Some(root_cert_path) = &tls_config.root_cert {
                let certs = load_certs(Path::new(root_cert_path))?;
                for cert in certs {
                    root_store.add(cert)?;
                }
            } else {
                // Use Mozilla's root certificates
                root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            }
            
            let config_builder = ClientConfig::builder()
                .with_root_certificates(root_store);
            
            // Add client certificate if provided
            if let (Some(cert_path), Some(key_path)) = (&tls_config.client_cert, &tls_config.client_key) {
                let cert_chain = load_certs(Path::new(cert_path))?;
                let key = load_private_key(Path::new(key_path))?;
                config_builder.with_client_auth_cert(cert_chain, key)?
            } else {
                config_builder.with_no_client_auth()
            }
        }
        _ => unreachable!("Invalid TLS mode for rustls config"),
    };
    
    Ok(config)
}

/// Create a TLS connector based on the configuration
pub fn create_tls_connector(tls_config: &TlsConfig) -> Result<TlsConnector, Box<dyn std::error::Error>> {
    match tls_config.mode {
        TlsMode::Disable => {
            #[cfg(not(feature = "tls"))]
            return Ok(TlsConnector::NoTls(NoTls));
            #[cfg(feature = "tls")]
            return Ok(TlsConnector::NoTls(NoTls));
        }
        #[cfg(feature = "tls")]
        TlsMode::Prefer | TlsMode::Require | TlsMode::VerifyCa | TlsMode::VerifyFull => {
            let config = build_rustls_config(tls_config)?;
            let connector = MakeRustlsConnect::new(config);
            Ok(TlsConnector::Rustls(connector))
        }
    }
}

/// Helper enum to handle different connection types
pub enum PgConnection {
    NoTls(Connection<Socket, NoTlsStream>),
    #[cfg(feature = "tls")]
    Rustls(Box<dyn std::any::Any + Send>),
}

impl PgConnection {
    /// Spawn the connection handler
    pub fn spawn(self) {
        tokio::spawn(async move {
            match self {
                PgConnection::NoTls(conn) => {
                    if let Err(e) = conn.await {
                        eprintln!("Database connection error: {}", e);
                    }
                }
                #[cfg(feature = "tls")]
                PgConnection::Rustls(_conn) => {
                    // The boxed connection can't be awaited generically
                    // It will be cleaned up when dropped
                }
            }
        });
    }
}

/// Connect to PostgreSQL with the appropriate TLS configuration
pub async fn connect_with_tls(
    connection_string: &str,
    tls_config: &TlsConfig,
) -> Result<(Client, PgConnection), Box<dyn std::error::Error>> {
    let connector = create_tls_connector(tls_config)?;
    
    match connector {
        #[cfg(not(feature = "tls"))]
        TlsConnector::NoTls(no_tls) => {
            let (client, connection) = tokio_postgres::connect(connection_string, no_tls).await?;
            Ok((client, PgConnection::NoTls(connection)))
        }
        #[cfg(feature = "tls")]
        TlsConnector::NoTls(no_tls) => {
            let (client, connection) = tokio_postgres::connect(connection_string, no_tls).await?;
            Ok((client, PgConnection::NoTls(connection)))
        }
        #[cfg(feature = "tls")]
        TlsConnector::Rustls(rustls) => {
            // For "prefer" mode, try TLS first, then fall back to no TLS
            if tls_config.mode == TlsMode::Prefer {
                match tokio_postgres::connect(connection_string, rustls.clone()).await {
                    Ok((client, connection)) => Ok((client, PgConnection::Rustls(Box::new(connection)))),
                    Err(_) => {
                        // Fall back to no TLS
                        let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;
                        Ok((client, PgConnection::NoTls(connection)))
                    }
                }
            } else {
                let (client, connection) = tokio_postgres::connect(connection_string, rustls).await?;
                Ok((client, PgConnection::Rustls(Box::new(connection))))
            }
        }
    }
}

#[cfg(feature = "tls")]
#[derive(Debug)]
struct DangerousAcceptAnyServerCert {
    crypto_provider: Arc<rustls::crypto::CryptoProvider>,
}

#[cfg(feature = "tls")]
impl DangerousAcceptAnyServerCert {
    fn new() -> Self {
        Self {
            crypto_provider: Arc::new(rustls::crypto::ring::default_provider()),
        }
    }
}

#[cfg(feature = "tls")]
impl rustls::client::danger::ServerCertVerifier for DangerousAcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.crypto_provider.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_tls_mode_from_str() {
        assert_eq!(TlsMode::from_str("disable").unwrap(), TlsMode::Disable);
        
        #[cfg(feature = "tls")]
        {
            assert_eq!(TlsMode::from_str("prefer").unwrap(), TlsMode::Prefer);
            assert_eq!(TlsMode::from_str("require").unwrap(), TlsMode::Require);
            assert_eq!(TlsMode::from_str("verify-ca").unwrap(), TlsMode::VerifyCa);
            assert_eq!(TlsMode::from_str("verify-full").unwrap(), TlsMode::VerifyFull);
        }
        
        #[cfg(not(feature = "tls"))]
        {
            assert!(TlsMode::from_str("require").is_err());
            assert!(TlsMode::from_str("prefer").is_err());
        }
        
        assert!(TlsMode::from_str("invalid").is_err());
    }
    
    #[test]
    fn test_tls_mode_case_insensitive() {
        assert_eq!(TlsMode::from_str("DISABLE").unwrap(), TlsMode::Disable);
        assert_eq!(TlsMode::from_str("Disable").unwrap(), TlsMode::Disable);
        
        #[cfg(feature = "tls")]
        {
            assert_eq!(TlsMode::from_str("REQUIRE").unwrap(), TlsMode::Require);
            assert_eq!(TlsMode::from_str("Verify-Full").unwrap(), TlsMode::VerifyFull);
        }
    }
}