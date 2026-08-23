use std::fmt;
use std::io::Cursor;
use std::sync::Arc;

use pkcs8::EncryptedPrivateKeyInfoRef;
use pkcs8::der::{Decode, SecretDocument};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme};
use zeroize::Zeroizing;

/// 会在释放时清零且不会把内容写进 Debug 的敏感字节。
pub struct SecretBytes(Zeroizing<Vec<u8>>);

impl SecretBytes {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(Zeroizing::new(bytes.into()))
    }

    pub fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SecretBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretBytes([REDACTED])")
    }
}

impl From<Vec<u8>> for SecretBytes {
    fn from(value: Vec<u8>) -> Self {
        Self::new(value)
    }
}

impl From<String> for SecretBytes {
    fn from(value: String) -> Self {
        Self::new(value.into_bytes())
    }
}

/// 私钥输入。带密码的格式第一版只接受加密 PKCS#8。
pub enum PrivateKeyInput {
    PlainPem(SecretBytes),
    PlainDer(PrivateKeyDer<'static>),
    EncryptedPkcs8Pem {
        pem: SecretBytes,
        password: SecretBytes,
    },
    EncryptedPkcs8Der {
        der: SecretBytes,
        password: SecretBytes,
    },
}

impl fmt::Debug for PrivateKeyInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::PlainPem(_) => "PlainPem",
            Self::PlainDer(_) => "PlainDer",
            Self::EncryptedPkcs8Pem { .. } => "EncryptedPkcs8Pem",
            Self::EncryptedPkcs8Der { .. } => "EncryptedPkcs8Der",
        };
        f.debug_tuple("PrivateKeyInput").field(&kind).finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TlsConfigError {
    #[error("证书 PEM 无效：{0}")]
    Certificate(String),
    #[error("证书链不能为空")]
    EmptyCertificateChain,
    #[error("私钥 PEM 无效")]
    InvalidPrivateKey,
    #[error("不支持传统 OpenSSL 加密私钥，请先转换为加密 PKCS#8（BEGIN ENCRYPTED PRIVATE KEY）")]
    LegacyEncryptedPrivateKey,
    #[error("私钥解密失败")]
    PrivateKeyDecrypt,
    #[error("信任根无效：{0}")]
    InvalidRoot(String),
    #[error("TLS 配置无效：{0}")]
    Rustls(String),
}

/// 客户端验证服务端身份的策略。
pub enum ServerVerification {
    SystemRoots {
        extra_roots: Vec<CertificateDer<'static>>,
    },
    MozillaRoots {
        extra_roots: Vec<CertificateDer<'static>>,
    },
    CustomRoots {
        roots: Vec<CertificateDer<'static>>,
    },
    /// 跳过证书链、主机名和握手签名验证。只应在受控测试环境使用。
    Disabled,
}

impl Default for ServerVerification {
    fn default() -> Self {
        Self::SystemRoots {
            extra_roots: Vec::new(),
        }
    }
}

impl fmt::Debug for ServerVerification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SystemRoots { extra_roots } => f
                .debug_struct("SystemRoots")
                .field("extra_roots", &extra_roots.len())
                .finish(),
            Self::MozillaRoots { extra_roots } => f
                .debug_struct("MozillaRoots")
                .field("extra_roots", &extra_roots.len())
                .finish(),
            Self::CustomRoots { roots } => f
                .debug_struct("CustomRoots")
                .field("roots", &roots.len())
                .finish(),
            Self::Disabled => f.write_str("Disabled(DANGEROUS)"),
        }
    }
}

#[derive(Clone)]
pub struct ClientTlsConfig(Arc<ClientConfig>);

impl ClientTlsConfig {
    pub fn from_rustls(config: Arc<ClientConfig>) -> Self {
        Self(config)
    }

    pub fn new(
        verification: ServerVerification,
        alpn_protocols: Vec<Vec<u8>>,
    ) -> Result<Self, TlsConfigError> {
        let mut config = client_builder(verification)?.with_no_client_auth();
        config.alpn_protocols = alpn_protocols;
        Ok(Self(Arc::new(config)))
    }

    pub fn with_client_identity(
        verification: ServerVerification,
        certificates: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyInput,
        alpn_protocols: Vec<Vec<u8>>,
    ) -> Result<Self, TlsConfigError> {
        if certificates.is_empty() {
            return Err(TlsConfigError::EmptyCertificateChain);
        }
        let key = decode_private_key(private_key)?;
        let mut config = client_builder(verification)?
            .with_client_auth_cert(certificates, key)
            .map_err(|error| TlsConfigError::Rustls(error.to_string()))?;
        config.alpn_protocols = alpn_protocols;
        Ok(Self(Arc::new(config)))
    }

    pub(crate) fn inner(&self) -> Arc<ClientConfig> {
        self.0.clone()
    }

    /// 取得底层 rustls 配置。QUIC 等同样使用 TLS 1.3 的协议层
    /// 可以复用这份身份验证策略，但不会经过 `TlsService`。
    pub fn rustls_config(&self) -> Arc<ClientConfig> {
        self.0.clone()
    }
}

impl fmt::Debug for ClientTlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClientTlsConfig(..)")
    }
}

#[derive(Clone)]
pub struct ServerTlsConfig(Arc<ServerConfig>);

impl ServerTlsConfig {
    pub fn from_rustls(config: Arc<ServerConfig>) -> Self {
        Self(config)
    }

    pub fn single_cert(
        certificates: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyInput,
        alpn_protocols: Vec<Vec<u8>>,
    ) -> Result<Self, TlsConfigError> {
        if certificates.is_empty() {
            return Err(TlsConfigError::EmptyCertificateChain);
        }
        let key = decode_private_key(private_key)?;
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|error| TlsConfigError::Rustls(error.to_string()))?
            .with_no_client_auth()
            .with_single_cert(certificates, key)
            .map_err(|error| TlsConfigError::Rustls(error.to_string()))?;
        config.alpn_protocols = alpn_protocols;
        Ok(Self(Arc::new(config)))
    }

    pub(crate) fn inner(&self) -> Arc<ServerConfig> {
        self.0.clone()
    }

    /// 取得底层 rustls 配置，供 QUIC 等 TLS 1.3 协议复用证书与 ALPN。
    pub fn rustls_config(&self) -> Arc<ServerConfig> {
        self.0.clone()
    }
}

impl fmt::Debug for ServerTlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ServerTlsConfig(..)")
    }
}

pub fn certificates_from_pem(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
    let certificates = rustls_pemfile::certs(&mut Cursor::new(pem))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| TlsConfigError::Certificate(error.to_string()))?;
    if certificates.is_empty() {
        return Err(TlsConfigError::EmptyCertificateChain);
    }
    Ok(certificates)
}

fn decode_private_key(input: PrivateKeyInput) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
    match input {
        PrivateKeyInput::PlainDer(key) => Ok(key),
        PrivateKeyInput::PlainPem(pem) => {
            let bytes = pem.expose();
            let text = String::from_utf8_lossy(bytes);
            if text.contains("Proc-Type: 4,ENCRYPTED") || text.contains("DEK-Info:") {
                return Err(TlsConfigError::LegacyEncryptedPrivateKey);
            }
            if text.contains("BEGIN ENCRYPTED PRIVATE KEY") {
                return Err(TlsConfigError::PrivateKeyDecrypt);
            }
            rustls_pemfile::private_key(&mut Cursor::new(bytes))
                .map_err(|_| TlsConfigError::InvalidPrivateKey)?
                .ok_or(TlsConfigError::InvalidPrivateKey)
        }
        PrivateKeyInput::EncryptedPkcs8Pem { pem, password } => {
            let text =
                std::str::from_utf8(pem.expose()).map_err(|_| TlsConfigError::PrivateKeyDecrypt)?;
            if !text.contains("BEGIN ENCRYPTED PRIVATE KEY") {
                return Err(TlsConfigError::LegacyEncryptedPrivateKey);
            }
            let (label, document) =
                SecretDocument::from_pem(text).map_err(|_| TlsConfigError::PrivateKeyDecrypt)?;
            if label != "ENCRYPTED PRIVATE KEY" {
                return Err(TlsConfigError::LegacyEncryptedPrivateKey);
            }
            let encrypted = EncryptedPrivateKeyInfoRef::from_der(document.as_bytes())
                .map_err(|_| TlsConfigError::PrivateKeyDecrypt)?;
            let plain = encrypted
                .decrypt(password.expose())
                .map_err(|_| TlsConfigError::PrivateKeyDecrypt)?;
            Ok(PrivatePkcs8KeyDer::from(plain.as_bytes().to_vec()).into())
        }
        PrivateKeyInput::EncryptedPkcs8Der { der, password } => {
            let encrypted = EncryptedPrivateKeyInfoRef::from_der(der.expose())
                .map_err(|_| TlsConfigError::PrivateKeyDecrypt)?;
            let plain = encrypted
                .decrypt(password.expose())
                .map_err(|_| TlsConfigError::PrivateKeyDecrypt)?;
            Ok(PrivatePkcs8KeyDer::from(plain.as_bytes().to_vec()).into())
        }
    }
}

type ClientBuilder = rustls::ConfigBuilder<ClientConfig, rustls::client::WantsClientCert>;

fn client_builder(verification: ServerVerification) -> Result<ClientBuilder, TlsConfigError> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|error| TlsConfigError::Rustls(error.to_string()))?;
    match verification {
        ServerVerification::SystemRoots { extra_roots } => {
            let verifier =
                rustls_platform_verifier::Verifier::new_with_extra_roots(extra_roots, provider)
                    .map_err(|error| TlsConfigError::InvalidRoot(error.to_string()))?;
            Ok(builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier)))
        }
        ServerVerification::MozillaRoots { extra_roots } => {
            let mut roots =
                RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            add_roots(&mut roots, extra_roots)?;
            Ok(builder.with_root_certificates(roots))
        }
        ServerVerification::CustomRoots { roots } => {
            let mut store = RootCertStore::empty();
            add_roots(&mut store, roots)?;
            Ok(builder.with_root_certificates(store))
        }
        ServerVerification::Disabled => Ok(builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertificateVerification::new(&provider)))),
    }
}

fn add_roots(
    store: &mut RootCertStore,
    roots: Vec<CertificateDer<'static>>,
) -> Result<(), TlsConfigError> {
    for root in roots {
        store
            .add(root)
            .map_err(|error| TlsConfigError::InvalidRoot(error.to_string()))?;
    }
    Ok(())
}

#[derive(Debug)]
struct NoCertificateVerification {
    schemes: Vec<SignatureScheme>,
}

impl NoCertificateVerification {
    fn new(provider: &CryptoProvider) -> Self {
        Self {
            schemes: provider
                .signature_verification_algorithms
                .supported_schemes(),
        }
    }
}

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pkcs8::PrivateKeyInfoRef;
    use rcgen::{CertifiedKey, generate_simple_self_signed};

    #[test]
    fn secrets_are_redacted() {
        assert_eq!(
            format!("{:?}", SecretBytes::new(b"hunter2".to_vec())),
            "SecretBytes([REDACTED])"
        );
        let input = PrivateKeyInput::EncryptedPkcs8Der {
            der: SecretBytes::new(vec![1, 2, 3]),
            password: SecretBytes::new(b"hunter2".to_vec()),
        };
        let shown = format!("{input:?}");
        assert!(!shown.contains("hunter2"));
        assert!(!shown.contains("1, 2, 3"));
    }

    #[test]
    fn legacy_encrypted_pem_is_rejected_explicitly() {
        let input = PrivateKeyInput::PlainPem(SecretBytes::new(
            b"-----BEGIN RSA PRIVATE KEY-----\nProc-Type: 4,ENCRYPTED\nDEK-Info: AES-256-CBC,00\n-----END RSA PRIVATE KEY-----\n".to_vec(),
        ));
        assert!(matches!(
            decode_private_key(input),
            Err(TlsConfigError::LegacyEncryptedPrivateKey)
        ));
    }

    #[test]
    fn encrypted_pkcs8_der_accepts_only_the_right_password() {
        let CertifiedKey { signing_key, .. } =
            generate_simple_self_signed(["localhost".to_string()]).unwrap();
        let plain = signing_key.serialize_der();
        let key_info = PrivateKeyInfoRef::try_from(plain.as_slice()).unwrap();
        let encrypted = key_info.encrypt(b"correct horse").unwrap();

        let decoded = decode_private_key(PrivateKeyInput::EncryptedPkcs8Der {
            der: SecretBytes::new(encrypted.as_bytes().to_vec()),
            password: SecretBytes::new(b"correct horse".to_vec()),
        });
        assert!(decoded.is_ok());

        let error = decode_private_key(PrivateKeyInput::EncryptedPkcs8Der {
            der: SecretBytes::new(encrypted.as_bytes().to_vec()),
            password: SecretBytes::new(b"wrong".to_vec()),
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "私钥解密失败");
        assert!(!error.to_string().contains("wrong"));
    }
}
