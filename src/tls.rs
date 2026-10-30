//! TLS acceptor construction.
//!
//! Gated behind the `tls` feature — when built without it this module is
//! empty and the compiler eliminates all TLS-related branches.

#[cfg(feature = "tls")]
pub mod acceptor {
    use rustls_pemfile::{certs, private_key};
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::rustls::server::WebPkiClientVerifier;
    use tokio_rustls::rustls::server::danger::ClientCertVerifier;
    use tokio_rustls::rustls::{RootCertStore, ServerConfig};

    /// The files a server's TLS is set up from. `CONFIG SET` changes them
    /// while it runs (#156).
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct Files {
        pub cert: String,
        pub key: String,
        pub ca: String,
        pub auth_clients: String,
        pub client_cert: String,
        pub client_key: String,
        /// `tls-auth-clients-allowed-names`, as `config::allowed_names`
        /// writes it: lower case, separated by spaces.
        pub allowed_names: String,
    }

    /// The settings that make up [`Files`].
    pub const SETTINGS: [&str; 7] = [
        "tls-cert-file",
        "tls-key-file",
        "tls-ca-cert-file",
        "tls-auth-clients",
        "tls-auth-clients-allowed-names",
        "tls-client-cert-file",
        "tls-client-key-file",
    ];

    impl Files {
        pub fn of(config: &crate::config::Config) -> Self {
            let names = &config.tls_auth_clients_allowed_names;
            Files {
                cert: config.tls_cert_file.clone(),
                key: config.tls_key_file.clone(),
                ca: config.tls_ca_cert_file.clone(),
                auth_clients: config.tls_auth_clients.clone(),
                client_cert: config.tls_client_cert_file.clone(),
                client_key: config.tls_client_key_file.clone(),
                // Checked at startup: see `Config::validate`.
                allowed_names: crate::config::allowed_names(names)
                    .map_or_else(|_| names.clone(), |names| names.join(" ")),
            }
        }

        fn slot(&mut self, name: &str) -> Option<&mut String> {
            Some(match name {
                "tls-cert-file" => &mut self.cert,
                "tls-key-file" => &mut self.key,
                "tls-ca-cert-file" => &mut self.ca,
                "tls-auth-clients" => &mut self.auth_clients,
                "tls-auth-clients-allowed-names" => &mut self.allowed_names,
                "tls-client-cert-file" => &mut self.client_cert,
                "tls-client-key-file" => &mut self.client_key,
                _ => return None,
            })
        }

        /// One of [`SETTINGS`] by its name.
        pub fn get(&self, name: &str) -> Option<String> {
            self.clone().slot(name).map(|value| value.clone())
        }

        /// Set one of [`SETTINGS`] by its name.
        pub fn set(&mut self, name: &str, value: &str) {
            if let Some(slot) = self.slot(name) {
                *slot = value.to_string();
            }
        }
    }

    /// The TLS acceptor of each server of this process, with the files it
    /// is set up from. A handshake takes the one there now, so that a new
    /// one serves every connection from then on; a test process runs
    /// several servers, hence one each. A server without TLS has the files
    /// and no acceptor, until `CONFIG SET tls-port` opens its TLS port.
    #[allow(clippy::type_complexity)]
    static LISTENERS: std::sync::LazyLock<
        parking_lot::Mutex<
            std::collections::HashMap<crate::listen::Server, (Files, Option<TlsAcceptor>)>,
        >,
    > = std::sync::LazyLock::new(Default::default);

    /// Build the acceptor of `server` from `files`, and serve every
    /// handshake from now on with it. Nothing changes if it cannot be built.
    pub fn install(server: crate::listen::Server, files: &Files) -> Result<TlsAcceptor, String> {
        let acceptor = build(files)?;
        LISTENERS
            .lock()
            .insert(server, (files.clone(), Some(acceptor.clone())));
        Ok(acceptor)
    }

    /// Keep `files` as what `server`'s TLS is set up from, without loading
    /// them: while its TLS port is closed a certificate may be set before
    /// its key. Opening the port loads them.
    pub fn keep(server: crate::listen::Server, files: &Files) {
        LISTENERS
            .lock()
            .entry(server)
            .and_modify(|(kept, _)| *kept = files.clone())
            .or_insert_with(|| (files.clone(), None));
    }

    /// The acceptor `server` serves TLS handshakes with now.
    pub fn current(server: crate::listen::Server) -> Option<TlsAcceptor> {
        LISTENERS
            .lock()
            .get(&server)
            .and_then(|(_, acceptor)| acceptor.clone())
    }

    /// The files `server`'s TLS is set up from now.
    pub fn files(server: crate::listen::Server) -> Option<Files> {
        LISTENERS
            .lock()
            .get(&server)
            .map(|(files, _)| files.clone())
    }

    /// The listener of the TLS port of the server `config` starts, with its
    /// acceptor: listening when TLS is set up, and on no port otherwise,
    /// until `CONFIG SET tls-port` opens one. Call it in a Tokio runtime.
    pub fn listener(config: &crate::config::Config) -> Result<crate::listen::Listener, String> {
        use crate::listen::{Kind, Listener};
        let server = crate::listen::server_of(config);
        let files = Files::of(config);
        let addr: std::net::SocketAddr = format!("{}:{}", config.bind, config.tls_port)
            .parse()
            .map_err(|_| format!("invalid TLS bind address '{}'", config.bind))?;
        if !config.tls_enabled() {
            keep(server, &files);
            return Ok(Listener::new(None, addr, server, Kind::Tls));
        }
        install(server, &files)?;
        let socket = std::net::TcpListener::bind(addr)
            .and_then(|socket| {
                socket.set_nonblocking(true)?;
                tokio::net::TcpListener::from_std(socket)
            })
            .map_err(|e| format!("can't listen on {addr}: {e}"))?;
        crate::log_info!("TLS listener   : {addr}");
        Ok(Listener::new(Some(socket), addr, server, Kind::Tls))
    }

    /// Build a `TlsAcceptor` from the PEM cert and key files of `files`.
    ///
    /// When `files.ca` is set, client certificates are verified against it
    /// as `files.auth_clients` (`tls-auth-clients`) says: `yes` requires
    /// one, `optional` accepts a client without one but refuses an invalid
    /// one, `no` does not ask. With no CA file clients are not asked for a
    /// certificate.
    ///
    /// Returns an error string for the log, before the process exits.
    pub fn build(files: &Files) -> Result<TlsAcceptor, String> {
        let (cert_path, key_path, ca_path, auth_clients) =
            (&files.cert, &files.key, &files.ca, &files.auth_clients);
        let cert_file = File::open(cert_path)
            .map_err(|e| format!("[TLS] Cannot open cert file '{cert_path}': {e}"))?;
        let key_file = File::open(key_path)
            .map_err(|e| format!("[TLS] Cannot open key file '{key_path}': {e}"))?;

        let certs = certs(&mut BufReader::new(cert_file))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("[TLS] Failed to parse cert: {e}"))?;

        let key = private_key(&mut BufReader::new(key_file))
            .map_err(|e| format!("[TLS] Failed to parse key: {e}"))?
            .ok_or_else(|| format!("[TLS] No private key found in '{key_path}'"))?;

        let names = crate::config::allowed_names(&files.allowed_names)
            .map_err(|e| format!("[TLS] tls-auth-clients-allowed-names: {e}"))?;
        let builder = ServerConfig::builder();
        let builder = match client_verifier(ca_path, auth_clients)? {
            Some(verifier) if names.is_empty() => builder.with_client_cert_verifier(verifier),
            Some(verifier) => {
                builder.with_client_cert_verifier(Arc::new(AllowedNames { verifier, names }))
            }
            None => builder.with_no_client_auth(),
        };
        let config = builder
            .with_single_cert(certs, key)
            .map_err(|e| format!("[TLS] TLS config error: {e}"))?;

        Ok(TlsAcceptor::from(Arc::new(config)))
    }

    /// The verifier for client certificates, or `None` when clients are not
    /// asked for one.
    fn client_verifier(
        ca_path: &str,
        auth_clients: &str,
    ) -> Result<Option<Arc<dyn ClientCertVerifier>>, String> {
        if ca_path.is_empty() || auth_clients == "no" {
            return Ok(None);
        }

        let ca_file = File::open(ca_path)
            .map_err(|e| format!("[TLS] Cannot open CA cert file '{ca_path}': {e}"))?;
        let mut roots = RootCertStore::empty();
        for cert in certs(&mut BufReader::new(ca_file)) {
            let cert = cert.map_err(|e| format!("[TLS] Failed to parse CA cert: {e}"))?;
            roots
                .add(cert)
                .map_err(|e| format!("[TLS] Invalid CA cert in '{ca_path}': {e}"))?;
        }
        if roots.is_empty() {
            return Err(format!("[TLS] No CA certificate found in '{ca_path}'"));
        }

        let builder = WebPkiClientVerifier::builder(Arc::new(roots));
        let builder = if auth_clients == "optional" {
            builder.allow_unauthenticated()
        } else {
            builder
        };
        let verifier = builder
            .build()
            .map_err(|e| format!("[TLS] Client certificate verifier error: {e}"))?;
        Ok(Some(verifier))
    }

    use tokio_rustls::rustls;
    use tokio_rustls::rustls::client::danger::HandshakeSignatureValid;
    use tokio_rustls::rustls::pki_types::{CertificateDer, UnixTime};
    use tokio_rustls::rustls::server::danger::ClientCertVerified;
    use tokio_rustls::rustls::{DigitallySignedStruct, DistinguishedName, SignatureScheme};

    /// `tls-auth-clients-allowed-names`: a certificate the CA's verifier
    /// takes is taken only if its subject CN or one of its DNS names matches
    /// one of `names`. One that does not fails the handshake, as a
    /// certificate from another CA does.
    #[derive(Debug)]
    struct AllowedNames {
        verifier: Arc<dyn ClientCertVerifier>,
        names: Vec<String>,
    }

    impl ClientCertVerifier for AllowedNames {
        fn offer_client_auth(&self) -> bool {
            self.verifier.offer_client_auth()
        }

        fn client_auth_mandatory(&self) -> bool {
            self.verifier.client_auth_mandatory()
        }

        fn root_hint_subjects(&self) -> &[DistinguishedName] {
            self.verifier.root_hint_subjects()
        }

        fn verify_client_cert(
            &self,
            end_entity: &CertificateDer<'_>,
            intermediates: &[CertificateDer<'_>],
            now: UnixTime,
        ) -> Result<ClientCertVerified, rustls::Error> {
            let verified = self
                .verifier
                .verify_client_cert(end_entity, intermediates, now)?;
            let presented = presented_names(end_entity);
            if presented.iter().any(|name| {
                self.names
                    .iter()
                    .any(|pattern| crate::config::allowed_name_matches(pattern, name))
            }) {
                return Ok(verified);
            }
            // The names, not the certificate; a few at most, and short.
            let shown: Vec<String> = presented
                .iter()
                .take(8)
                .map(|name| name.chars().take(100).collect())
                .collect();
            crate::log_throttled!(
                10,
                crate::loglevel::WARN,
                "TLS: client certificate refused: its names ({}) match none of \
                 tls-auth-clients-allowed-names",
                if shown.is_empty() {
                    "none".to_string()
                } else {
                    shown.join(", ")
                }
            );
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ))
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            self.verifier.verify_tls12_signature(message, cert, dss)
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            self.verifier.verify_tls13_signature(message, cert, dss)
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.verifier.supported_verify_schemes()
        }
    }

    /// The subject CNs of a certificate, then its DNS names.
    fn presented_names(cert: &CertificateDer<'_>) -> Vec<String> {
        let Ok(parsed) = webpki::EndEntityCert::try_from(cert) else {
            return Vec::new();
        };
        let mut names = common_names(parsed.subject());
        names.extend(parsed.valid_dns_names().map(str::to_string));
        names
    }

    /// One DER element at the start of `input`: its tag, its contents, and
    /// what follows it.
    fn der(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
        let (&tag, rest) = input.split_first()?;
        let (&first, rest) = rest.split_first()?;
        let (len, rest) = if first < 0x80 {
            (first as usize, rest)
        } else {
            let n = (first & 0x7f) as usize;
            if n == 0 || n > 4 || rest.len() < n {
                return None;
            }
            let len = rest[..n]
                .iter()
                .fold(0usize, |len, &b| (len << 8) | b as usize);
            (len, &rest[n..])
        };
        (rest.len() >= len).then(|| (tag, &rest[..len], &rest[len..]))
    }

    /// The common names (OID 2.5.4.3) in a certificate's subject: the DER
    /// of a Name without its outer SEQUENCE, a SET of attributes for each
    /// part.
    fn common_names(subject: &[u8]) -> Vec<String> {
        const SET: u8 = 0x31;
        const SEQUENCE: u8 = 0x30;
        const OID: u8 = 0x06;
        const CN: &[u8] = &[0x55, 0x04, 0x03];
        let mut names = Vec::new();
        let mut parts = subject;
        while let Some((SET, mut attributes, rest)) = der(parts) {
            parts = rest;
            while let Some((SEQUENCE, attribute, more)) = der(attributes) {
                attributes = more;
                if let Some((OID, oid, value)) = der(attribute)
                    && oid == CN
                    // UTF8String, PrintableString, TeletexString, IA5String.
                    && let Some((0x0c | 0x13 | 0x14 | 0x16, text, _)) = der(value)
                    && let Ok(text) = std::str::from_utf8(text)
                {
                    names.push(text.to_string());
                }
            }
        }
        names
    }
}
