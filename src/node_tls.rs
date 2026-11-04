// src/node_tls.rs
//
// TLS between the nodes of a cluster, and from a replica to its master
// (Redis's `tls-cluster` and `tls-replication`).
//
// With `tls-cluster yes` the cluster bus is TLS, and a node announces its
// TLS port as the port to reach it on; so everything one node opens to
// another's client port — replication, slot migration, MIGRATE, index
// definitions — is TLS as well. `tls-replication yes` does the same for a
// replica of a standalone master; `CONFIG SET` changes it, for the
// replica's next connection. `tls-cluster` is set at startup only.
//
// Both ends present `tls-cert-file` (or `tls-client-cert-file` when
// connecting, as in Redis) and check the other's against
// `tls-ca-cert-file`. Nodes are dialled by address, and certificates seldom
// name addresses, so as in Redis the chain is verified and the name is not.

#[cfg(feature = "tls")]
mod imp {
    use rustls_pemfile::{certs, private_key};
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;
    use tokio_rustls::rustls;
    use tokio_rustls::rustls::client::danger::{
        HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
    };
    use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use tokio_rustls::rustls::{
        CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
    };

    struct Settings {
        cluster: bool,
        replication: bool,
        client: Arc<ClientConfig>,
        bus: tokio_rustls::TlsAcceptor,
        files: crate::tls::acceptor::Files,
    }

    /// Set at startup; replaced by `CONFIG SET` of a TLS file (#156).
    static SETTINGS: parking_lot::RwLock<Option<Arc<Settings>>> = parking_lot::const_rwlock(None);

    /// `tls-replication` as it was set, which `CONFIG GET` answers with:
    /// `tls-cluster` implies it without setting it.
    static REPLICATION_SET: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);

    fn settings() -> Option<Arc<Settings>> {
        SETTINGS.read().clone()
    }

    /// Checks the chain against the CA, and not the name: see the module.
    #[derive(Debug)]
    struct ChainOnly(Arc<dyn ServerCertVerifier>);

    impl ServerCertVerifier for ChainOnly {
        fn verify_server_cert(
            &self,
            end_entity: &CertificateDer<'_>,
            intermediates: &[CertificateDer<'_>],
            server_name: &ServerName<'_>,
            ocsp: &[u8],
            now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            match self
                .0
                .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
            {
                Err(rustls::Error::InvalidCertificate(
                    CertificateError::NotValidForName
                    | CertificateError::NotValidForNameContext { .. },
                )) => Ok(ServerCertVerified::assertion()),
                other => other,
            }
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            self.0.verify_tls12_signature(message, cert, dss)
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            self.0.verify_tls13_signature(message, cert, dss)
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.supported_verify_schemes()
        }
    }

    fn read_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, String> {
        let file = File::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
        certs(&mut BufReader::new(file))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("cannot parse '{path}': {e}"))
    }

    /// Set up TLS between nodes, if the configuration asks for it.
    pub fn init(config: &crate::config::Config) -> Result<(), String> {
        if !config.tls_cluster && !config.tls_replication {
            return Ok(());
        }
        if config.tls_replication {
            REPLICATION_SET.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let built = build(
            crate::tls::acceptor::Files::of(config),
            config.tls_cluster,
            config.tls_replication || config.tls_cluster,
        )?;
        // The first server of the process sets it up.
        SETTINGS.write().get_or_insert(Arc::new(built));
        Ok(())
    }

    /// The files TLS between nodes is set up from now, if it is on.
    pub fn files() -> Option<crate::tls::acceptor::Files> {
        settings().map(|s| s.files.clone())
    }

    /// Set TLS between nodes up again from `files`: connections from now on
    /// use them (#156). Nothing changes if they cannot be loaded.
    pub fn reload(files: crate::tls::acceptor::Files) -> Result<(), String> {
        let Some(now) = settings() else {
            return Ok(());
        };
        let built = build(files, now.cluster, now.replication)?;
        *SETTINGS.write() = Some(Arc::new(built));
        Ok(())
    }

    /// `CONFIG SET tls-replication`: whether a replica connects to its
    /// master over TLS, set up from `files`, from its next connection on;
    /// a link already up stays as it is. Nothing changes if they cannot be
    /// loaded. `tls-cluster` implies it, and is not changed here.
    pub fn set_replication(on: bool, files: crate::tls::acceptor::Files) -> Result<(), String> {
        let cluster = cluster();
        if !on && !cluster {
            *SETTINGS.write() = None;
        } else {
            let built = build(files, cluster, on || cluster)?;
            *SETTINGS.write() = Some(Arc::new(built));
        }
        REPLICATION_SET.store(on, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// `tls-replication` as it was set.
    pub fn replication_set() -> bool {
        REPLICATION_SET.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn build(
        files: crate::tls::acceptor::Files,
        cluster: bool,
        replication: bool,
    ) -> Result<Settings, String> {
        let mut roots = RootCertStore::empty();
        for cert in read_certs(&files.ca)? {
            roots
                .add(cert)
                .map_err(|e| format!("invalid CA certificate: {e}"))?;
        }
        let (cert_path, key_path) = if files.client_cert.is_empty() {
            (&files.cert, &files.key)
        } else {
            (&files.client_cert, &files.client_key)
        };
        let chain = read_certs(cert_path)?;
        let key_file =
            File::open(key_path).map_err(|e| format!("cannot open '{key_path}': {e}"))?;
        let key = private_key(&mut BufReader::new(key_file))
            .map_err(|e| format!("cannot parse '{key_path}': {e}"))?
            .ok_or_else(|| format!("no private key in '{key_path}'"))?;
        let webpki = rustls::client::WebPkiServerVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| format!("certificate verifier: {e}"))?;
        let client = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(ChainOnly(webpki)))
            .with_client_auth_cert(chain, key)
            .map_err(|e| format!("client certificate: {e}"))?;
        // The bus asks every node for its certificate.
        let bus = crate::tls::acceptor::build(&crate::tls::acceptor::Files {
            auth_clients: "yes".to_string(),
            ..files.clone()
        })?;
        Ok(Settings {
            cluster,
            replication,
            client: Arc::new(client),
            bus,
            files,
        })
    }

    pub fn cluster() -> bool {
        settings().is_some_and(|s| s.cluster)
    }

    pub fn replication() -> bool {
        settings().is_some_and(|s| s.replication)
    }

    fn name_for(host: &str) -> ServerName<'static> {
        ServerName::try_from(host.to_string())
            .unwrap_or_else(|_| ServerName::try_from("kividb-node".to_string()).unwrap())
    }

    pub async fn connect_async(
        host: &str,
        tcp: tokio::net::TcpStream,
    ) -> std::io::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
        let settings =
            settings().ok_or_else(|| std::io::Error::other("TLS between nodes is not set up"))?;
        tokio_rustls::TlsConnector::from(Arc::clone(&settings.client))
            .connect(name_for(host), tcp)
            .await
    }

    pub async fn accept_bus(
        tcp: tokio::net::TcpStream,
    ) -> std::io::Result<tokio_rustls::server::TlsStream<tokio::net::TcpStream>> {
        let settings =
            settings().ok_or_else(|| std::io::Error::other("TLS between nodes is not set up"))?;
        settings.bus.accept(tcp).await
    }

    pub type SyncTls = rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>;

    pub fn connect_sync(host: &str, tcp: std::net::TcpStream) -> std::io::Result<SyncTls> {
        let settings =
            settings().ok_or_else(|| std::io::Error::other("TLS between nodes is not set up"))?;
        let conn = rustls::ClientConnection::new(Arc::clone(&settings.client), name_for(host))
            .map_err(std::io::Error::other)?;
        let mut stream = rustls::StreamOwned::new(conn, tcp);
        // Handshake now, so that a refusal is an error here and not on the
        // first command.
        while stream.conn.is_handshaking() {
            stream.conn.complete_io(&mut stream.sock)?;
        }
        Ok(stream)
    }
}

#[cfg(feature = "tls")]
pub use imp::{
    accept_bus, cluster, connect_async, connect_sync, files, init, reload, replication,
    replication_set, set_replication,
};

#[cfg(not(feature = "tls"))]
pub fn init(config: &crate::config::Config) -> Result<(), String> {
    if config.tls_cluster || config.tls_replication {
        return Err(
            "tls-cluster and tls-replication need a build with TLS (the `tls` feature)".into(),
        );
    }
    Ok(())
}

#[cfg(not(feature = "tls"))]
pub fn cluster() -> bool {
    false
}

#[cfg(not(feature = "tls"))]
pub fn replication() -> bool {
    false
}

// Never reached without the `tls` feature: `cluster()` and `replication()`
// are false there.
#[cfg(not(feature = "tls"))]
pub async fn connect_async(
    _host: &str,
    tcp: tokio::net::TcpStream,
) -> std::io::Result<tokio::net::TcpStream> {
    Ok(tcp)
}

#[cfg(not(feature = "tls"))]
pub async fn accept_bus(tcp: tokio::net::TcpStream) -> std::io::Result<tokio::net::TcpStream> {
    Ok(tcp)
}

/// A blocking connection to another node's client port: TLS when
/// `tls-cluster` says so. Returns the stream to talk on, and the socket
/// itself for its timeouts.
pub fn connect_node(
    host: &str,
    addr: &std::net::SocketAddr,
    timeout: std::time::Duration,
) -> std::io::Result<(Box<dyn ReadWrite>, std::net::TcpStream)> {
    let tcp = std::net::TcpStream::connect_timeout(addr, timeout)?;
    let _ = tcp.set_nodelay(true);
    let _ = tcp.set_read_timeout(Some(timeout));
    let _ = tcp.set_write_timeout(Some(timeout));
    let handle = tcp.try_clone()?;
    #[cfg(feature = "tls")]
    if cluster() {
        return Ok((Box::new(connect_sync(host, tcp)?), handle));
    }
    let _ = host;
    Ok((Box::new(tcp), handle))
}

pub trait ReadWrite: std::io::Read + std::io::Write + Send {}
impl<T: std::io::Read + std::io::Write + Send> ReadWrite for T {}

/// An async stream to another node: the bus, or a replica's link.
pub trait AsyncStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> AsyncStream for T {}
