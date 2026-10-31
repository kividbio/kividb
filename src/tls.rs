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

    /// Build a `TlsAcceptor` from PEM cert + key files.
    ///
    /// When `ca_path` is set, client certificates are verified against it as
    /// `auth_clients` (`tls-auth-clients`) says: `yes` requires one, `optional`
    /// accepts a client without one but refuses an invalid one, `no` does not
    /// ask. With no CA file clients are not asked for a certificate.
    ///
    /// Returns an error string for the log, before the process exits.
    pub fn build(
        cert_path: &str,
        key_path: &str,
        ca_path: &str,
        auth_clients: &str,
    ) -> Result<TlsAcceptor, String> {
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

        let builder = ServerConfig::builder();
        let builder = match client_verifier(ca_path, auth_clients)? {
            Some(verifier) => builder.with_client_cert_verifier(verifier),
            None => builder.with_no_client_auth(),
        };
        let config = builder
            .with_single_cert(certs, key)
            .map_err(|e| format!("[TLS] TLS config error: {e}"))?;

        Ok(TlsAcceptor::from(Arc::new(config)))
    }

    /// How long a client has to finish its handshake once connected.
    pub const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    /// Complete the handshake of a client that connected from `peer`, or
    /// log why it did not and give up on it. The client keeps its `slot`
    /// once connected; it is given back if the handshake fails.
    pub async fn handshake(
        acceptor: &TlsAcceptor,
        tcp: tokio::net::TcpStream,
        peer: std::net::SocketAddr,
        slot: crate::client_registry::Slot,
    ) -> Option<(
        tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
        crate::client_registry::Slot,
    )> {
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
            Ok(Ok(tls)) => Some((tls, slot)),
            Ok(Err(e)) => {
                // Full `Debug` — rustls errors often have empty `Display`.
                crate::log_verbose!("TLS: handshake failed from {peer}: {e} ({e:?})");
                None
            }
            Err(_) => {
                crate::log_verbose!("TLS: handshake timed out from {peer}");
                None
            }
        }
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

    #[cfg(test)]
    mod tests {
        use super::*;
        use tokio::io::AsyncWriteExt;
        use tokio::net::{TcpListener, TcpStream};
        use tokio_rustls::rustls::pki_types::{PrivateKeyDer, ServerName};

        async fn connect() -> (TcpStream, TcpStream, std::net::SocketAddr) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (server, peer) = listener.accept().await.unwrap();
            (client, server, peer)
        }

        /// A client keeps its slot once connected; a handshake that fails,
        /// runs out of time or is cancelled gives it back.
        #[tokio::test]
        async fn a_handshake_keeps_its_slot_only_if_it_connects() {
            let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let cert = certified.cert.der().clone();
            let key = PrivateKeyDer::Pkcs8(certified.key_pair.serialize_der().into());
            let config = ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![cert.clone()], key)
                .unwrap();
            let acceptor = TlsAcceptor::from(Arc::new(config));
            let registry = crate::client_registry::new_client_registry();
            let slot = || crate::client_registry::admit(&registry).unwrap();

            let mut roots = RootCertStore::empty();
            roots.add(cert).unwrap();
            let client = tokio_rustls::TlsConnector::from(Arc::new(
                tokio_rustls::rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            ));
            let (tcp, server, peer) = connect().await;
            let (accepted, connected) = tokio::join!(
                handshake(&acceptor, server, peer, slot()),
                client.connect(ServerName::try_from("localhost").unwrap(), tcp),
            );
            let (tls, kept) = accepted.unwrap();
            assert!(connected.is_ok());
            assert_eq!(registry.slots_held(), 1);
            drop((tls, kept));
            assert_eq!(registry.slots_held(), 0);

            let (mut tcp, server, peer) = connect().await;
            tcp.write_all(b"PING\r\n").await.unwrap();
            assert!(handshake(&acceptor, server, peer, slot()).await.is_none());
            assert_eq!(registry.slots_held(), 0);

            let (_tcp, server, peer) = connect().await;
            let task = {
                let (acceptor, slot) = (acceptor.clone(), slot());
                tokio::spawn(async move { handshake(&acceptor, server, peer, slot).await })
            };
            assert_eq!(registry.slots_held(), 1);
            task.abort();
            assert!(task.await.is_err_and(|e| e.is_cancelled()));
            assert_eq!(registry.slots_held(), 0);

            let (_tcp, server, peer) = connect().await;
            tokio::time::pause();
            assert!(handshake(&acceptor, server, peer, slot()).await.is_none());
            assert_eq!(registry.slots_held(), 0);
        }
    }
}
