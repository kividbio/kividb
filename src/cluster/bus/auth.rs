// src/cluster/bus/auth.rs
//
// The cluster bus's shared secret (`cluster-bus-secret`).
//
// On a bus with a secret, every connection starts with a challenge. The node
// that accepts it sends a random nonce:
//
//   BUSNONCE <32 hex digits>
//
// and the node that connected must answer, before anything it sends is read,
// with
//
//   BUSAUTH <hex HMAC-SHA256(secret, "kividb-cluster-bus-1:" + nonce)>
//
// A wrong or missing answer closes the connection. The secret never crosses
// the wire, and an answer seen on one connection is no good on another. What
// follows is not encrypted: this keeps strangers out of the cluster, it does
// not hide the traffic from someone on the path (that would be TLS).
//
// Every node of a cluster has the same secret or none. A node with one and a
// node without cannot talk: the one without never answers the challenge, or
// is never sent one.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const NONCE: &str = "BUSNONCE";
const AUTH: &str = "BUSAUTH";
/// What the HMAC is taken of, ahead of the nonce: a proof for this purpose
/// is not one for any other use of the same secret.
const CONTEXT: &[u8] = b"kividb-cluster-bus-1:";
/// How long a connecting node has to answer.
const ANSWER_WITHIN: Duration = Duration::from_secs(5);

fn hmac_sha256(key: &[u8], message: &[&[u8]]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut block = [0u8; 64];
    if key.len() > block.len() {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(block.map(|b| b ^ 0x36));
    for part in message {
        inner.update(part);
    }
    let mut outer = Sha256::new();
    outer.update(block.map(|b| b ^ 0x5c));
    outer.update(inner.finalize());
    outer.finalize().into()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The answer to `nonce` that proves knowledge of `secret`.
fn proof(secret: &[u8], nonce: &[u8]) -> String {
    hex(&hmac_sha256(secret, &[CONTEXT, nonce]))
}

/// Equal, in a time that does not depend on where they first differ.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn frame(kind: &str, value: &str) -> Vec<u8> {
    format!(
        "*2\r\n${}\r\n{kind}\r\n${}\r\n{value}\r\n",
        kind.len(),
        value.len()
    )
    .into_bytes()
}

/// Read until `buf` holds one whole frame of `kind`; return its value and
/// whatever came after it.
async fn read_frame(
    sock: &mut (impl tokio::io::AsyncRead + Unpin),
    buf: &mut Vec<u8>,
    kind: &str,
    within: Duration,
) -> Result<(Vec<u8>, Vec<u8>), String> {
    let deadline = tokio::time::Instant::now() + within;
    let mut tmp = [0u8; 1024];
    loop {
        match super::message::parse_resp_array(buf) {
            Ok(Some((parts, consumed))) => {
                return match parts.as_slice() {
                    [k, value] if k.as_slice() == kind.as_bytes() => {
                        Ok((value.clone(), buf[consumed..].to_vec()))
                    }
                    _ => Err(format!("expected {kind} first")),
                };
            }
            Ok(None) if buf.len() > 4096 => return Err(format!("oversized {kind}")),
            Ok(None) => {}
            Err(e) => return Err(format!("expected {kind} first: {e}")),
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, sock.read(&mut tmp)).await {
            Err(_) => return Err(format!("no {kind} within {within:?}")),
            Ok(Ok(0)) => return Err(format!("closed before {kind}")),
            Ok(Ok(n)) => buf.extend_from_slice(&tmp[..n]),
            Ok(Err(e)) => return Err(e.to_string()),
        }
    }
}

/// The accepting side: challenge the node that connected. `Ok(rest)` once
/// it has proved it knows the secret, with whatever it sent after its
/// answer; `Err` if it has not, and the connection is to be closed.
pub async fn challenge(
    sock: &mut (impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin),
    secret: &[u8],
) -> Result<Vec<u8>, String> {
    let nonce = hex(&rand::random::<[u8; 16]>());
    sock.write_all(&frame(NONCE, &nonce))
        .await
        .map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    let (answer, rest) = read_frame(sock, &mut buf, AUTH, ANSWER_WITHIN).await?;
    if !same(&answer, proof(secret, nonce.as_bytes()).as_bytes()) {
        return Err("wrong cluster-bus-secret".into());
    }
    Ok(rest)
}

/// The connecting side: answer the challenge of the node connected to.
pub async fn answer(
    sock: &mut (impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin),
    secret: &[u8],
    within: Duration,
) -> Result<(), String> {
    let mut buf = Vec::new();
    let (nonce, _) = read_frame(sock, &mut buf, NONCE, within)
        .await
        .map_err(|e| format!("{e} (is cluster-bus-secret set on that node?)"))?;
    sock.write_all(&frame(AUTH, &proof(secret, &nonce)))
        .await
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpStream;

    /// RFC 4231, test case 2.
    #[test]
    fn hmac_is_hmac_sha256() {
        assert_eq!(
            hex(&hmac_sha256(
                b"Jefe",
                &[b"what do ya want ", b"for nothing?"]
            )),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        // A key longer than the block is hashed first (RFC 4231, case 6).
        assert_eq!(
            hex(&hmac_sha256(
                &[0xaa; 131],
                &[b"Test Using Larger Than Block-Size Key - Hash Key First"]
            )),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn a_proof_depends_on_the_secret_and_the_nonce() {
        let p = proof(b"s3cret", b"00ff");
        assert!(same(p.as_bytes(), proof(b"s3cret", b"00ff").as_bytes()));
        assert!(!same(p.as_bytes(), proof(b"s3cret", b"00fe").as_bytes()));
        assert!(!same(p.as_bytes(), proof(b"s3cres", b"00ff").as_bytes()));
    }

    #[tokio::test]
    async fn only_a_node_with_the_secret_gets_through() {
        async fn attempt(server_secret: &'static [u8], client_secret: &'static [u8]) -> bool {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut sock, _) = listener.accept().await.unwrap();
                challenge(&mut sock, server_secret).await
            });
            let mut sock = TcpStream::connect(addr).await.unwrap();
            let answered = answer(&mut sock, client_secret, Duration::from_secs(2)).await;
            // Something sent straight after the answer is handed on.
            sock.write_all(b"next").await.unwrap();
            let accepted = server.await.unwrap();
            answered.is_ok() && accepted.is_ok_and(|rest| rest.is_empty() || rest == b"next")
        }
        assert!(attempt(b"one", b"one").await);
        assert!(!attempt(b"one", b"two").await);
    }
}
