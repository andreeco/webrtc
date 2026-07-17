//! Shared RFC 4571 ICE/TCP listener multiplexing.
//!
//! A [`TcpMux`] owns one passive TCP listener and dispatches each newly accepted
//! ICE/TCP connection to the peer connection named by the local username
//! fragment in its first STUN binding request.

use super::driver::PeerConnectionDriverEvent;
use crate::runtime::{AsyncTcpStream, Runtime, Sender};
use bytes::BytesMut;
use log::trace;
use rtc::shared::{FourTuple, TaggedBytesMut, TransportContext, TransportProtocol};
use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const RFC4571_HEADER_LEN: usize = 2;
const STUN_HEADER_LEN: usize = 20;
const STUN_MAGIC_COOKIE: [u8; 4] = [0x21, 0x12, 0xA4, 0x42];
const STUN_ATTR_USERNAME: u16 = 0x0006;

/// A passive TCP listener that can be shared by multiple peer connections.
///
/// Each peer connection configured with
/// [`PeerConnectionBuilder::with_tcp_mux`](super::PeerConnectionBuilder::with_tcp_mux)
/// publishes the mux's address as its passive ICE/TCP candidate. The mux reads
/// the first RFC 4571 frame, extracts the destination ICE ufrag from its STUN
/// `USERNAME` attribute, and transfers the stream to the matching connection.
/// Dropping or closing a peer connection unregisters only that connection; it
/// does not close the mux or affect other registered peer connections.
#[derive(Clone)]
pub struct TcpMux {
    inner: Arc<TcpMuxInner>,
}

struct TcpMuxInner {
    local_addr: SocketAddr,
    routes: Mutex<HashMap<String, Sender<PeerConnectionDriverEvent>>>,
}

impl std::fmt::Debug for TcpMux {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TcpMux")
            .field("local_addr", &self.inner.local_addr)
            .finish_non_exhaustive()
    }
}

impl TcpMux {
    /// Binds a shared passive TCP listener and starts its accept loop on `runtime`.
    pub fn bind<A: ToSocketAddrs>(runtime: Arc<dyn Runtime>, addr: A) -> io::Result<Self> {
        let listener = std::net::TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;
        let listener = runtime.wrap_tcp_listener(listener)?;
        let inner = Arc::new(TcpMuxInner {
            local_addr,
            routes: Mutex::new(HashMap::new()),
        });
        let accept_inner = inner.clone();
        let accept_runtime = runtime.clone();

        runtime.spawn(Box::pin(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer_addr)) => {
                        let inner = accept_inner.clone();
                        accept_runtime.spawn(Box::pin(async move {
                            dispatch_initial_frame(inner, stream, peer_addr).await;
                        }));
                    }
                    Err(error) => {
                        log::error!("shared ICE/TCP listener accept error: {error}");
                        break;
                    }
                }
            }
        }));

        Ok(Self { inner })
    }

    /// Returns the local address published in passive ICE/TCP candidates.
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    pub(crate) fn register(
        &self,
        ufrag: String,
        sender: Sender<PeerConnectionDriverEvent>,
    ) -> rtc::shared::error::Result<TcpMuxRegistration> {
        let mut routes = self.inner.routes.lock().map_err(|_| {
            rtc::shared::error::Error::Other("TCP mux route registry poisoned".into())
        })?;
        if routes.contains_key(&ufrag) {
            return Err(rtc::shared::error::Error::Other(format!(
                "TCP mux already has an ICE ufrag registered: {ufrag}"
            )));
        }
        routes.insert(ufrag.clone(), sender);
        Ok(TcpMuxRegistration {
            inner: Arc::downgrade(&self.inner),
            ufrag,
        })
    }
}

pub(crate) struct TcpMuxRegistration {
    inner: std::sync::Weak<TcpMuxInner>,
    ufrag: String,
}

impl TcpMuxRegistration {
    pub(crate) fn ufrag(&self) -> &str {
        &self.ufrag
    }
}

impl Drop for TcpMuxRegistration {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade()
            && let Ok(mut routes) = inner.routes.lock()
        {
            routes.remove(&self.ufrag);
        }
    }
}

async fn dispatch_initial_frame(
    inner: Arc<TcpMuxInner>,
    stream: Arc<dyn AsyncTcpStream>,
    peer_addr: SocketAddr,
) {
    let Some(packet) = read_rfc4571_frame(stream.clone()).await else {
        return;
    };
    let Some(ufrag) = destination_ufrag(&packet) else {
        trace!("dropping shared ICE/TCP connection without an initial STUN USERNAME");
        return;
    };
    let sender = match inner.routes.lock() {
        Ok(routes) => routes.get(ufrag).cloned(),
        Err(_) => None,
    };
    let Some(sender) = sender else {
        trace!("dropping shared ICE/TCP connection for unknown ufrag {ufrag}");
        return;
    };

    let local_addr = stream.local_addr().unwrap_or(inner.local_addr);
    let four_tuple = FourTuple {
        local_addr,
        peer_addr,
    };
    let initial_packet = TaggedBytesMut {
        now: Instant::now(),
        transport: TransportContext {
            local_addr,
            peer_addr,
            ecn: None,
            transport_protocol: TransportProtocol::TCP,
        },
        message: BytesMut::from(packet.as_slice()),
    };
    let _ = sender
        .send(PeerConnectionDriverEvent::IncomingTcpStream {
            four_tuple,
            stream,
            initial_packet: Some(initial_packet),
        })
        .await;
}

async fn read_rfc4571_frame(stream: Arc<dyn AsyncTcpStream>) -> Option<Vec<u8>> {
    let mut header = [0; RFC4571_HEADER_LEN];
    read_exact(&stream, &mut header).await?;
    let length = u16::from_be_bytes(header) as usize;
    if length == 0 {
        return None;
    }
    let mut packet = vec![0; length];
    read_exact(&stream, &mut packet).await?;
    Some(packet)
}

async fn read_exact(stream: &Arc<dyn AsyncTcpStream>, buffer: &mut [u8]) -> Option<()> {
    let mut offset = 0;
    while offset < buffer.len() {
        let count = stream.read(&mut buffer[offset..]).await.ok()?;
        if count == 0 {
            return None;
        }
        offset += count;
    }
    Some(())
}

fn destination_ufrag(packet: &[u8]) -> Option<&str> {
    if packet.len() < STUN_HEADER_LEN
        || packet[0] & 0b1100_0000 != 0
        || packet[4..8] != STUN_MAGIC_COOKIE
    {
        return None;
    }

    let message_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
    if message_len + STUN_HEADER_LEN != packet.len() {
        return None;
    }

    let mut offset = STUN_HEADER_LEN;
    while offset + 4 <= packet.len() {
        let attribute_type = u16::from_be_bytes([packet[offset], packet[offset + 1]]);
        let value_len = u16::from_be_bytes([packet[offset + 2], packet[offset + 3]]) as usize;
        let value_start = offset + 4;
        let value_end = value_start.checked_add(value_len)?;
        if value_end > packet.len() {
            return None;
        }
        if attribute_type == STUN_ATTR_USERNAME {
            let username = std::str::from_utf8(&packet[value_start..value_end]).ok()?;
            return username
                .rsplit_once(':')
                .map(|(_, local_ufrag)| local_ufrag);
        }
        offset = value_end.checked_add((4 - value_len % 4) % 4)?;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::destination_ufrag;

    #[test]
    fn destination_ufrag_uses_the_last_username_component() {
        let mut packet = vec![0, 1, 0, 16, 0x21, 0x12, 0xA4, 0x42];
        packet.extend_from_slice(&[0; 12]);
        packet.extend_from_slice(&[0, 6, 0, 12]);
        packet.extend_from_slice(b"remote:local");

        assert_eq!(destination_ufrag(&packet), Some("local"));
    }
}
