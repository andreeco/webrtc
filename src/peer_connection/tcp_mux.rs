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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const DEFAULT_INITIAL_FRAME_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_MAX_INITIAL_DISPATCHES: usize = 128;

const RFC4571_HEADER_LEN: usize = 2;
const STUN_HEADER_LEN: usize = 20;
const STUN_MAGIC_COOKIE: [u8; 4] = [0x21, 0x12, 0xA4, 0x42];
const STUN_ATTR_USERNAME: u16 = 0x0006;

/// Configuration for a shared [`TcpMux`].
///
/// The defaults allow a remote peer five seconds to provide its first RFC 4571
/// STUN frame and permit 128 concurrent connections awaiting that frame. These
/// bounds prevent idle TCP clients from retaining unbounded tasks and sockets
/// while leaving enough headroom for normal ICE/TCP connection bursts.
#[derive(Clone, Debug)]
pub struct TcpMuxConfig {
    initial_frame_timeout: Duration,
    max_initial_dispatches: usize,
}

impl Default for TcpMuxConfig {
    fn default() -> Self {
        Self {
            initial_frame_timeout: DEFAULT_INITIAL_FRAME_TIMEOUT,
            max_initial_dispatches: DEFAULT_MAX_INITIAL_DISPATCHES,
        }
    }
}

impl TcpMuxConfig {
    /// Sets the maximum time to receive the first complete RFC 4571 frame.
    pub fn with_initial_frame_timeout(mut self, timeout: Duration) -> Self {
        self.initial_frame_timeout = timeout;
        self
    }

    /// Sets the maximum number of connections awaiting their initial frame.
    ///
    /// A value of zero is rejected by [`TcpMux::bind_with_config`].
    pub fn with_max_initial_dispatches(mut self, max_initial_dispatches: usize) -> Self {
        self.max_initial_dispatches = max_initial_dispatches;
        self
    }
}

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
    initial_frame_timeout: Duration,
    max_initial_dispatches: usize,
    initial_dispatches: AtomicUsize,
    routes: Mutex<HashMap<String, Sender<PeerConnectionDriverEvent>>>,
}

struct InitialDispatchPermit {
    inner: Arc<TcpMuxInner>,
}

impl Drop for InitialDispatchPermit {
    fn drop(&mut self) {
        self.inner
            .initial_dispatches
            .fetch_sub(1, Ordering::Release);
    }
}

impl TcpMuxInner {
    fn try_acquire_initial_dispatch(self: &Arc<Self>) -> Option<InitialDispatchPermit> {
        let mut current = self.initial_dispatches.load(Ordering::Acquire);
        loop {
            if current >= self.max_initial_dispatches {
                return None;
            }
            match self.initial_dispatches.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(InitialDispatchPermit {
                        inner: self.clone(),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }
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
    /// Binds a shared passive TCP listener with [`TcpMuxConfig::default`] bounds.
    pub fn bind<A: ToSocketAddrs>(runtime: Arc<dyn Runtime>, addr: A) -> io::Result<Self> {
        Self::bind_with_config(runtime, addr, TcpMuxConfig::default())
    }

    /// Binds a shared passive TCP listener with explicit initial-dispatch bounds.
    pub fn bind_with_config<A: ToSocketAddrs>(
        runtime: Arc<dyn Runtime>,
        addr: A,
        config: TcpMuxConfig,
    ) -> io::Result<Self> {
        if config.max_initial_dispatches == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TCP mux initial dispatch limit must be greater than zero",
            ));
        }

        let listener = std::net::TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;
        let listener = runtime.wrap_tcp_listener(listener)?;
        let inner = Arc::new(TcpMuxInner {
            local_addr,
            initial_frame_timeout: config.initial_frame_timeout,
            max_initial_dispatches: config.max_initial_dispatches,
            initial_dispatches: AtomicUsize::new(0),
            routes: Mutex::new(HashMap::new()),
        });
        let accept_inner = inner.clone();
        let accept_runtime = runtime.clone();

        runtime.spawn(Box::pin(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer_addr)) => {
                        let Some(permit) = accept_inner.try_acquire_initial_dispatch() else {
                            trace!(
                                "dropping shared ICE/TCP connection: initial dispatch limit reached"
                            );
                            continue;
                        };
                        let inner = accept_inner.clone();
                        accept_runtime.spawn(Box::pin(async move {
                            dispatch_initial_frame(inner, stream, peer_addr, permit).await;
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
    _permit: InitialDispatchPermit,
) {
    let packet = match crate::runtime::timeout(
        inner.initial_frame_timeout,
        read_rfc4571_frame(stream.clone()),
    )
    .await
    {
        Ok(Some(packet)) => packet,
        Ok(None) => return,
        Err(()) => {
            trace!("dropping shared ICE/TCP connection: initial RFC 4571 frame timed out");
            return;
        }
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

#[cfg(all(test, feature = "runtime-tokio"))]
mod tests {
    use super::{RFC4571_HEADER_LEN, TcpMux, TcpMuxConfig, destination_ufrag};
    use crate::runtime::{TokioRuntime, channel};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn destination_ufrag_uses_the_last_username_component() {
        let mut packet = vec![0, 1, 0, 16, 0x21, 0x12, 0xA4, 0x42];
        packet.extend_from_slice(&[0; 12]);
        packet.extend_from_slice(&[0, 6, 0, 12]);
        packet.extend_from_slice(b"remote:local");

        assert_eq!(destination_ufrag(&packet), Some("local"));
    }

    #[tokio::test]
    async fn stalled_initial_frame_times_out_and_releases_dispatch_capacity() {
        let runtime = Arc::new(TokioRuntime);
        let mux = TcpMux::bind_with_config(
            runtime,
            "127.0.0.1:0",
            TcpMuxConfig::default()
                .with_initial_frame_timeout(Duration::from_millis(25))
                .with_max_initial_dispatches(1),
        )
        .expect("bind TCP mux");
        let (sender, mut receiver) = channel(1);
        let _registration = mux
            .register("local".into(), sender)
            .expect("register route");

        let stalled = tokio::net::TcpStream::connect(mux.local_addr())
            .await
            .expect("connect stalled client");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut valid = tokio::net::TcpStream::connect(mux.local_addr())
            .await
            .expect("connect valid client");
        valid
            .write_all(&rfc4571_stun_frame("remote:local"))
            .await
            .expect("write initial STUN frame");

        assert!(
            tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("valid connection was dispatched")
                .is_some()
        );
        drop(stalled);
    }

    #[tokio::test]
    async fn initial_dispatch_limit_rejects_excess_connections_and_recovers() {
        let runtime = Arc::new(TokioRuntime);
        let mux = TcpMux::bind_with_config(
            runtime,
            "127.0.0.1:0",
            TcpMuxConfig::default()
                .with_initial_frame_timeout(Duration::from_secs(1))
                .with_max_initial_dispatches(1),
        )
        .expect("bind TCP mux");
        let (sender, mut receiver) = channel(1);
        let _registration = mux
            .register("local".into(), sender)
            .expect("register route");

        let stalled = tokio::net::TcpStream::connect(mux.local_addr())
            .await
            .expect("connect stalled client");
        tokio::time::sleep(Duration::from_millis(25)).await;

        let mut rejected = tokio::net::TcpStream::connect(mux.local_addr())
            .await
            .expect("connect rejected client");
        rejected
            .write_all(&rfc4571_stun_frame("remote:local"))
            .await
            .expect("write rejected initial STUN frame");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), receiver.recv())
                .await
                .is_err(),
            "the full dispatch limit must reject the second connection"
        );

        drop(stalled);
        tokio::time::sleep(Duration::from_millis(25)).await;

        let mut recovered = tokio::net::TcpStream::connect(mux.local_addr())
            .await
            .expect("connect recovered client");
        recovered
            .write_all(&rfc4571_stun_frame("remote:local"))
            .await
            .expect("write recovered initial STUN frame");
        assert!(
            tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("recovered connection was dispatched")
                .is_some()
        );
    }

    fn rfc4571_stun_frame(username: &str) -> Vec<u8> {
        let username_len = username.len();
        let padded_username_len = (username_len + 3) & !3;
        let mut packet = vec![
            0,
            1,
            0,
            (4 + padded_username_len) as u8,
            0x21,
            0x12,
            0xA4,
            0x42,
        ];
        packet.extend_from_slice(&[0; 12]);
        packet.extend_from_slice(&[0, 6, 0, username_len as u8]);
        packet.extend_from_slice(username.as_bytes());
        packet.resize(packet.len() + padded_username_len - username_len, 0);

        let mut frame = Vec::with_capacity(RFC4571_HEADER_LEN + packet.len());
        frame.extend_from_slice(&(packet.len() as u16).to_be_bytes());
        frame.extend_from_slice(&packet);
        frame
    }
}
