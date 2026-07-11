use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use webrtc::error::Error;
use webrtc::peer_connection::{
    PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
    RTCPeerConnectionState, SettingEngine,
};
use webrtc::runtime::{Runtime, block_on, channel, default_runtime, sleep, timeout};

const SLOW_THRESHOLD_BPS: u32 = 21_024;
const PAYLOAD_SIZE: usize = 100;
const SEND_COUNT: u64 = 3_000;

#[derive(Clone, Copy)]
struct BitrateWindow {
    start: Instant,
    bytes: usize,
}

struct BitrateCalculator {
    windows: VecDeque<BitrateWindow>,
    active: BitrateWindow,
    bytes_total: usize,
    last_buffered_amount: u64,
    start: Instant,
}

impl BitrateCalculator {
    const DURATION: Duration = Duration::from_secs(2);
    const WINDOW: Duration = Duration::from_millis(100);

    fn new() -> Self {
        let now = Instant::now();
        Self {
            windows: VecDeque::new(),
            active: BitrateWindow {
                start: now,
                bytes: 0,
            },
            bytes_total: 0,
            last_buffered_amount: 0,
            start: now,
        }
    }

    fn add_sample(&mut self, now: Instant, bytes: usize, buffered_amount: u64) {
        let buffered_delta = buffered_amount as i128 - self.last_buffered_amount as i128;
        let delivered = (bytes as i128 - buffered_delta).max(0) as usize;
        self.last_buffered_amount = buffered_amount;

        if now.saturating_duration_since(self.active.start) >= Self::WINDOW {
            let previous = std::mem::replace(
                &mut self.active,
                BitrateWindow {
                    start: now,
                    bytes: 0,
                },
            );
            self.windows.push_back(previous);

            while self.windows.front().is_some_and(|window| {
                now.saturating_duration_since(window.start) > (Self::DURATION + Self::WINDOW)
            }) {
                if let Some(expired) = self.windows.pop_front() {
                    self.bytes_total = self.bytes_total.saturating_sub(expired.bytes);
                }
            }

            if let Some(front) = self.windows.front() {
                self.start = front.start;
            } else {
                self.start = now;
                self.bytes_total = 0;
            }
        }

        self.bytes_total = self.bytes_total.saturating_add(delivered);
        self.active.bytes = self.active.bytes.saturating_add(delivered);
    }

    fn bitrate_bps(&self, now: Instant) -> Option<u32> {
        let elapsed = now.saturating_duration_since(self.start);
        if elapsed < Self::WINDOW {
            return None;
        }

        let elapsed_millis = elapsed.as_millis();
        if elapsed_millis == 0 {
            return None;
        }

        let bps = (self.bytes_total as u128)
            .saturating_mul(8)
            .saturating_mul(1_000)
            .saturating_div(elapsed_millis);
        Some(bps.min(u128::from(u32::MAX)) as u32)
    }
}

struct DataChannelReader {
    target_bps: u32,
    windows: VecDeque<BitrateWindow>,
    active: BitrateWindow,
    bytes_total: usize,
    start: Instant,
}

impl DataChannelReader {
    const DURATION: Duration = Duration::from_secs(10);
    const WINDOW: Duration = Duration::from_millis(100);

    fn new(target_bps: u32) -> Self {
        let now = Instant::now();
        Self {
            target_bps,
            windows: VecDeque::new(),
            active: BitrateWindow {
                start: now,
                bytes: 0,
            },
            bytes_total: 0,
            start: now,
        }
    }

    fn add_bytes(&mut self, bytes: usize, now: Instant) {
        if now.saturating_duration_since(self.active.start) >= Self::WINDOW {
            let previous = std::mem::replace(
                &mut self.active,
                BitrateWindow {
                    start: now,
                    bytes: 0,
                },
            );
            self.windows.push_back(previous);

            while self.windows.front().is_some_and(|window| {
                now.saturating_duration_since(window.start) > (Self::DURATION + Self::WINDOW)
            }) {
                if let Some(expired) = self.windows.pop_front() {
                    self.bytes_total = self.bytes_total.saturating_sub(expired.bytes);
                }
            }

            if let Some(front) = self.windows.front() {
                self.start = front.start;
            } else {
                self.start = now;
                self.bytes_total = 0;
            }
        }

        self.bytes_total = self.bytes_total.saturating_add(bytes);
        self.active.bytes = self.active.bytes.saturating_add(bytes);
    }

    fn force_bitrate_bps(&self, now: Instant) -> u128 {
        let elapsed = now.saturating_duration_since(self.start).max(Self::WINDOW);
        let elapsed_millis = elapsed.as_millis().max(1);
        (self.bytes_total as u128)
            .saturating_mul(8)
            .saturating_mul(1_000)
            .saturating_div(elapsed_millis)
    }

    async fn read(&mut self, bytes: usize) {
        loop {
            let now = Instant::now();
            if self.force_bitrate_bps(now) <= u128::from(self.target_bps) {
                self.add_bytes(bytes, now);
                return;
            }

            sleep(Duration::from_millis(10)).await;
            self.add_bytes(0, Instant::now());
        }
    }
}

struct SenderHandler {
    gather_complete_tx: webrtc::runtime::Sender<()>,
    connected_tx: webrtc::runtime::Sender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for SenderHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_complete_tx.try_send(());
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        if state == RTCPeerConnectionState::Connected {
            let _ = self.connected_tx.try_send(());
        }
    }
}

struct ReceiverHandler {
    gather_complete_tx: webrtc::runtime::Sender<()>,
    connected_tx: webrtc::runtime::Sender<()>,
    data_channel_tx: webrtc::runtime::Sender<Arc<dyn DataChannel>>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for ReceiverHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_complete_tx.try_send(());
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        if state == RTCPeerConnectionState::Connected {
            let _ = self.connected_tx.try_send(());
        }
    }

    async fn on_data_channel(&self, data_channel: Arc<dyn DataChannel>) {
        let _ = self.data_channel_tx.try_send(data_channel);
    }
}

struct Pair {
    sender_pc: Arc<dyn webrtc::peer_connection::PeerConnection>,
    receiver_pc: Arc<dyn webrtc::peer_connection::PeerConnection>,
    sender_dc: Arc<dyn DataChannel>,
}

async fn connected_pair(
    runtime: Arc<dyn Runtime>,
    label: &str,
) -> anyhow::Result<(Pair, Arc<dyn DataChannel>)> {
    let mut setting_engine = SettingEngine::default();
    setting_engine.detach_data_channels();
    setting_engine.set_data_channel_block_write(true);

    let (s_gather_tx, mut s_gather_rx) = channel(1);
    let (s_conn_tx, mut s_conn_rx) = channel(1);
    let sender_pc = PeerConnectionBuilder::new()
        .with_setting_engine(setting_engine.clone())
        .with_handler(Arc::new(SenderHandler {
            gather_complete_tx: s_gather_tx,
            connected_tx: s_conn_tx,
        }))
        .with_runtime(runtime.clone())
        .with_udp_addrs(vec!["127.0.0.1:0".to_owned()])
        .build()
        .await?;
    let sender_pc: Arc<dyn webrtc::peer_connection::PeerConnection> = Arc::new(sender_pc);

    let (r_gather_tx, mut r_gather_rx) = channel(1);
    let (r_conn_tx, mut r_conn_rx) = channel(1);
    let (r_dc_tx, mut r_dc_rx) = channel(1);
    let receiver_pc = PeerConnectionBuilder::new()
        .with_setting_engine(setting_engine)
        .with_handler(Arc::new(ReceiverHandler {
            gather_complete_tx: r_gather_tx,
            connected_tx: r_conn_tx,
            data_channel_tx: r_dc_tx,
        }))
        .with_runtime(runtime)
        .with_udp_addrs(vec!["127.0.0.1:0".to_owned()])
        .build()
        .await?;
    let receiver_pc: Arc<dyn webrtc::peer_connection::PeerConnection> = Arc::new(receiver_pc);

    let sender_dc = sender_pc.create_data_channel(label, None).await?;

    let offer = sender_pc.create_offer(None).await?;
    sender_pc.set_local_description(offer).await?;
    let _ = timeout(Duration::from_secs(5), s_gather_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for sender ICE gathering"))?;
    let offer_sdp = sender_pc
        .local_description()
        .await
        .ok_or_else(|| std::io::Error::other("sender local description missing"))?;

    receiver_pc.set_remote_description(offer_sdp).await?;
    let answer = receiver_pc.create_answer(None).await?;
    receiver_pc.set_local_description(answer).await?;
    let _ = timeout(Duration::from_secs(5), r_gather_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for receiver ICE gathering"))?;
    let answer_sdp = receiver_pc
        .local_description()
        .await
        .ok_or_else(|| std::io::Error::other("receiver local description missing"))?;

    sender_pc.set_remote_description(answer_sdp).await?;

    let _ = timeout(Duration::from_secs(10), s_conn_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for sender connected"))?
        .ok_or_else(|| anyhow::anyhow!("sender connection channel closed"))?;
    let _ = timeout(Duration::from_secs(10), r_conn_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for receiver connected"))?
        .ok_or_else(|| anyhow::anyhow!("receiver connection channel closed"))?;

    let receiver_dc = timeout(Duration::from_secs(10), r_dc_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for receiver data channel"))?
        .ok_or_else(|| anyhow::anyhow!("receiver data-channel receiver closed"))?;

    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(DataChannelEvent::OnOpen) = sender_dc.poll().await {
                break;
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for sender data channel open"))?;

    Ok((
        Pair {
            sender_pc,
            receiver_pc,
            sender_dc,
        },
        receiver_dc,
    ))
}

enum SendOutcome {
    Sent,
    DroppedBySlowReader,
}

struct ReliableWriter {
    channel: Arc<dyn DataChannel>,
    threshold_bps: u32,
    rate: BitrateCalculator,
}

impl ReliableWriter {
    fn new(channel: Arc<dyn DataChannel>, threshold_bps: u32) -> Self {
        Self {
            channel,
            threshold_bps,
            rate: BitrateCalculator::new(),
        }
    }

    async fn send(&mut self, payload: &[u8]) -> anyhow::Result<SendOutcome> {
        loop {
            let result = self
                .channel
                .send_with_timeout(BytesMut::from(payload), Duration::from_millis(50))
                .await;
            let buffered_amount = self.channel.buffered_amount().await?;
            let now = Instant::now();

            match result {
                Ok(()) => {
                    self.rate.add_sample(now, payload.len(), buffered_amount);
                    return Ok(SendOutcome::Sent);
                }
                Err(Error::ErrTimeout) => {
                    self.rate.add_sample(now, 0, buffered_amount);
                    let should_retry = self
                        .rate
                        .bitrate_bps(now)
                        .map(|bps| bps >= self.threshold_bps)
                        .unwrap_or(true);
                    if should_retry {
                        continue;
                    }
                    return Ok(SendOutcome::DroppedBySlowReader);
                }
                Err(err) => return Err(anyhow::anyhow!(err.to_string())),
            }
        }
    }
}

#[test]
fn test_reliable_timeout_threshold_keeps_above_threshold_receiver_contiguous() {
    block_on(async {
        let runtime = default_runtime().expect("runtime should be available");

        let (fast_pair, fast_receiver_dc) = connected_pair(runtime.clone(), "fast").await?;
        let (no_drop_pair, no_drop_receiver_dc) =
            connected_pair(runtime.clone(), "no-drop").await?;
        let (drop_pair, drop_receiver_dc) = connected_pair(runtime.clone(), "drop").await?;

        for dc in [
            &fast_pair.sender_dc,
            &no_drop_pair.sender_dc,
            &drop_pair.sender_dc,
        ] {
            dc.set_buffered_amount_low_threshold(SLOW_THRESHOLD_BPS / 2)
                .await?;
            dc.set_buffered_amount_high_threshold(SLOW_THRESHOLD_BPS)
                .await?;
        }

        let fast_last = Arc::new(AtomicU64::new(0));
        let no_drop_last = Arc::new(AtomicU64::new(0));
        let drop_last = Arc::new(AtomicU64::new(0));
        let fast_gap = Arc::new(AtomicBool::new(false));
        let no_drop_gap = Arc::new(AtomicBool::new(false));
        let drop_gap = Arc::new(AtomicBool::new(false));

        let fast_last_task = fast_last.clone();
        let fast_gap_task = fast_gap.clone();
        let fast_task = runtime.spawn(Box::pin(async move {
            while let Some(event) = fast_receiver_dc.poll().await {
                if let DataChannelEvent::OnMessage(message) = event {
                    let payload = message.data;
                    let tail = &payload[payload.len().saturating_sub(8)..];
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(tail);
                    let idx = u64::from_be_bytes(bytes);
                    let expected = fast_last_task.load(Ordering::Relaxed) + 1;
                    if idx != expected {
                        fast_gap_task.store(true, Ordering::Relaxed);
                    }
                    fast_last_task.store(idx, Ordering::Relaxed);
                }
            }
        }));

        let no_drop_last_task = no_drop_last.clone();
        let no_drop_gap_task = no_drop_gap.clone();
        let no_drop_task = runtime.spawn(Box::pin(async move {
            let mut reader = DataChannelReader::new(SLOW_THRESHOLD_BPS * 2);
            while let Some(event) = no_drop_receiver_dc.poll().await {
                if let DataChannelEvent::OnMessage(message) = event {
                    let payload = message.data;
                    let tail = &payload[payload.len().saturating_sub(8)..];
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(tail);
                    let idx = u64::from_be_bytes(bytes);
                    let expected = no_drop_last_task.load(Ordering::Relaxed) + 1;
                    if idx != expected {
                        no_drop_gap_task.store(true, Ordering::Relaxed);
                    }
                    no_drop_last_task.store(idx, Ordering::Relaxed);
                    reader.read(payload.len()).await;
                }
            }
        }));

        let drop_last_task = drop_last.clone();
        let drop_gap_task = drop_gap.clone();
        let drop_task = runtime.spawn(Box::pin(async move {
            let mut reader = DataChannelReader::new(SLOW_THRESHOLD_BPS / 2);
            while let Some(event) = drop_receiver_dc.poll().await {
                if let DataChannelEvent::OnMessage(message) = event {
                    let payload = message.data;
                    let tail = &payload[payload.len().saturating_sub(8)..];
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(tail);
                    let idx = u64::from_be_bytes(bytes);
                    let expected = drop_last_task.load(Ordering::Relaxed) + 1;
                    if idx != expected {
                        drop_gap_task.store(true, Ordering::Relaxed);
                    }
                    drop_last_task.store(idx, Ordering::Relaxed);
                    reader.read(payload.len()).await;
                }
            }
        }));

        let mut fast_writer = ReliableWriter::new(fast_pair.sender_dc.clone(), SLOW_THRESHOLD_BPS);
        let mut no_drop_writer =
            ReliableWriter::new(no_drop_pair.sender_dc.clone(), SLOW_THRESHOLD_BPS);
        let mut drop_writer = ReliableWriter::new(drop_pair.sender_dc.clone(), SLOW_THRESHOLD_BPS);

        let mut drop_observed = false;
        for idx in 1..=SEND_COUNT {
            let mut payload = vec![0u8; PAYLOAD_SIZE];
            payload[(PAYLOAD_SIZE - 8)..].copy_from_slice(&idx.to_be_bytes());

            let fast_outcome = fast_writer.send(&payload).await?;
            assert!(matches!(fast_outcome, SendOutcome::Sent));

            let no_drop_outcome = no_drop_writer.send(&payload).await?;
            if matches!(no_drop_outcome, SendOutcome::DroppedBySlowReader) {
                anyhow::bail!("above-threshold receiver was classified slow at sequence {idx}");
            }

            let drop_outcome = drop_writer.send(&payload).await?;
            if matches!(drop_outcome, SendOutcome::DroppedBySlowReader) {
                drop_observed = true;
            }

            if idx % 64 == 0 {
                sleep(Duration::from_millis(1)).await;
            }
        }

        timeout(Duration::from_secs(20), async {
            loop {
                let sent = SEND_COUNT;
                if fast_last.load(Ordering::Relaxed) == sent
                    && no_drop_last.load(Ordering::Relaxed) == sent
                {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for receiver delivery"))?;

        assert!(drop_observed || drop_gap.load(Ordering::Relaxed));
        assert!(
            !fast_gap.load(Ordering::Relaxed),
            "fast receiver should remain contiguous"
        );
        assert!(
            !no_drop_gap.load(Ordering::Relaxed),
            "above-threshold receiver should remain contiguous"
        );

        fast_pair.sender_pc.close().await?;
        fast_pair.receiver_pc.close().await?;
        no_drop_pair.sender_pc.close().await?;
        no_drop_pair.receiver_pc.close().await?;
        drop_pair.sender_pc.close().await?;
        drop_pair.receiver_pc.close().await?;

        fast_task.abort();
        no_drop_task.abort();
        drop_task.abort();

        Ok::<(), anyhow::Error>(())
    })
    .expect("slow-reader threshold regression should pass");
}
