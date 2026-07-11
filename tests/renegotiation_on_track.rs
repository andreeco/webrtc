//! Connected-peer regression coverage for post-connect codec renegotiation.
//!
//! A second video media section must produce a new remote-track callback with
//! the negotiated codec ready before RTP is delivered.

use anyhow::{Context, Result};
use rtc::media_stream::MediaStreamTrack;
use rtc::peer_connection::configuration::media_engine::{MIME_TYPE_H264, MIME_TYPE_VP8};
use rtc::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodecParameters, RTCRtpCodingParameters, RTCRtpEncodingParameters,
    RtpCodecKind,
};
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use std::time::{Duration, Instant};
use webrtc::media_stream::track_local::TrackLocal;
use webrtc::media_stream::track_local::static_rtp::TrackLocalStaticRTP;
use webrtc::media_stream::track_remote::{TrackRemote, TrackRemoteEvent};
use webrtc::peer_connection::{
    MediaEngine, PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler,
    RTCIceGatheringState, RTCPeerConnectionState,
};
use webrtc::rtp_transceiver::{RTCRtpTransceiverDirection, RTCRtpTransceiverInit};
use webrtc::runtime::{Runtime, Sender, block_on, channel, default_runtime, sleep, timeout};

struct OffererHandler {
    gather_complete_tx: Sender<()>,
    connected_tx: Sender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for OffererHandler {
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

struct AnswererHandler {
    gather_complete_tx: Sender<()>,
    connected_tx: Sender<()>,
    vp8_track_count: Arc<AtomicU32>,
    h264_track_count: Arc<AtomicU32>,
    h264_rtp_packet_count: Arc<AtomicU32>,
    runtime: Arc<dyn Runtime>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for AnswererHandler {
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

    async fn on_track(&self, track: Arc<dyn TrackRemote>) {
        let ssrcs = track.ssrcs().await;
        let codec_mime = match ssrcs.first().copied() {
            Some(ssrc) => track.codec(ssrc).await.map(|codec| codec.mime_type),
            None => None,
        };

        match codec_mime.as_deref() {
            Some(MIME_TYPE_VP8) => {
                self.vp8_track_count.fetch_add(1, Ordering::SeqCst);
            }
            Some(MIME_TYPE_H264) => {
                self.h264_track_count.fetch_add(1, Ordering::SeqCst);
                let h264_rtp_packet_count = self.h264_rtp_packet_count.clone();
                self.runtime.spawn(Box::pin(async move {
                    while let Some(event) = track.poll().await {
                        if matches!(event, TrackRemoteEvent::OnRtpPacket(_)) {
                            h264_rtp_packet_count.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }));
            }
            _ => {}
        }
    }
}

fn new_video_track(
    stream_id: &str,
    track_id: &str,
    ssrc: u32,
    mime_type: &str,
) -> Arc<TrackLocalStaticRTP> {
    Arc::new(TrackLocalStaticRTP::new(MediaStreamTrack::new(
        stream_id.to_owned(),
        track_id.to_owned(),
        format!("track-{track_id}"),
        RtpCodecKind::Video,
        vec![RTCRtpEncodingParameters {
            rtp_coding_parameters: RTCRtpCodingParameters {
                ssrc: Some(ssrc),
                ..Default::default()
            },
            codec: RTCRtpCodec {
                mime_type: mime_type.to_owned(),
                clock_rate: 90_000,
                channels: 0,
                sdp_fmtp_line: String::new(),
                rtcp_feedback: vec![],
            },
            ..Default::default()
        }],
    )))
}

async fn negotiate(
    offerer: &Arc<dyn PeerConnection>,
    answerer: &Arc<dyn PeerConnection>,
) -> Result<()> {
    let offer = offerer.create_offer(None).await?;
    offerer.set_local_description(offer).await?;
    let offer = offerer
        .local_description()
        .await
        .expect("offerer local description should be set");
    answerer.set_remote_description(offer).await?;

    let answer = answerer.create_answer(None).await?;
    answerer.set_local_description(answer).await?;
    let answer = answerer
        .local_description()
        .await
        .expect("answerer local description should be set");
    offerer.set_remote_description(answer).await?;

    Ok(())
}

async fn send_rtp(track: &Arc<TrackLocalStaticRTP>, ssrc: u32, payload_type: u8) -> Result<()> {
    for sequence_number in 0_u16..20 {
        track
            .write_rtp(rtc::rtp::packet::Packet {
                header: rtc::rtp::header::Header {
                    version: 2,
                    payload_type,
                    sequence_number,
                    timestamp: u32::from(sequence_number) * 3_000,
                    ssrc,
                    ..Default::default()
                },
                payload: bytes::Bytes::from_static(&[0x90, 0x90, 0x90, 0x90]),
            })
            .await?;
        sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

async fn wait_for(condition: impl Fn() -> bool, description: &str) -> Result<()> {
    let started = Instant::now();
    while !condition() {
        if started.elapsed() > Duration::from_secs(10) {
            anyhow::bail!("timeout waiting for {description}");
        }
        sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

#[test]
fn renegotiation_adds_h264_remote_track_with_rtp() {
    block_on(run_test()).unwrap();
}

async fn run_test() -> Result<()> {
    let runtime =
        default_runtime().ok_or_else(|| std::io::Error::other("no async runtime found"))?;
    let mut offerer_media = MediaEngine::default();
    for (mime_type, payload_type) in [(MIME_TYPE_VP8, 96), (MIME_TYPE_H264, 102)] {
        offerer_media.register_codec(
            RTCRtpCodecParameters {
                rtp_codec: RTCRtpCodec {
                    mime_type: mime_type.to_owned(),
                    clock_rate: 90_000,
                    channels: 0,
                    sdp_fmtp_line: String::new(),
                    rtcp_feedback: vec![],
                },
                payload_type,
                ..Default::default()
            },
            RtpCodecKind::Video,
        )?;
    }
    let answerer_media = offerer_media.clone();

    let (offerer_gather_tx, mut offerer_gather_rx) = channel(1);
    let (offerer_connected_tx, mut offerer_connected_rx) = channel(1);
    let (answerer_gather_tx, mut answerer_gather_rx) = channel(1);
    let (answerer_connected_tx, mut answerer_connected_rx) = channel(1);
    let vp8_track_count = Arc::new(AtomicU32::new(0));
    let h264_track_count = Arc::new(AtomicU32::new(0));
    let h264_rtp_packet_count = Arc::new(AtomicU32::new(0));

    let offerer: Arc<dyn PeerConnection> = Arc::new(
        PeerConnectionBuilder::new()
            .with_media_engine(offerer_media)
            .with_handler(Arc::new(OffererHandler {
                gather_complete_tx: offerer_gather_tx,
                connected_tx: offerer_connected_tx,
            }))
            .with_runtime(runtime.clone())
            .with_udp_addrs(vec!["127.0.0.1:0".to_owned()])
            .build()
            .await?,
    );
    let answerer: Arc<dyn PeerConnection> = Arc::new(
        PeerConnectionBuilder::new()
            .with_media_engine(answerer_media)
            .with_handler(Arc::new(AnswererHandler {
                gather_complete_tx: answerer_gather_tx,
                connected_tx: answerer_connected_tx,
                vp8_track_count: vp8_track_count.clone(),
                h264_track_count: h264_track_count.clone(),
                h264_rtp_packet_count: h264_rtp_packet_count.clone(),
                runtime: runtime.clone(),
            }))
            .with_runtime(runtime.clone())
            .with_udp_addrs(vec!["127.0.0.1:0".to_owned()])
            .build()
            .await?,
    );

    let vp8_ssrc = 0x1020_3040;
    let vp8_track = new_video_track("stream-vp8", "vp8", vp8_ssrc, MIME_TYPE_VP8);
    offerer
        .add_track(Arc::clone(&vp8_track) as Arc<dyn TrackLocal>)
        .await
        .context("add initial VP8 track")?;
    let initial_offer = offerer.create_offer(None).await?;
    offerer.set_local_description(initial_offer).await?;
    timeout(Duration::from_secs(5), offerer_gather_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timeout waiting for offerer ICE gathering"))?;
    let initial_offer = offerer
        .local_description()
        .await
        .expect("offerer local description should be set");
    answerer.set_remote_description(initial_offer).await?;
    let initial_answer = answerer.create_answer(None).await?;
    answerer.set_local_description(initial_answer).await?;
    timeout(Duration::from_secs(5), answerer_gather_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timeout waiting for answerer ICE gathering"))?;
    let initial_answer = answerer
        .local_description()
        .await
        .expect("answerer local description should be set");
    offerer.set_remote_description(initial_answer).await?;
    timeout(Duration::from_secs(15), offerer_connected_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timeout waiting for offerer connection"))?;
    timeout(Duration::from_secs(15), answerer_connected_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timeout waiting for answerer connection"))?;

    send_rtp(&vp8_track, vp8_ssrc, 96).await?;
    wait_for(
        || vp8_track_count.load(Ordering::SeqCst) == 1,
        "initial VP8 remote track",
    )
    .await?;

    let h264_ssrc = 0x5060_7080;
    let h264_track = new_video_track("stream-h264", "h264", h264_ssrc, MIME_TYPE_H264);
    offerer
        .add_transceiver_from_track(
            Arc::clone(&h264_track) as Arc<dyn TrackLocal>,
            Some(RTCRtpTransceiverInit {
                direction: RTCRtpTransceiverDirection::Sendonly,
                streams: vec![],
                send_encodings: vec![],
            }),
        )
        .await
        .context("add renegotiated H264 transceiver")?;
    negotiate(&offerer, &answerer).await?;
    send_rtp(&h264_track, h264_ssrc, 102).await?;

    wait_for(
        || h264_track_count.load(Ordering::SeqCst) == 1,
        "renegotiated H264 remote track with resolved codec",
    )
    .await?;
    wait_for(
        || h264_rtp_packet_count.load(Ordering::SeqCst) > 0,
        "RTP on renegotiated H264 remote track",
    )
    .await?;

    offerer.close().await?;
    answerer.close().await?;
    Ok(())
}
