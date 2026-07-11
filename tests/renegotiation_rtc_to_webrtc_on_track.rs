//! Cross-implementation regression coverage for post-connect video renegotiation.
//!
//! An `rtc::RTCPeerConnection` offerer first sends VP8, then adds a distinct
//! H264 media section. The async `webrtc::PeerConnection` answerer must expose
//! the H264 track with its codec resolved and deliver RTP on it.

use anyhow::Result;
use bytes::BytesMut;
use futures::FutureExt;
use rtc::interceptor::{Interceptor, Registry};
use rtc::media_stream::MediaStreamTrack;
use rtc::peer_connection::RTCPeerConnectionBuilder;
use rtc::peer_connection::configuration::RTCConfigurationBuilder;
use rtc::peer_connection::configuration::interceptor_registry::register_default_interceptors;
use rtc::peer_connection::configuration::media_engine::{
    MIME_TYPE_H264, MIME_TYPE_VP8, MediaEngine,
};
use rtc::peer_connection::configuration::setting_engine::SettingEngine;
use rtc::peer_connection::event::RTCPeerConnectionEvent;
use rtc::peer_connection::state::{RTCIceConnectionState, RTCPeerConnectionState};
use rtc::peer_connection::transport::RTCDtlsRole;
use rtc::peer_connection::transport::{CandidateConfig, CandidateHostConfig, RTCIceCandidate};
use rtc::rtp;
use rtc::rtp_transceiver::RTCRtpSenderId;
use rtc::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodecParameters, RTCRtpCodingParameters, RTCRtpEncodingParameters,
    RtpCodecKind,
};
use rtc::sansio::Protocol;
use rtc::shared::{TaggedBytesMut, TransportContext, TransportProtocol};
use std::sync::Arc;
use std::time::{Duration, Instant};
use webrtc::media_stream::track_remote::{TrackRemote, TrackRemoteEvent};
use webrtc::peer_connection::RTCIceGatheringState;
use webrtc::peer_connection::{PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler};
use webrtc::runtime::{Runtime, Sender, block_on, channel, default_runtime, sleep, timeout};

const POLL_INTERVAL: Duration = Duration::from_millis(20);
const TEST_TIMEOUT: Duration = Duration::from_secs(15);

struct TrackObservation {
    ssrc: u32,
    codec_mime: String,
}

struct AnswererHandler {
    gather_complete_tx: Sender<()>,
    state_tx: Sender<RTCPeerConnectionState>,
    track_ready_tx: Sender<TrackObservation>,
    rtp_tx: Sender<String>,
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
        let _ = self.state_tx.try_send(state);
    }

    async fn on_track(&self, track: Arc<dyn TrackRemote>) {
        let Some(ssrc) = track.ssrcs().await.first().copied() else {
            return;
        };
        let Some(codec) = track.codec(ssrc).await else {
            return;
        };
        let codec_mime = codec.mime_type;

        let _ = self.track_ready_tx.try_send(TrackObservation {
            ssrc,
            codec_mime: codec_mime.clone(),
        });

        let rtp_tx = self.rtp_tx.clone();
        self.runtime.spawn(Box::pin(async move {
            while let Some(event) = track.poll().await {
                if matches!(event, TrackRemoteEvent::OnRtpPacket(_)) {
                    let _ = rtp_tx.try_send(codec_mime.clone());
                }
            }
        }));
    }
}

fn video_codec(mime_type: &str, payload_type: u8) -> RTCRtpCodecParameters {
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
    }
}

fn video_track(
    stream_id: &str,
    track_id: &str,
    ssrc: u32,
    codec: &RTCRtpCodec,
) -> MediaStreamTrack {
    MediaStreamTrack::new(
        stream_id.to_owned(),
        track_id.to_owned(),
        format!("{track_id}-label"),
        RtpCodecKind::Video,
        vec![RTCRtpEncodingParameters {
            rtp_coding_parameters: RTCRtpCodingParameters {
                ssrc: Some(ssrc),
                ..Default::default()
            },
            codec: codec.clone(),
            ..Default::default()
        }],
    )
}

fn write_rtp<I: Interceptor>(
    rtc_pc: &mut rtc::peer_connection::RTCPeerConnection<I>,
    sender_id: RTCRtpSenderId,
    ssrc: u32,
    payload_type: u8,
    sequence_number: u16,
) -> Result<()> {
    let mut sender = rtc_pc
        .rtp_sender(sender_id)
        .ok_or_else(|| anyhow::anyhow!("missing RTP sender"))?;
    let _ = sender.write_rtp(rtp::packet::Packet {
        header: rtp::header::Header {
            version: 2,
            payload_type,
            sequence_number,
            timestamp: u32::from(sequence_number) * 3_000,
            ssrc,
            ..Default::default()
        },
        payload: bytes::Bytes::from_static(&[0x90, 0x90, 0x90, 0x90]),
    });
    Ok(())
}

#[test]
fn rtc_offerer_renegotiates_h264_track_to_async_answerer() {
    block_on(run_test()).unwrap();
}

async fn run_test() -> Result<()> {
    let runtime =
        default_runtime().ok_or_else(|| std::io::Error::other("no async runtime found"))?;
    let config = RTCConfigurationBuilder::new().build();
    let vp8_codec = video_codec(MIME_TYPE_VP8, 96);
    let h264_codec = video_codec(MIME_TYPE_H264, 102);

    let (gather_complete_tx, mut gather_complete_rx) = channel::<()>(1);
    let (state_tx, mut state_rx) = channel::<RTCPeerConnectionState>(8);
    let (track_ready_tx, mut track_ready_rx) = channel::<TrackObservation>(4);
    let (rtp_tx, mut rtp_rx) = channel::<String>(16);

    let mut answerer_media_engine = MediaEngine::default();
    answerer_media_engine.register_codec(vp8_codec.clone(), RtpCodecKind::Video)?;
    answerer_media_engine.register_codec(h264_codec.clone(), RtpCodecKind::Video)?;
    let answerer: Arc<dyn PeerConnection> = Arc::new(
        PeerConnectionBuilder::new()
            .with_configuration(config.clone())
            .with_media_engine(answerer_media_engine)
            .with_handler(Arc::new(AnswererHandler {
                gather_complete_tx,
                state_tx,
                track_ready_tx,
                rtp_tx,
                runtime: runtime.clone(),
            }))
            .with_runtime(runtime.clone())
            .with_udp_addrs(vec!["127.0.0.1:0".to_owned()])
            .build()
            .await?,
    );

    let std_socket = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let rtc_local_addr = std_socket.local_addr()?;
    let rtc_socket = runtime.wrap_udp_socket(std_socket)?;

    let mut setting_engine = SettingEngine::default();
    setting_engine.set_answering_dtls_role(RTCDtlsRole::Server)?;
    let mut offerer_media_engine = MediaEngine::default();
    offerer_media_engine.register_codec(vp8_codec.clone(), RtpCodecKind::Video)?;
    offerer_media_engine.register_codec(h264_codec.clone(), RtpCodecKind::Video)?;
    let registry = register_default_interceptors(Registry::new(), &mut offerer_media_engine)?;
    let mut offerer = RTCPeerConnectionBuilder::new()
        .with_configuration(config)
        .with_setting_engine(setting_engine)
        .with_media_engine(offerer_media_engine)
        .with_interceptor_registry(registry)
        .build()?;

    let vp8_ssrc = 0x1020_3040;
    let vp8_sender_id = offerer.add_track(video_track(
        "vp8-stream",
        "vp8-track",
        vp8_ssrc,
        &vp8_codec.rtp_codec,
    ))?;

    let candidate = CandidateHostConfig {
        base_config: CandidateConfig {
            network: "udp".to_owned(),
            address: rtc_local_addr.ip().to_string(),
            port: rtc_local_addr.port(),
            component: 1,
            ..Default::default()
        },
        ..Default::default()
    }
    .new_candidate_host()?;
    offerer.add_local_candidate(RTCIceCandidate::from(&candidate).to_json()?)?;

    let initial_offer = offerer.create_offer(None)?;
    offerer.set_local_description(initial_offer.clone())?;
    answerer
        .set_remote_description(rtc::peer_connection::sdp::RTCSessionDescription::offer(
            initial_offer.sdp,
        )?)
        .await?;
    let initial_answer = answerer.create_answer(None).await?;
    answerer.set_local_description(initial_answer).await?;
    timeout(Duration::from_secs(5), gather_complete_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timeout waiting for answerer ICE gathering"))?;
    let initial_answer = answerer
        .local_description()
        .await
        .expect("answerer local description should be set");
    offerer.set_remote_description(rtc::peer_connection::sdp::RTCSessionDescription::answer(
        initial_answer.sdp,
    )?)?;

    let mut buffer = vec![0_u8; 2_000];
    let mut rtc_connected = false;
    let mut answerer_connected = false;
    let mut vp8_ready = false;
    let mut vp8_rtp_received = false;
    let mut sequence_number = 0_u16;
    let started = Instant::now();

    while !(rtc_connected && answerer_connected && vp8_ready && vp8_rtp_received) {
        if started.elapsed() > TEST_TIMEOUT {
            anyhow::bail!("timeout establishing connected VP8 RTP flow");
        }

        while let Some(message) = offerer.poll_write() {
            rtc_socket
                .send_to(&message.message, message.transport.peer_addr)
                .await?;
        }
        while let Some(event) = offerer.poll_event() {
            match event {
                RTCPeerConnectionEvent::OnIceConnectionStateChangeEvent(
                    RTCIceConnectionState::Failed,
                ) => anyhow::bail!("rtc ICE failed"),
                RTCPeerConnectionEvent::OnConnectionStateChangeEvent(
                    RTCPeerConnectionState::Failed,
                ) => anyhow::bail!("rtc peer connection failed"),
                RTCPeerConnectionEvent::OnConnectionStateChangeEvent(
                    RTCPeerConnectionState::Connected,
                ) => rtc_connected = true,
                _ => {}
            }
        }
        while let Ok(state) = state_rx.try_recv() {
            if state == RTCPeerConnectionState::Connected {
                answerer_connected = true;
            }
        }
        if rtc_connected && answerer_connected {
            write_rtp(&mut offerer, vp8_sender_id, vp8_ssrc, 96, sequence_number)?;
            sequence_number = sequence_number.wrapping_add(1);
        }
        while let Ok(observation) = track_ready_rx.try_recv() {
            if observation.codec_mime == MIME_TYPE_VP8 {
                assert_eq!(observation.ssrc, vp8_ssrc);
                vp8_ready = true;
            }
        }
        while let Ok(codec_mime) = rtp_rx.try_recv() {
            if codec_mime == MIME_TYPE_VP8 {
                vp8_rtp_received = true;
            }
        }

        let deadline = offerer
            .poll_timeout()
            .unwrap_or(Instant::now() + POLL_INTERVAL);
        let delay = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default()
            .min(POLL_INTERVAL);
        if delay.is_zero() {
            offerer.handle_timeout(Instant::now())?;
            continue;
        }
        futures::select! {
            _ = sleep(delay).fuse() => offerer.handle_timeout(Instant::now())?,
            received = rtc_socket.recv_from(&mut buffer).fuse() => {
                let (length, peer_addr) = received?;
                offerer.handle_read(TaggedBytesMut {
                    now: Instant::now(),
                    transport: TransportContext {
                        local_addr: rtc_local_addr,
                        peer_addr,
                        ecn: None,
                        transport_protocol: TransportProtocol::UDP,
                    },
                    message: BytesMut::from(&buffer[..length]),
                })?;
            }
        }
    }

    let h264_ssrc = 0x5060_7080;
    let h264_sender_id = offerer.add_track(video_track(
        "h264-stream",
        "h264-track",
        h264_ssrc,
        &h264_codec.rtp_codec,
    ))?;
    let renegotiation_offer = offerer.create_offer(None)?;
    offerer.set_local_description(renegotiation_offer.clone())?;
    answerer
        .set_remote_description(rtc::peer_connection::sdp::RTCSessionDescription::offer(
            renegotiation_offer.sdp,
        )?)
        .await?;
    let renegotiation_answer = answerer.create_answer(None).await?;
    answerer.set_local_description(renegotiation_answer).await?;
    let renegotiation_answer = answerer
        .local_description()
        .await
        .expect("answerer local description should be set");
    offerer.set_remote_description(rtc::peer_connection::sdp::RTCSessionDescription::answer(
        renegotiation_answer.sdp,
    )?)?;

    let mut h264_ready = false;
    let mut h264_rtp_received = false;
    let started = Instant::now();
    while !(h264_ready && h264_rtp_received) {
        if started.elapsed() > TEST_TIMEOUT {
            anyhow::bail!("timeout waiting for renegotiated H264 track and RTP");
        }

        while let Some(message) = offerer.poll_write() {
            rtc_socket
                .send_to(&message.message, message.transport.peer_addr)
                .await?;
        }
        while let Some(event) = offerer.poll_event() {
            match event {
                RTCPeerConnectionEvent::OnIceConnectionStateChangeEvent(
                    RTCIceConnectionState::Failed,
                ) => anyhow::bail!("rtc ICE failed after renegotiation"),
                RTCPeerConnectionEvent::OnConnectionStateChangeEvent(
                    RTCPeerConnectionState::Failed,
                ) => anyhow::bail!("rtc peer connection failed after renegotiation"),
                _ => {}
            }
        }
        write_rtp(
            &mut offerer,
            h264_sender_id,
            h264_ssrc,
            102,
            sequence_number,
        )?;
        sequence_number = sequence_number.wrapping_add(1);
        while let Ok(observation) = track_ready_rx.try_recv() {
            if observation.codec_mime == MIME_TYPE_H264 {
                assert_eq!(observation.ssrc, h264_ssrc);
                h264_ready = true;
            }
        }
        while let Ok(codec_mime) = rtp_rx.try_recv() {
            if codec_mime == MIME_TYPE_H264 {
                h264_rtp_received = true;
            }
        }

        let deadline = offerer
            .poll_timeout()
            .unwrap_or(Instant::now() + POLL_INTERVAL);
        let delay = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default()
            .min(POLL_INTERVAL);
        if delay.is_zero() {
            offerer.handle_timeout(Instant::now())?;
            continue;
        }
        futures::select! {
            _ = sleep(delay).fuse() => offerer.handle_timeout(Instant::now())?,
            received = rtc_socket.recv_from(&mut buffer).fuse() => {
                let (length, peer_addr) = received?;
                offerer.handle_read(TaggedBytesMut {
                    now: Instant::now(),
                    transport: TransportContext {
                        local_addr: rtc_local_addr,
                        peer_addr,
                        ecn: None,
                        transport_protocol: TransportProtocol::UDP,
                    },
                    message: BytesMut::from(&buffer[..length]),
                })?;
            }
        }
    }

    answerer.close().await?;
    offerer.close()?;
    Ok(())
}
