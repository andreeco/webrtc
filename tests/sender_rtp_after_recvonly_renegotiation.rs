//! Regression test for sender RTP delivery after single-PC style recvonly renegotiation.
//!
//! The publishing peer keeps an existing audio sender while reserving additional recvonly
//! sections and renegotiating. RTP written through `write_rtp_with_sdes_mid` must continue
//! reaching the remote peer after renegotiation.

use rtc::media_stream::MediaStreamTrack;
use rtc::rtp::header::Header;
use rtc::rtp::packet::Packet;
use rtc::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodingParameters, RTCRtpEncodingParameters, RtpCodecKind,
};
use rtc::rtp_transceiver::{RTCRtpTransceiverDirection, RTCRtpTransceiverInit};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use webrtc::media_stream::track_local::TrackLocal;
use webrtc::media_stream::track_local::static_rtp::TrackLocalStaticRTP;
use webrtc::media_stream::track_remote::{TrackRemote, TrackRemoteEvent};
use webrtc::peer_connection::{
    MediaEngine, PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler,
    RTCIceGatheringState, RTCPeerConnectionState,
};
use webrtc::runtime::{Runtime, Sender, block_on, channel, default_runtime, sleep, timeout};

struct TestHandler {
    gathered_tx: Sender<()>,
    connected_tx: Sender<()>,
    packets_rx: Option<Arc<AtomicUsize>>,
    runtime: Arc<dyn Runtime>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for TestHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gathered_tx.try_send(());
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        if state == RTCPeerConnectionState::Connected {
            let _ = self.connected_tx.try_send(());
        }
    }

    async fn on_track(&self, track: Arc<dyn TrackRemote>) {
        let Some(counter) = self.packets_rx.clone() else {
            return;
        };
        self.runtime.spawn(Box::pin(async move {
            while let Some(event) = track.poll().await {
                if matches!(event, TrackRemoteEvent::OnRtpPacket(_)) {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }
}

fn new_audio_track(stream_id: &str, track_id: &str, ssrc: u32) -> Arc<TrackLocalStaticRTP> {
    Arc::new(TrackLocalStaticRTP::new(MediaStreamTrack::new(
        stream_id.to_owned(),
        track_id.to_owned(),
        track_id.to_owned(),
        RtpCodecKind::Audio,
        vec![RTCRtpEncodingParameters {
            rtp_coding_parameters: RTCRtpCodingParameters {
                ssrc: Some(ssrc),
                ..Default::default()
            },
            codec: RTCRtpCodec {
                mime_type: "audio/opus".to_owned(),
                clock_rate: 48_000,
                channels: 2,
                sdp_fmtp_line: "minptime=10;useinbandfec=1".to_owned(),
                rtcp_feedback: vec![],
            },
            ..Default::default()
        }],
    )))
}

async fn negotiate(
    offerer: &Arc<dyn PeerConnection>,
    answerer: &Arc<dyn PeerConnection>,
    offerer_gather_rx: &mut webrtc::runtime::Receiver<()>,
    answerer_gather_rx: &mut webrtc::runtime::Receiver<()>,
) -> anyhow::Result<()> {
    let offer = offerer.create_offer(None).await?;
    offerer.set_local_description(offer).await?;
    let _ = timeout(Duration::from_secs(5), offerer_gather_rx.recv()).await;

    let offer_sdp = offerer
        .local_description()
        .await
        .ok_or_else(|| anyhow::anyhow!("offerer local description missing"))?;
    answerer.set_remote_description(offer_sdp).await?;

    let answer = answerer.create_answer(None).await?;
    answerer.set_local_description(answer).await?;
    let _ = timeout(Duration::from_secs(5), answerer_gather_rx.recv()).await;

    let answer_sdp = answerer
        .local_description()
        .await
        .ok_or_else(|| anyhow::anyhow!("answerer local description missing"))?;
    offerer.set_remote_description(answer_sdp).await?;

    Ok(())
}

async fn wait_for_packets(counter: &Arc<AtomicUsize>, at_least: usize) -> anyhow::Result<()> {
    timeout(Duration::from_secs(10), async {
        while counter.load(Ordering::SeqCst) < at_least {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for RTP packets >= {at_least}"))
}

#[test]
fn sender_rtp_survives_recvonly_renegotiation() {
    block_on(run_test()).expect("RTP should continue after recvonly renegotiation");
}

async fn run_test() -> anyhow::Result<()> {
    let runtime = default_runtime().ok_or_else(|| std::io::Error::other("no runtime"))?;
    let mut media = MediaEngine::default();
    media.register_default_codecs()?;

    let receiver_packets = Arc::new(AtomicUsize::new(0));
    let (offer_gather_tx, mut offer_gather_rx) = channel(1);
    let (answer_gather_tx, mut answer_gather_rx) = channel(1);
    let (offer_connected_tx, mut offer_connected_rx) = channel(1);
    let (answer_connected_tx, mut answer_connected_rx) = channel(1);

    let offerer: Arc<dyn PeerConnection> = Arc::new(
        PeerConnectionBuilder::new()
            .with_media_engine(media.clone())
            .with_handler(Arc::new(TestHandler {
                gathered_tx: offer_gather_tx,
                connected_tx: offer_connected_tx,
                packets_rx: None,
                runtime: runtime.clone(),
            }))
            .with_runtime(runtime.clone())
            .with_udp_addrs(vec!["127.0.0.1:0".to_owned()])
            .build()
            .await?,
    );

    let answerer: Arc<dyn PeerConnection> = Arc::new(
        PeerConnectionBuilder::new()
            .with_media_engine(media)
            .with_handler(Arc::new(TestHandler {
                gathered_tx: answer_gather_tx,
                connected_tx: answer_connected_tx,
                packets_rx: Some(receiver_packets.clone()),
                runtime: runtime.clone(),
            }))
            .with_runtime(runtime.clone())
            .with_udp_addrs(vec!["127.0.0.1:0".to_owned()])
            .build()
            .await?,
    );

    offerer.create_data_channel("data", None).await?;

    let audio_track = new_audio_track("stream-a", "audio-a", 0x4455_6677);
    let audio_sender = offerer
        .add_track(audio_track.clone() as Arc<dyn TrackLocal>)
        .await?;

    // Reserve additional receive sections (single-PC style).
    offerer
        .add_transceiver_from_kind(
            RtpCodecKind::Audio,
            Some(RTCRtpTransceiverInit {
                direction: RTCRtpTransceiverDirection::Recvonly,
                ..Default::default()
            }),
        )
        .await?;
    for _ in 0..3 {
        offerer
            .add_transceiver_from_kind(
                RtpCodecKind::Video,
                Some(RTCRtpTransceiverInit {
                    direction: RTCRtpTransceiverDirection::Recvonly,
                    ..Default::default()
                }),
            )
            .await?;
    }

    negotiate(
        &offerer,
        &answerer,
        &mut offer_gather_rx,
        &mut answer_gather_rx,
    )
    .await?;

    timeout(Duration::from_secs(10), offer_connected_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("offerer did not connect"))?
        .ok_or_else(|| anyhow::anyhow!("offerer connection channel closed"))?;
    timeout(Duration::from_secs(10), answer_connected_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("answerer did not connect"))?
        .ok_or_else(|| anyhow::anyhow!("answerer connection channel closed"))?;

    let sender_id = audio_sender.id();
    let mut sender_mid = None;
    for transceiver in offerer.get_transceivers().await {
        let Some(sender) = transceiver.sender().await? else {
            continue;
        };
        if sender.id() == sender_id {
            sender_mid = transceiver.mid().await?;
            break;
        }
    }
    let sender_mid = sender_mid.ok_or_else(|| anyhow::anyhow!("could not find sender mid"))?;

    for seq in 0..40u16 {
        audio_track
            .write_rtp_with_sdes_mid(
                Packet {
                    header: Header {
                        version: 2,
                        payload_type: 111,
                        sequence_number: seq,
                        timestamp: u32::from(seq) * 960,
                        ssrc: 0x4455_6677,
                        ..Default::default()
                    },
                    payload: bytes::Bytes::from_static(&[0xf8, 0xff, 0xfe]),
                },
                sender_mid.as_bytes(),
                false,
            )
            .await?;
        sleep(Duration::from_millis(10)).await;
    }
    wait_for_packets(&receiver_packets, 5).await?;

    // Renegotiate by reserving another recvonly audio section.
    offerer
        .add_transceiver_from_kind(
            RtpCodecKind::Audio,
            Some(RTCRtpTransceiverInit {
                direction: RTCRtpTransceiverDirection::Recvonly,
                ..Default::default()
            }),
        )
        .await?;
    negotiate(
        &offerer,
        &answerer,
        &mut offer_gather_rx,
        &mut answer_gather_rx,
    )
    .await?;

    let before = receiver_packets.load(Ordering::SeqCst);
    for seq in 100..140u16 {
        audio_track
            .write_rtp_with_sdes_mid(
                Packet {
                    header: Header {
                        version: 2,
                        payload_type: 111,
                        sequence_number: seq,
                        timestamp: u32::from(seq) * 960,
                        ssrc: 0x4455_6677,
                        ..Default::default()
                    },
                    payload: bytes::Bytes::from_static(&[0xf8, 0xff, 0xfe]),
                },
                sender_mid.as_bytes(),
                false,
            )
            .await?;
        sleep(Duration::from_millis(10)).await;
    }
    wait_for_packets(&receiver_packets, before + 5).await?;

    offerer.close().await?;
    answerer.close().await?;
    Ok(())
}
