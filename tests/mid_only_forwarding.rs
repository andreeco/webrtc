//! Regression coverage for receiving a non-simulcast forwarding stream identified only by MID.

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

struct ReceiverHandler {
    gathered: Sender<()>,
    connected: Sender<()>,
    packets: Arc<AtomicUsize>,
    runtime: Arc<dyn Runtime>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for ReceiverHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gathered.try_send(());
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        if state == RTCPeerConnectionState::Connected {
            let _ = self.connected.try_send(());
        }
    }

    async fn on_track(&self, track: Arc<dyn TrackRemote>) {
        let packets = self.packets.clone();
        self.runtime.spawn(Box::pin(async move {
            while let Some(event) = track.poll().await {
                if matches!(event, TrackRemoteEvent::OnRtpPacket(_)) {
                    packets.fetch_add(1, Ordering::SeqCst);
                    break;
                }
            }
        }));
    }
}

fn audio_track() -> Arc<TrackLocalStaticRTP> {
    Arc::new(TrackLocalStaticRTP::new(MediaStreamTrack::new(
        "forwarded-stream".to_owned(),
        "forwarded-track".to_owned(),
        "forwarded-track".to_owned(),
        RtpCodecKind::Audio,
        vec![RTCRtpEncodingParameters {
            rtp_coding_parameters: RTCRtpCodingParameters {
                // The receiver must learn this at runtime: it is intentionally absent from SDP.
                ssrc: Some(0x4444_5555),
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

#[test]
fn forwarding_rtp_with_mid_and_no_rid_opens_remote_track() {
    block_on(run_test()).expect("MID-only forwarding stream should be received");
}

async fn run_test() -> anyhow::Result<()> {
    let runtime = default_runtime().ok_or_else(|| std::io::Error::other("no async runtime"))?;
    let mut media = MediaEngine::default();
    media.register_default_codecs()?;

    let packets = Arc::new(AtomicUsize::new(0));
    let (offer_gathered_tx, mut offer_gathered_rx) = channel(1);
    let (answer_gathered_tx, mut answer_gathered_rx) = channel(1);
    let (offer_connected_tx, mut offer_connected_rx) = channel(1);
    let (answer_connected_tx, mut answer_connected_rx) = channel(1);

    let offerer = Arc::new(
        PeerConnectionBuilder::new()
            .with_media_engine(media.clone())
            .with_runtime(runtime.clone())
            .with_handler(Arc::new(ReceiverHandler {
                gathered: offer_gathered_tx,
                connected: offer_connected_tx,
                packets: packets.clone(),
                runtime: runtime.clone(),
            }))
            .with_udp_addrs(vec!["127.0.0.1:0".to_owned()])
            .build()
            .await?,
    );
    let answerer = Arc::new(
        PeerConnectionBuilder::new()
            .with_media_engine(media)
            .with_runtime(runtime.clone())
            .with_handler(Arc::new(ReceiverHandler {
                gathered: answer_gathered_tx,
                connected: answer_connected_tx,
                packets: Arc::new(AtomicUsize::new(0)),
                runtime: runtime.clone(),
            }))
            .with_udp_addrs(vec!["127.0.0.1:0".to_owned()])
            .build()
            .await?,
    );

    offerer.create_data_channel("data", None).await?;
    // Reserve two same-kind receive sections. The forwarding sender must bind to
    // the second one rather than whichever sender-less audio transceiver comes first.
    for _ in 0..2 {
        offerer
            .add_transceiver_from_kind(
                RtpCodecKind::Audio,
                Some(RTCRtpTransceiverInit {
                    direction: RTCRtpTransceiverDirection::Recvonly,
                    ..Default::default()
                }),
            )
            .await?;
    }
    let offer = offerer.create_offer(None).await?;
    offerer.set_local_description(offer).await?;
    let _ = timeout(Duration::from_secs(5), offer_gathered_rx.recv()).await;
    answerer
        .set_remote_description(
            offerer
                .local_description()
                .await
                .expect("offer description"),
        )
        .await?;

    let track = audio_track();
    answerer
        .add_track_to_mid("1", track.clone() as Arc<dyn TrackLocal>)
        .await?;
    let transceiver = answerer
        .get_transceivers()
        .await
        .into_iter()
        .find(|transceiver| {
            futures::executor::block_on(transceiver.mid())
                .ok()
                .flatten()
                .as_deref()
                == Some("1")
        })
        .ok_or_else(|| anyhow::anyhow!("missing offered audio transceiver"))?;
    transceiver
        .set_direction(RTCRtpTransceiverDirection::Sendonly)
        .await?;
    let answer = answerer.create_answer(None).await?;
    answerer.set_local_description(answer).await?;
    let _ = timeout(Duration::from_secs(5), answer_gathered_rx.recv()).await;
    offerer
        .set_remote_description(
            answerer
                .local_description()
                .await
                .expect("answer description"),
        )
        .await?;

    timeout(Duration::from_secs(15), offer_connected_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("offerer did not connect"))?
        .ok_or_else(|| anyhow::anyhow!("offerer connection channel closed"))?;
    timeout(Duration::from_secs(15), answer_connected_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("answerer did not connect"))?
        .ok_or_else(|| anyhow::anyhow!("answerer connection channel closed"))?;
    for sequence_number in 0..20 {
        track
            .write_rtp_with_sdes_mid(
                Packet {
                    header: Header {
                        version: 2,
                        payload_type: 111,
                        sequence_number,
                        timestamp: u32::from(sequence_number) * 960,
                        ssrc: 0x4444_5555,
                        ..Default::default()
                    },
                    payload: bytes::Bytes::from_static(&[0xf8, 0xff, 0xfe]),
                },
                b"1",
                false,
            )
            .await?;
        sleep(Duration::from_millis(10)).await;
    }
    timeout(Duration::from_secs(5), async {
        while packets.load(Ordering::SeqCst) == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("no forwarded RTP packet received"))?;
    offerer.close().await?;
    answerer.close().await?;
    Ok(())
}
