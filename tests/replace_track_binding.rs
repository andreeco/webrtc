use anyhow::Result;
use rtc::media_stream::MediaStreamTrack;
use rtc::rtp::Packet as RtpPacket;
use rtc::rtp::header::Header as RtpHeader;
use rtc::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodingParameters, RTCRtpEncodingParameters, RtpCodecKind,
};
use std::sync::Arc;
use webrtc::media_stream::track_local::{TrackLocal, static_rtp::TrackLocalStaticRTP};
use webrtc::peer_connection::{
    MediaEngine, PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler,
    RTCConfigurationBuilder,
};

#[derive(Clone)]
struct TestHandler;

#[async_trait::async_trait]
impl PeerConnectionEventHandler for TestHandler {}

fn video_track(stream_id: &str, track_id: &str, ssrc: u32) -> Arc<TrackLocalStaticRTP> {
    Arc::new(TrackLocalStaticRTP::new(MediaStreamTrack::new(
        stream_id.to_owned(),
        track_id.to_owned(),
        track_id.to_owned(),
        RtpCodecKind::Video,
        vec![RTCRtpEncodingParameters {
            rtp_coding_parameters: RTCRtpCodingParameters {
                ssrc: Some(ssrc),
                ..Default::default()
            },
            codec: RTCRtpCodec {
                mime_type: "video/VP8".to_string(),
                clock_rate: 90_000,
                channels: 0,
                sdp_fmtp_line: String::new(),
                rtcp_feedback: vec![],
            },
            ..Default::default()
        }],
    )))
}

#[tokio::test]
async fn replace_track_binds_new_track_and_unbinds_old_track() -> Result<()> {
    let mut media_engine = MediaEngine::default();
    media_engine.register_default_codecs()?;

    let peer_connection = PeerConnectionBuilder::new()
        .with_configuration(RTCConfigurationBuilder::default().build())
        .with_media_engine(media_engine)
        .with_handler(Arc::new(TestHandler))
        .with_udp_addrs(vec!["127.0.0.1:0"])
        .build()
        .await?;

    let track_a = video_track("stream-a", "video-a", 0x1111_1111);
    let track_b = video_track("stream-b", "video-b", 0x2222_2222);

    let sender = peer_connection
        .add_track(Arc::clone(&track_a) as Arc<dyn TrackLocal>)
        .await?;

    let packet = RtpPacket {
        header: RtpHeader {
            version: 2,
            payload_type: 96,
            sequence_number: 1,
            timestamp: 1,
            ssrc: 0x2222_2222,
            ..Default::default()
        },
        payload: bytes::Bytes::from_static(&[0x01, 0x02, 0x03]),
        ..Default::default()
    };

    sender
        .replace_track(Arc::clone(&track_b) as Arc<dyn TrackLocal>)
        .await?;

    track_b
        .write_rtp(packet.clone())
        .await
        .expect("replacement track should be bound after replace_track");

    let old_track_write = track_a.write_rtp(packet).await;
    assert!(
        old_track_write.is_err(),
        "old track should be unbound after replace_track"
    );

    Ok(())
}
