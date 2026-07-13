//! Local Media Stream Tracks
//!
//! This module provides the [`TrackLocal`](crate::media_stream::track_local::TrackLocal) trait, which represents a media track generated locally.
//! It also includes two concrete implementations:
//! *   **[`TrackLocalStaticRTP`](crate::media_stream::track_local::static_rtp::TrackLocalStaticRTP)**: For writing pre-packetized RTP packets.
//! *   **[`TrackLocalStaticSample`](crate::media_stream::track_local::static_sample::TrackLocalStaticSample)**: For writing raw media samples.
//!
//! # Examples
//!
//! ## Writing Media Samples
//!
//! ```no_run
//! use webrtc::media_stream::track_local::TrackLocal;
//! use webrtc::media_stream::track_local::static_sample::TrackLocalStaticSample;
//! use rtc::media_stream::{MediaStreamTrack, MediaStreamTrackId, MediaStreamId};
//! use rtc::rtp_transceiver::rtp_sender::RtpCodecKind;
//! use rtc::media::Sample;
//! use std::time::Duration;
//! use std::sync::Arc;
//!
//! # async fn example() -> webrtc::error::Result<()> {
//! // Create a local video track
//! let track = MediaStreamTrack::new(
//!     MediaStreamTrackId::new(),
//!     MediaStreamId::new(),
//!     "video-label".to_owned(),
//!     RtpCodecKind::Video,
//!     vec![],
//! );
//! let local_track = Arc::new(TrackLocalStaticSample::new(track)?);
//!
//! // Write a raw VP8/H.264 frame as a sample
//! let sample = Sample {
//!     data: bytes::Bytes::from(vec![0x00, 0x01, 0x02]),
//!     duration: Duration::from_millis(33), // ~30 fps
//!     ..Default::default()
//! };
//!
//! // Write the sample to SSRC 1234
//! local_track.write_sample(1234, &sample, &[]).await?;
//! # Ok(())
//! # }
//! ```

use crate::error::Result;
use crate::media_stream::Track;
use crate::peer_connection::driver::PeerConnectionDriverEvent;
use crate::runtime::Sender;
use rtc::media_stream::MediaStreamTrack;
use rtc::rtp_transceiver::rtp_sender::RTCRtpParameters;
use rtc::rtp_transceiver::{PayloadType, RTCRtpSenderId, SSRC};
use rtc::{rtcp, rtp};

/// Local track implementation that accepts pre-packetized RTP packets.
pub mod static_rtp;
/// Local track implementation that accepts raw media samples and packetizes them.
pub mod static_sample;

/// TrackLocalContext is the Context passed when a TrackLocal has been Binded/Unbinded from a PeerConnection, and used
/// in Interceptors.
/// Pre-resolved RTP sender values for hot-path packet forwarding.
#[derive(Clone)]
pub struct PreparedTrackLocalRtpContext {
    pub(crate) rtp_sender_id: RTCRtpSenderId,
    pub(crate) ssrc: SSRC,
    pub(crate) payload_type: PayloadType,
}

/// Context passed to a bound local track with negotiated sender state.
#[derive(Clone)]
pub struct TrackLocalContext {
    pub(crate) rtp_sender_id: RTCRtpSenderId,
    pub(crate) rtp_parameters: RTCRtpParameters,
    pub(crate) driver_event_tx: Sender<PeerConnectionDriverEvent>,
    pub(crate) prepared_rtp: Option<PreparedTrackLocalRtpContext>,
}

impl TrackLocalContext {
    pub(crate) fn build_prepared_rtp(
        rtp_sender_id: RTCRtpSenderId,
        rtp_parameters: &RTCRtpParameters,
        codings: &[rtc::rtp_transceiver::rtp_sender::RTCRtpEncodingParameters],
    ) -> Option<PreparedTrackLocalRtpContext> {
        let first_coding = codings.first()?;
        let ssrc = first_coding.rtp_coding_parameters.ssrc?;
        let payload_type = rtp_parameters
            .codecs
            .iter()
            .find(|codec| {
                codec
                    .rtp_codec
                    .mime_type
                    .eq_ignore_ascii_case(&first_coding.codec.mime_type)
                    && codec.rtp_codec.clock_rate == first_coding.codec.clock_rate
                    && codec.rtp_codec.channels == first_coding.codec.channels
                    && codec.rtp_codec.sdp_fmtp_line == first_coding.codec.sdp_fmtp_line
            })
            .or_else(|| {
                rtp_parameters.codecs.iter().find(|codec| {
                    codec
                        .rtp_codec
                        .mime_type
                        .eq_ignore_ascii_case(&first_coding.codec.mime_type)
                })
            })?
            .payload_type;

        Some(PreparedTrackLocalRtpContext {
            rtp_sender_id,
            ssrc,
            payload_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::TrackLocalContext;
    use rtc::rtp_transceiver::RTCRtpSenderId;
    use rtc::rtp_transceiver::rtp_sender::{
        RTCRtpCodec, RTCRtpCodecParameters, RTCRtpCodingParameters, RTCRtpEncodingParameters,
        RTCRtpParameters,
    };

    fn codec(mime_type: &str) -> RTCRtpCodec {
        RTCRtpCodec {
            mime_type: mime_type.to_string(),
            clock_rate: 90_000,
            channels: 0,
            sdp_fmtp_line: String::new(),
            rtcp_feedback: vec![],
        }
    }

    #[test]
    fn prepared_rtp_uses_the_local_encoding_codec_payload_type() {
        let parameters = RTCRtpParameters {
            codecs: vec![
                RTCRtpCodecParameters {
                    rtp_codec: codec("video/h264"),
                    payload_type: 125,
                    ..Default::default()
                },
                RTCRtpCodecParameters {
                    rtp_codec: codec("video/vp8"),
                    payload_type: 96,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let codings = vec![RTCRtpEncodingParameters {
            rtp_coding_parameters: RTCRtpCodingParameters {
                ssrc: Some(1234),
                ..Default::default()
            },
            codec: codec("video/vp8"),
            ..Default::default()
        }];

        let prepared =
            TrackLocalContext::build_prepared_rtp(RTCRtpSenderId::default(), &parameters, &codings)
                .expect("prepared RTP context should be available");

        assert_eq!(prepared.payload_type, 96);
    }

    #[test]
    fn prepared_rtp_rejects_an_unrelated_negotiated_codec() {
        let parameters = RTCRtpParameters {
            codecs: vec![RTCRtpCodecParameters {
                rtp_codec: codec("video/vp8"),
                payload_type: 96,
                ..Default::default()
            }],
            ..Default::default()
        };
        let codings = vec![RTCRtpEncodingParameters {
            rtp_coding_parameters: RTCRtpCodingParameters {
                ssrc: Some(1234),
                ..Default::default()
            },
            codec: codec("video/h264"),
            ..Default::default()
        }];

        assert!(
            TrackLocalContext::build_prepared_rtp(
                RTCRtpSenderId::default(),
                &parameters,
                &codings,
            )
            .is_none(),
            "an H264 source must not bind to a VP8-only negotiated context"
        );
    }
}

/// A local media track that can be sent to a remote peer.
///
/// This trait defines the interface for local media tracks. Applications write
/// RTP and RTCP packets to this track, which are then processed by the interceptor
/// pipeline and sent over the peer connection.
#[async_trait::async_trait]
pub trait TrackLocal: Track {
    /// Returns the underlying [`MediaStreamTrack`] for this local track.
    async fn track(&self) -> MediaStreamTrack;

    /// Binds the track to the peer connection context.
    ///
    /// This will be called internally after signaling is complete and the list of available
    /// codecs has been determined.
    async fn bind(&self, ctx: TrackLocalContext);

    /// Unbinds the track from the peer connection context, cleaning up any resources.
    async fn unbind(&self);

    /// Writes an RTP packet to the track.
    async fn write_rtp(&self, packet: rtp::Packet) -> Result<()>;

    /// Writes RTCP packets to the track.
    async fn write_rtcp(&self, packets: Vec<Box<dyn rtcp::Packet>>) -> Result<()>;
}
