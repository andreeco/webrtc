use crate::error::{Error, Result};
use crate::media_stream::Track;
use crate::media_stream::track_local::{TrackLocal, TrackLocalContext, TrackLocalEvent};
use crate::peer_connection::driver::PeerConnectionDriverEvent;
use crate::runtime::{Mutex, Receiver};
use bytes::{Bytes, BytesMut};
use rtc::media_stream::{
    MediaStreamId, MediaStreamTrack, MediaStreamTrackId, MediaStreamTrackState,
    MediaTrackCapabilities, MediaTrackConstraints, MediaTrackSettings,
};
use rtc::rtp_transceiver::rtp_sender::{RTCRtpCodec, RTCRtpEncodingParameters, RtpCodecKind};
use rtc::rtp_transceiver::{PayloadType, RtpStreamId, SSRC};
use rtc::shared::error::flatten_errs;
use rtc::shared::marshal::{Marshal, MarshalSize};
use rtc::{rtcp, rtp};
use std::collections::HashMap;

const SDES_MID_URI: &str = "urn:ietf:params:rtp-hdrext:sdes:mid";

/// Outcome of binding a static RTP track to a negotiated sender context.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrackLocalStaticRtpBindResult {
    /// No sender context has bound this track.
    Pending,
    /// The negotiated context has a compatible codec and selected payload type.
    Compatible {
        /// Payload type selected from the remote peer's negotiated codec parameters.
        payload_type: PayloadType,
    },
    /// The negotiated context contains no compatible codec.
    UnsupportedCodec,
}

/// TrackLocalStaticRTP  is a TrackLocal that has a pre-set codec and accepts RTP Packets.
/// If you wish to send a media.Sample use TrackLocalStaticSample
#[derive(Clone)]
pub struct TrackLocalStaticRTP {
    pub(crate) track: Mutex<MediaStreamTrack>,
    pub(crate) ctx: Mutex<Option<TrackLocalContext>>,
    /// Delivers RTCP feedback received about this sent track (set on bind).
    pub(crate) evt_rx: Mutex<Option<Receiver<TrackLocalEvent>>>,
    bind_result: Mutex<TrackLocalStaticRtpBindResult>,
}

impl TrackLocalStaticRTP {
    /// Creates a new `TrackLocalStaticRTP` with the given [`MediaStreamTrack`].
    pub fn new(track: MediaStreamTrack) -> Self {
        Self {
            track: Mutex::new(track),
            ctx: Mutex::new(None),
            evt_rx: Mutex::new(None),
            bind_result: Mutex::new(TrackLocalStaticRtpBindResult::Pending),
        }
    }

    /// Returns the most recent negotiated sender binding outcome.
    pub async fn bind_result(&self) -> TrackLocalStaticRtpBindResult {
        self.bind_result.lock().await.clone()
    }

    /// Writes an RTP packet to the track with the specified header extensions.
    pub async fn write_rtp_with_extensions(
        &self,
        mut pkt: rtp::Packet,
        extensions: &[rtp::extension::HeaderExtension],
    ) -> Result<()> {
        let mut write_errs = vec![];

        // Prepare the extensions data
        let extension_data: HashMap<_, _> = extensions
            .iter()
            .flat_map(|extension| {
                let buf = {
                    let mut buf = BytesMut::with_capacity(extension.marshal_size());
                    buf.resize(extension.marshal_size(), 0);
                    if let Err(err) = extension.marshal_to(&mut buf) {
                        write_errs.push(err);
                        return None;
                    }

                    buf.freeze()
                };

                Some((extension.uri(), buf))
            })
            .collect();

        {
            let ctx = self.ctx.lock().await;
            if let Some(ctx) = &*ctx {
                for (uri, data) in extension_data.iter() {
                    if let Some(id) = ctx
                        .rtp_parameters
                        .header_extensions
                        .iter()
                        .find(|ext| &ext.uri == uri)
                        .map(|ext| ext.id)
                        && let Err(err) = pkt.header.set_extension(id as u8, data.clone())
                    {
                        write_errs.push(err);
                        continue;
                    }
                }
            } else {
                return Err(Error::ErrBindFailed);
            }
        }

        if let Err(err) = self.write_rtp(pkt).await {
            write_errs.push(err);
        }

        flatten_errs(write_errs)
    }

    /// Writes an RTP packet with SDES MID extension set to the negotiated MID extension ID.
    ///
    /// This is a fast path used in SFU forwarding to avoid per-packet extension trait-object
    /// marshalling/allocation when only MID needs to be injected.
    pub async fn write_rtp_with_sdes_mid(
        &self,
        pkt: rtp::Packet,
        mid: &[u8],
        preserve_existing_extensions: bool,
    ) -> Result<()> {
        self.write_rtp_with_sdes_mid_bytes(
            pkt,
            Bytes::copy_from_slice(mid),
            preserve_existing_extensions,
        )
        .await
    }

    /// Writes an RTP packet with SDES MID extension using a reusable MID byte buffer.
    pub async fn write_rtp_with_sdes_mid_bytes(
        &self,
        mut pkt: rtp::Packet,
        mid: Bytes,
        preserve_existing_extensions: bool,
    ) -> Result<()> {
        if !preserve_existing_extensions {
            pkt.header.extension = false;
            pkt.header.extension_profile = 0;
            pkt.header.extensions.clear();
            pkt.header.extensions_padding = 0;
        }

        let (tx, rtp_sender_id, mid_ext_id, prepared_rtp) = {
            let ctx = self.ctx.lock().await;
            let Some(ctx) = &*ctx else {
                return Err(Error::ErrBindFailed);
            };
            let mid_ext_id = ctx
                .rtp_parameters
                .header_extensions
                .iter()
                .find(|ext| ext.uri == SDES_MID_URI)
                .map(|ext| ext.id as u8);
            (
                ctx.driver_event_tx.clone(),
                ctx.rtp_sender_id,
                mid_ext_id,
                ctx.prepared_rtp.clone(),
            )
        };

        if let Some(id) = mid_ext_id {
            pkt.header
                .set_extension(id, mid)
                .map_err(|e| Error::Other(format!("{:?}", e)))?;
        }

        let event = if let Some(prepared) = prepared_rtp {
            PeerConnectionDriverEvent::SenderRtpPrepared {
                sender_id: prepared.rtp_sender_id,
                packet: pkt,
                ssrc: prepared.ssrc,
                payload_type: prepared.payload_type,
            }
        } else {
            PeerConnectionDriverEvent::SenderRtp(rtp_sender_id, pkt)
        };

        tx.send(event)
            .await
            .map_err(|e| Error::Other(format!("{:?}", e)))
    }
}

#[async_trait::async_trait]
impl Track for TrackLocalStaticRTP {
    async fn stream_id(&self) -> MediaStreamId {
        let track = self.track.lock().await;
        track.stream_id().to_owned()
    }

    async fn track_id(&self) -> MediaStreamTrackId {
        let track = self.track.lock().await;
        track.track_id().to_owned()
    }

    async fn label(&self) -> String {
        let track = self.track.lock().await;
        track.label().to_owned()
    }

    async fn kind(&self) -> RtpCodecKind {
        let track = self.track.lock().await;
        track.kind()
    }

    async fn rid(&self, ssrc: SSRC) -> Option<RtpStreamId> {
        let track = self.track.lock().await;
        track.rid(ssrc).cloned()
    }

    async fn codec(&self, ssrc: SSRC) -> Option<RTCRtpCodec> {
        let track = self.track.lock().await;
        track.codec(ssrc).cloned()
    }

    async fn ssrcs(&self) -> Vec<SSRC> {
        let track = self.track.lock().await;
        track.ssrcs().collect()
    }

    async fn enabled(&self) -> bool {
        let track = self.track.lock().await;
        track.enabled()
    }

    async fn set_enabled(&self, enabled: bool) {
        let mut track = self.track.lock().await;
        track.set_enabled(enabled);
    }

    async fn muted(&self) -> bool {
        let track = self.track.lock().await;
        track.muted()
    }

    async fn set_muted(&self, muted: bool) {
        let mut track = self.track.lock().await;
        track.set_muted(muted);
    }

    async fn ready_state(&self) -> MediaStreamTrackState {
        let track = self.track.lock().await;
        track.ready_state()
    }

    async fn stop(&self) {
        let mut track = self.track.lock().await;
        track.stop();
    }

    async fn get_capabilities(&self) -> MediaTrackCapabilities {
        let track = self.track.lock().await;
        track.get_capabilities().clone()
    }

    async fn get_constraints(&self) -> MediaTrackConstraints {
        let track = self.track.lock().await;
        track.get_constraints().clone()
    }

    async fn get_settings(&self) -> MediaTrackSettings {
        let track = self.track.lock().await;
        track.get_settings().clone()
    }

    async fn apply_constraints(&self, constraints: Option<MediaTrackConstraints>) {
        let mut track = self.track.lock().await;
        track.apply_constraints(constraints);
    }

    async fn codings(&self) -> Vec<RTCRtpEncodingParameters> {
        let track = self.track.lock().await;
        track.codings().to_vec()
    }

    async fn add_coding(&self, coding: RTCRtpEncodingParameters) {
        let mut track = self.track.lock().await;
        track.add_coding(coding);
    }
}

#[async_trait::async_trait]
impl TrackLocal for TrackLocalStaticRTP {
    async fn track(&self) -> MediaStreamTrack {
        let track = self.track.lock().await;
        track.clone()
    }

    async fn bind(&self, ctx: TrackLocalContext, evt_rx: Receiver<TrackLocalEvent>) {
        let bind_result = match ctx.prepared_rtp.as_ref() {
            Some(prepared) => TrackLocalStaticRtpBindResult::Compatible {
                payload_type: prepared.payload_type,
            },
            None => TrackLocalStaticRtpBindResult::UnsupportedCodec,
        };
        let is_compatible = matches!(
            bind_result,
            TrackLocalStaticRtpBindResult::Compatible { .. }
        );
        *self.bind_result.lock().await = bind_result;
        *self.ctx.lock().await = is_compatible.then_some(ctx);
        *self.evt_rx.lock().await = Some(evt_rx);
    }

    async fn unbind(&self) {
        *self.bind_result.lock().await = TrackLocalStaticRtpBindResult::Pending;
        *self.ctx.lock().await = None;
        *self.evt_rx.lock().await = None;
    }

    }

    async fn write_rtp(&self, packet: rtp::Packet) -> Result<()> {
        let ctx_opt = self.ctx.lock().await;
        if let Some(ctx) = &*ctx_opt {
            let tx = ctx.driver_event_tx.clone();
            let rtp_sender_id = ctx.rtp_sender_id;
            let prepared_rtp = ctx.prepared_rtp.clone();
            drop(ctx_opt);
            let event = if let Some(prepared) = prepared_rtp {
                PeerConnectionDriverEvent::SenderRtpPrepared {
                    sender_id: prepared.rtp_sender_id,
                    packet,
                    ssrc: prepared.ssrc,
                    payload_type: prepared.payload_type,
                }
            } else {
                PeerConnectionDriverEvent::SenderRtp(rtp_sender_id, packet)
            };
            tx.send(event)
                .await
                .map_err(|e| Error::Other(format!("{:?}", e)))
        } else {
            Err(Error::Other("track is not binding yet".to_string()))
        }
    }

    async fn write_rtcp(&self, packets: Vec<Box<dyn rtcp::Packet>>) -> Result<()> {
        let ctx_opt = self.ctx.lock().await;
        if let Some(ctx) = &*ctx_opt {
            let tx = ctx.driver_event_tx.clone();
            let rtp_sender_id = ctx.rtp_sender_id;
            drop(ctx_opt);
            tx.send(PeerConnectionDriverEvent::SenderRtcp(
                rtp_sender_id,
                packets,
            ))
            .await
            .map_err(|e| Error::Other(format!("{:?}", e)))
        } else {
            Err(Error::Other("track is not binding yet".to_string()))
        }
    }

    async fn poll(&self) -> Option<TrackLocalEvent> {
        let mut guard = self.evt_rx.lock().await;
        match guard.as_mut() {
            Some(rx) => rx.recv().await,
            None => None,
        }
    }
}
