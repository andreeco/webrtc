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

struct TrackLocalStaticRtpBinding {
    context: TrackLocalContext,
    mid_extension_id: Option<u8>,
}

/// TrackLocalStaticRTP  is a TrackLocal that has a pre-set codec and accepts RTP Packets.
/// If you wish to send a media.Sample use TrackLocalStaticSample
#[derive(Clone)]
pub struct TrackLocalStaticRTP {
    pub(crate) track: Mutex<MediaStreamTrack>,
    ctx: Mutex<Option<TrackLocalStaticRtpBinding>>,
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
                        .context
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
            (
                ctx.context.driver_event_tx.clone(),
                ctx.context.rtp_sender_id,
                ctx.mid_extension_id,
                ctx.context.prepared_rtp.clone(),
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
        let mid_extension_id = ctx
            .rtp_parameters
            .header_extensions
            .iter()
            .find(|extension| extension.uri == SDES_MID_URI)
            .map(|extension| extension.id as u8);
        let binding = matches!(
            bind_result,
            TrackLocalStaticRtpBindResult::Compatible { .. }
        )
        .then_some(TrackLocalStaticRtpBinding {
            context: ctx,
            mid_extension_id,
        });
        *self.bind_result.lock().await = bind_result;
        *self.ctx.lock().await = binding;
        *self.evt_rx.lock().await = Some(evt_rx);
    }

    async fn unbind(&self) {
        *self.bind_result.lock().await = TrackLocalStaticRtpBindResult::Pending;
        *self.ctx.lock().await = None;
        *self.evt_rx.lock().await = None;
    }

    async fn write_rtp(&self, packet: rtp::Packet) -> Result<()> {
        let ctx_opt = self.ctx.lock().await;
        if let Some(ctx) = &*ctx_opt {
            let tx = ctx.context.driver_event_tx.clone();
            let rtp_sender_id = ctx.context.rtp_sender_id;
            let prepared_rtp = ctx.context.prepared_rtp.clone();
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
            let tx = ctx.context.driver_event_tx.clone();
            let rtp_sender_id = ctx.context.rtp_sender_id;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media_stream::track_local::PreparedTrackLocalRtpContext;
    use crate::runtime::{block_on, channel};
    use rtc::media_stream::MediaStreamTrack;
    use rtc::rtp::header::Header;
    use rtc::rtp::packet::Packet;
    use rtc::rtp_transceiver::RTCRtpSenderId;
    use rtc::rtp_transceiver::rtp_sender::{
        RTCRtpHeaderExtensionParameters, RTCRtpParameters, RtpCodecKind,
    };

    fn track() -> TrackLocalStaticRTP {
        TrackLocalStaticRTP::new(MediaStreamTrack::new(
            "stream".to_owned(),
            "track".to_owned(),
            "track".to_owned(),
            RtpCodecKind::Video,
            vec![],
        ))
    }

    fn packet() -> Packet {
        Packet {
            header: Header {
                version: 2,
                payload_type: 96,
                sequence_number: 0x1234,
                timestamp: 0x0102_0304,
                ssrc: 0x0506_0708,
                ..Default::default()
            },
            payload: Bytes::from_static(&[0xaa, 0xbb]),
        }
    }

    fn context(
        sender_id: RTCRtpSenderId,
        mid_extension_id: u16,
        driver_event_tx: crate::runtime::Sender<PeerConnectionDriverEvent>,
    ) -> TrackLocalContext {
        TrackLocalContext {
            rtp_sender_id: sender_id,
            rtp_parameters: RTCRtpParameters {
                header_extensions: vec![RTCRtpHeaderExtensionParameters {
                    uri: SDES_MID_URI.to_owned(),
                    id: mid_extension_id,
                    encrypted: false,
                }],
                ..Default::default()
            },
            driver_event_tx,
            prepared_rtp: Some(PreparedTrackLocalRtpContext {
                rtp_sender_id: sender_id,
                ssrc: 0x1112_1314,
                payload_type: 97,
            }),
        }
    }

    async fn bind(
        track: &TrackLocalStaticRTP,
        sender_id: RTCRtpSenderId,
        mid_extension_id: u16,
    ) -> crate::runtime::Receiver<PeerConnectionDriverEvent> {
        let (driver_event_tx, driver_event_rx) = channel(2);
        let (_event_tx, event_rx) = channel(1);
        track
            .bind(
                context(sender_id, mid_extension_id, driver_event_tx),
                event_rx,
            )
            .await;
        driver_event_rx
    }

    fn sender_packet(event: PeerConnectionDriverEvent) -> Packet {
        match event {
            PeerConnectionDriverEvent::SenderRtpPrepared { packet, .. }
            | PeerConnectionDriverEvent::SenderRtp(_, packet) => packet,
            event => panic!("expected RTP event, got {event:?}"),
        }
    }

    #[test]
    fn sdes_mid_injection_has_exact_rtp_output() {
        block_on(async {
            let track = track();
            let mut events = bind(&track, RTCRtpSenderId::default(), 4).await;

            track
                .write_rtp_with_sdes_mid(packet(), b"a", false)
                .await
                .expect("MID write should queue an RTP packet");

            let packet = sender_packet(events.recv().await.expect("queued RTP event"));
            let mut actual = vec![0; packet.marshal_size()];
            packet
                .marshal_to(&mut actual)
                .expect("packet should marshal");

            assert_eq!(
                actual,
                [
                    0x90, 0x60, 0x12, 0x34, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0xbe,
                    0xde, 0x00, 0x01, 0x40, b'a', 0x00, 0x00, 0xaa, 0xbb,
                ]
            );
        });
    }

    #[test]
    fn sdes_mid_uses_the_extension_id_cached_at_bind_time() {
        block_on(async {
            let track = track();
            let mut events = bind(&track, RTCRtpSenderId::default(), 4).await;
            track
                .ctx
                .lock()
                .await
                .as_mut()
                .expect("bound context")
                .context
                .rtp_parameters
                .header_extensions[0]
                .id = 9;

            track
                .write_rtp_with_sdes_mid(packet(), b"mid", false)
                .await
                .expect("MID write should queue an RTP packet");

            let packet = sender_packet(events.recv().await.expect("queued RTP event"));
            assert_eq!(packet.header.extensions[0].id, 4);
        });
    }

    #[test]
    fn sdes_mid_write_delivers_to_multiple_independently_bound_targets() {
        block_on(async {
            let first_track = track();
            let second_track = track();
            let mut first_events = bind(&first_track, RTCRtpSenderId::default(), 4).await;
            let mut second_events = bind(&second_track, RTCRtpSenderId::default(), 9).await;

            first_track
                .write_rtp_with_sdes_mid(packet(), b"target", false)
                .await
                .expect("first target write should queue RTP");
            second_track
                .write_rtp_with_sdes_mid(packet(), b"target", false)
                .await
                .expect("second target write should queue RTP");

            let first = sender_packet(first_events.recv().await.expect("first target RTP event"));
            let second =
                sender_packet(second_events.recv().await.expect("second target RTP event"));
            assert_eq!(first.header.extensions[0].id, 4);
            assert_eq!(second.header.extensions[0].id, 9);
        });
    }

    #[test]
    fn sdes_mid_write_keeps_source_and_target_packets_independent() {
        block_on(async {
            let first_track = track();
            let second_track = track();
            let mut first_events = bind(&first_track, RTCRtpSenderId::default(), 4).await;
            let mut second_events = bind(&second_track, RTCRtpSenderId::default(), 9).await;
            let source = packet();
            let source_before_write = source.clone();

            first_track
                .write_rtp_with_sdes_mid(source.clone(), b"owned", false)
                .await
                .expect("first target write should queue RTP");
            second_track
                .write_rtp_with_sdes_mid(source.clone(), b"owned", false)
                .await
                .expect("second target write should queue RTP");

            assert_eq!(
                source, source_before_write,
                "write must not mutate the caller packet"
            );

            let mut first =
                sender_packet(first_events.recv().await.expect("first target RTP event"));
            let second =
                sender_packet(second_events.recv().await.expect("second target RTP event"));
            first.header.extensions[0].payload = Bytes::from_static(b"changed");

            assert_eq!(
                second.header.extensions[0].payload,
                Bytes::from_static(b"owned")
            );
            assert_eq!(second.payload, source.payload);
        });
    }
}
