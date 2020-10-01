use crate::error::BitswapError;
use crate::message::BitswapMessage;
use crate::protocol::{BitswapProtocolConfig, BitswapStreamSink};
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use libp2p::core::upgrade::{InboundUpgrade, OutboundUpgrade, UpgradeError};
use libp2p::swarm::protocols_handler::{
    KeepAlive, ProtocolsHandler, ProtocolsHandlerEvent, ProtocolsHandlerUpgrErr,
};
use libp2p::swarm::{NegotiatedSubstream, SubstreamProtocol};
use std::collections::VecDeque;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tiny_multihash::{MultihashCode, U64};

#[derive(Clone, Debug)]
pub struct BitswapHandlerConfig<MH> {
    /// Configuration for the wire protocol.
    pub protocol_config: BitswapProtocolConfig<MH>,
    /// Time after which we close an idle connection.
    pub idle_timeout: Duration,
}

pub struct BitswapHandler<MH> {
    /// Configuration for the Bitswap protocol.
    config: BitswapHandlerConfig<MH>,
    /// Keep alive.
    keep_alive: KeepAlive,
    /// Open outbound.
    open_outbound: bool,
    /// Pending outbound.
    outbound: VecDeque<BitswapMessage>,
    /// Outbound stream.
    outbound_stream: Option<BitswapStreamSink<NegotiatedSubstream>>,
    /// Inbound stream.
    inbound_stream: Option<BitswapStreamSink<NegotiatedSubstream>>,
    /// Pending error.
    pending_error: Option<ProtocolsHandlerUpgrErr<BitswapError>>,
}

impl<MH> BitswapHandler<MH> {
    /// Create a [`BitswapHandler`] using the given configuration.
    pub fn new(config: BitswapHandlerConfig<MH>) -> Self {
        Self {
            config,
            keep_alive: KeepAlive::Yes,
            open_outbound: false,
            outbound: Default::default(),
            outbound_stream: None,
            inbound_stream: None,
            pending_error: None,
        }
    }
}

impl<MH: MultihashCode<AllocSize = U64>> ProtocolsHandler for BitswapHandler<MH> {
    type InEvent = BitswapMessage;
    type OutEvent = BitswapMessage;
    type Error = ProtocolsHandlerUpgrErr<BitswapError>;
    type InboundProtocol = BitswapProtocolConfig<MH>;
    type OutboundProtocol = BitswapProtocolConfig<MH>;
    type OutboundOpenInfo = ();
    type InboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(self.config.protocol_config, ())
            .with_timeout(self.config.idle_timeout)
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        protocol: <Self::OutboundProtocol as OutboundUpgrade<NegotiatedSubstream>>::Output,
        _: Self::OutboundOpenInfo,
    ) {
        assert!(self.outbound_stream.is_none());
        self.outbound_stream = Some(protocol);
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        protocol: <Self::InboundProtocol as InboundUpgrade<NegotiatedSubstream>>::Output,
        _: Self::InboundOpenInfo,
    ) {
        assert!(self.inbound_stream.is_none());
        self.inbound_stream = Some(protocol);
    }

    fn inject_dial_upgrade_error(
        &mut self,
        _: Self::OutboundOpenInfo,
        err: ProtocolsHandlerUpgrErr<BitswapError>,
    ) {
        self.pending_error = Some(err);
    }

    fn inject_listen_upgrade_error(
        &mut self,
        _: Self::InboundOpenInfo,
        err: ProtocolsHandlerUpgrErr<BitswapError>,
    ) {
        self.pending_error = Some(err);
    }

    fn inject_event(&mut self, message: BitswapMessage) {
        self.keep_alive = KeepAlive::Yes;
        self.outbound.push_back(message);
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        self.keep_alive
    }

    #[allow(clippy::type_complexity)]
    fn poll(
        &mut self,
        cx: &mut Context,
    ) -> Poll<
        ProtocolsHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::OutEvent,
            Self::Error,
        >,
    > {
        // Check for a fatal error.
        if let Some(err) = self.pending_error.take() {
            // The handler will not be polled again by the `Swarm`.
            return Poll::Ready(ProtocolsHandlerEvent::Close(err));
        }

        // Open an outbound stream if we haven't yet.
        if !self.open_outbound && self.outbound_stream.is_none() {
            self.open_outbound = true;
            return Poll::Ready(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(self.config.protocol_config, ())
                    .with_timeout(self.config.idle_timeout),
            });
        }

        // Poll the inbound stream.
        if let Some(ch) = self.inbound_stream.as_mut() {
            if let Poll::Ready(Some(result)) = ch.poll_next_unpin(cx) {
                match result {
                    Ok(message) => {
                        self.keep_alive = KeepAlive::Yes;
                        return Poll::Ready(ProtocolsHandlerEvent::Custom(message));
                    }
                    Err(err) => {
                        return Poll::Ready(ProtocolsHandlerEvent::Close(error(err)));
                    }
                }
            }
        }

        // Poll the outbound stream.
        if let Some(ch) = self.outbound_stream.as_mut() {
            while let Poll::Ready(result) = ch.poll_ready_unpin(cx) {
                match result {
                    Ok(()) => {
                        if let Some(message) = self.outbound.pop_front() {
                            if let Err(err) = ch.start_send_unpin(message) {
                                return Poll::Ready(ProtocolsHandlerEvent::Close(error(err)));
                            }
                        } else {
                            break;
                        }
                    }
                    Err(err) => {
                        return Poll::Ready(ProtocolsHandlerEvent::Close(error(err)));
                    }
                }
            }
            let _ = ch.poll_flush_unpin(cx);
        }

        if self.outbound.is_empty() && self.keep_alive.is_yes() {
            // No new inbound or outbound requests.
            let until = Instant::now() + self.config.idle_timeout;
            self.keep_alive = KeepAlive::Until(until);
        }

        Poll::Pending
    }
}

fn error(err: BitswapError) -> ProtocolsHandlerUpgrErr<BitswapError> {
    ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Apply(err))
}
