//! Handles the `/ipfs/bitswap/1.0.0` and `/ipfs/bitswap/1.1.0` protocols. This
//! allows exchanging IPFS blocks.
//!
//! # Usage
//!
//! The `Bitswap` struct implements the `NetworkBehaviour` trait. When used, it
//! will allow providing and reciving IPFS blocks.
use crate::handler::{BitswapHandler, BitswapHandlerConfig};
use crate::message::BitswapMessage;
use crate::protocol::BitswapProtocolConfig;
use cuckoofilter::{CuckooError, CuckooFilter};
use fnv::{FnvHashSet, FnvHasher};
use futures::task::Context;
use futures::task::Poll;
use libp2p::core::connection::ConnectionId;
use libp2p::core::{Multiaddr, PeerId};
use libp2p::swarm::protocols_handler::{IntoProtocolsHandler, ProtocolsHandler};
use libp2p::swarm::{NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, PollParameters};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::time::Duration;
use tiny_cid::Cid;
use tiny_multihash::MultihashDigest;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BitswapEvent {
    Have {
        peer_id: PeerId,
        cid: Cid,
        have: bool,
    },
    Want {
        peer_id: PeerId,
        cid: Cid,
    },
    Block {
        peer_id: PeerId,
        cid: Cid,
        data: Vec<u8>,
    },
}

/// Bitswap configuration.
pub struct BitswapConfig {
    pub max_packet_size: usize,
    pub connection_idle_timeout: Duration,
}

impl Default for BitswapConfig {
    fn default() -> Self {
        Self {
            max_packet_size: crate::DEFAULT_MAX_PACKET_SIZE,
            connection_idle_timeout: Duration::from_secs(10),
        }
    }
}

/// Network behaviour that handles sending and receiving IPFS blocks.
pub struct Bitswap<MH = tiny_multihash::Multihash> {
    _marker: PhantomData<MH>,
    /// Bitswap config.
    config: BitswapConfig,
    /// Queue of events to report to the user.
    events: VecDeque<NetworkBehaviourAction<BitswapMessage, BitswapEvent>>,
    /// List of connected peers.
    connected_peers: FnvHashSet<PeerId>,
    /// Wanted blocks.
    wanted_blocks: FnvHashSet<Cid>,
    /// Have blocks.
    have_blocks: CuckooFilter<FnvHasher>,
}

impl<MH: MultihashDigest> Default for Bitswap<MH> {
    fn default() -> Self {
        Self::new(BitswapConfig::default())
    }
}

impl<MH: MultihashDigest> Bitswap<MH> {
    /// Creates a new `Bitswap`.
    pub fn new(config: BitswapConfig) -> Self {
        Self {
            _marker: Default::default(),
            config,
            events: Default::default(),
            connected_peers: Default::default(),
            wanted_blocks: Default::default(),
            have_blocks: CuckooFilter::with_capacity(cuckoofilter::DEFAULT_CAPACITY),
        }
    }

    pub fn want_block(&mut self, cid: Cid) {
        self.wanted_blocks.insert(cid);
    }

    pub fn cancel_block(&mut self, cid: &Cid) {
        self.wanted_blocks.remove(cid);
    }

    pub fn have_block(&mut self, cid: &Cid) -> Result<(), CuckooError> {
        self.have_blocks.test_and_add(&cid)?;
        Ok(())
    }

    pub fn dont_have_block(&mut self, cid: &Cid) {
        self.have_blocks.delete(cid);
    }

    pub fn request_have(&mut self, peer_id: PeerId, cid: Cid) {
        self.events
            .push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id,
                handler: NotifyHandler::Any,
                event: BitswapMessage::HaveRequest(cid),
            });
    }

    pub fn request_block(&mut self, peer_id: PeerId, cid: Cid) {
        self.events
            .push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id,
                handler: NotifyHandler::Any,
                event: BitswapMessage::BlockRequest(cid),
            });
    }

    fn respond_have(&mut self, peer_id: PeerId, cid: Cid, have: bool) {
        self.events
            .push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id,
                handler: NotifyHandler::Any,
                event: BitswapMessage::HaveResponse(cid, have),
            });
    }

    pub fn respond_block(&mut self, peer_id: PeerId, cid: Cid, data: Vec<u8>) {
        if !self.connected_peers.contains(&peer_id) {
            return;
        }
        self.events
            .push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id,
                handler: NotifyHandler::Any,
                event: BitswapMessage::BlockResponse(cid, data),
            });
    }
}

impl<MH: MultihashDigest> NetworkBehaviour for Bitswap<MH> {
    type ProtocolsHandler = BitswapHandler<MH>;
    type OutEvent = BitswapEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        BitswapHandler::new(BitswapHandlerConfig {
            protocol_config: BitswapProtocolConfig {
                _marker: PhantomData,
                max_packet_size: self.config.max_packet_size,
            },
            idle_timeout: self.config.connection_idle_timeout,
        })
    }

    fn addresses_of_peer(&mut self, _peer_id: &PeerId) -> Vec<Multiaddr> {
        Default::default()
    }

    fn inject_connected(&mut self, peer_id: &PeerId) {
        self.connected_peers.insert(peer_id.clone());
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId) {
        self.connected_peers.remove(peer_id);
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        _connection: ConnectionId,
        message: BitswapMessage,
    ) {
        match message {
            BitswapMessage::HaveRequest(cid) => {
                let have = self.have_blocks.contains(&cid);
                self.respond_have(peer_id, cid, have);
            }
            BitswapMessage::HaveResponse(cid, have) => {
                let ev = BitswapEvent::Have { peer_id, cid, have };
                self.events
                    .push_back(NetworkBehaviourAction::GenerateEvent(ev));
            }
            BitswapMessage::BlockResponse(cid, data) => {
                if self.wanted_blocks.remove(&cid) {
                    let ev = BitswapEvent::Block { peer_id, cid, data };
                    self.events
                        .push_back(NetworkBehaviourAction::GenerateEvent(ev));
                }
            }
            BitswapMessage::BlockRequest(cid) => {
                if self.have_blocks.contains(&cid) {
                    let ev = BitswapEvent::Want { peer_id, cid };
                    self.events
                        .push_back(NetworkBehaviourAction::GenerateEvent(ev));
                }
            }
        }
    }

    #[allow(clippy::type_complexity)]
    fn poll(&mut self, _: &mut Context, _: &mut impl PollParameters)
        -> Poll<NetworkBehaviourAction<<<Self::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::InEvent, Self::OutEvent>>
    {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::tests::create_cid;
    use futures::channel::mpsc;
    use futures::prelude::*;
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::boxed::Boxed;
    use libp2p::core::transport::upgrade::Version;
    use libp2p::identity;
    use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
    use libp2p::swarm::SwarmEvent;
    use libp2p::tcp::TcpConfig;
    use libp2p::yamux::Config as YamuxConfig;
    use libp2p::{PeerId, Swarm, Transport};
    use std::io::{Error, ErrorKind};
    use std::time::Duration;
    use tiny_multihash::Multihash;

    fn mk_transport() -> (PeerId, Boxed<(PeerId, StreamMuxerBox), Error>) {
        let id_key = identity::Keypair::generate_ed25519();
        let peer_id = id_key.public().into_peer_id();
        let dh_key = Keypair::<X25519Spec>::new()
            .into_authentic(&id_key)
            .unwrap();
        let noise = NoiseConfig::xx(dh_key).into_authenticated();

        let transport = TcpConfig::new()
            .nodelay(true)
            .upgrade(Version::V1)
            .authenticate(noise)
            .multiplex(YamuxConfig::default())
            .timeout(Duration::from_secs(20))
            .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
            .map_err(|err| Error::new(ErrorKind::Other, err))
            .boxed();
        (peer_id, transport)
    }

    #[async_std::test]
    async fn test_bitswap_behaviour() {
        env_logger::try_init().ok();

        let (peer1_id, trans) = mk_transport();
        let mut swarm1 = Swarm::new(trans, Bitswap::<Multihash>::default(), peer1_id.clone());

        let (peer2_id, trans) = mk_transport();
        let mut swarm2 = Swarm::new(trans, Bitswap::<Multihash>::default(), peer2_id.clone());

        let (mut tx, mut rx) = mpsc::channel::<Multiaddr>(1);
        Swarm::listen_on(&mut swarm1, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

        let orig_data = b"hello world".to_vec();
        let orig_cid = create_cid(&orig_data);

        let peer1 = async move {
            while swarm1.next().now_or_never().is_some() {}

            for l in Swarm::listeners(&swarm1) {
                tx.send(l.clone()).await.unwrap();
            }

            swarm1.have_block(&orig_cid).unwrap();
            log::debug!("polling swarm1");

            loop {
                match swarm1.next_event().await {
                    SwarmEvent::Behaviour(BitswapEvent::Want { peer_id, cid }) => {
                        if cid == orig_cid {
                            swarm1.respond_block(peer_id, cid, orig_data.clone());
                        }
                    }
                    e => {
                        log::debug!("swarm1 {:?}", e);
                    }
                }
            }
        };

        let peer2 = async move {
            Swarm::dial_addr(&mut swarm2, rx.next().await.unwrap()).unwrap();
            swarm2.want_block(orig_cid);
            log::debug!("polling swarm2");

            loop {
                match swarm2.next_event().await {
                    SwarmEvent::Behaviour(BitswapEvent::Have { peer_id, cid, have }) => {
                        if cid == orig_cid && have {
                            swarm2.request_block(peer_id, cid);
                        }
                    }
                    SwarmEvent::Behaviour(BitswapEvent::Block {
                        peer_id: _,
                        cid,
                        data,
                    }) => {
                        if cid == orig_cid {
                            return data;
                        }
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        swarm2.request_have(peer_id, orig_cid);
                    }
                    e => {
                        log::debug!("swarm2 {:?}", e);
                    }
                }
            }
        };

        let block = future::select(Box::pin(peer1), Box::pin(peer2))
            .await
            .factor_first()
            .0;
        assert_eq!(&block[..], b"hello world");
    }
}
