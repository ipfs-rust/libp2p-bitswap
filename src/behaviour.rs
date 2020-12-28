//! Handles the `/ipfs/bitswap/1.0.0` and `/ipfs/bitswap/1.1.0` protocols. This
//! allows exchanging IPFS blocks.
//!
//! # Usage
//!
//! The `Bitswap` struct implements the `NetworkBehaviour` trait. When used, it
//! will allow providing and reciving IPFS blocks.
use crate::protocol::{
    BitswapCodec, BitswapProtocol, BitswapRequest, BitswapResponse, RequestType,
};
use crate::query::{Query, QueryEvent, QueryManager, QueryResult, QueryType};
use crate::stats::BitswapStats;
use fnv::{FnvHashMap, FnvHashSet};
use futures::task::{Context, Poll};
use libipld::block::Block;
use libipld::cid::Cid;
use libipld::store::StoreParams;
use libp2p::core::connection::{ConnectionId, ListenerId};
use libp2p::core::{ConnectedPoint, Multiaddr, PeerId};
use libp2p::request_response::{
    throttled::{Event as ThrottledEvent, ResponseChannel, Throttled},
    InboundFailure, OutboundFailure, ProtocolSupport, RequestId, RequestResponseConfig,
    RequestResponseEvent, RequestResponseMessage,
};
use libp2p::swarm::ProtocolsHandler;
use libp2p::swarm::{NetworkBehaviour, NetworkBehaviourAction, PollParameters};
use std::collections::VecDeque;
use std::error::Error;
use std::num::NonZeroU16;
use std::time::Duration;

/// Bitswap response channel.
pub type Channel = ResponseChannel<BitswapResponse>;

/// Event emitted by the bitswap behaviour.
#[derive(Debug)]
pub enum BitswapEvent<P: StoreParams> {
    /// The sync algorithm wants to know the providers of a block. To handle
    /// this event use the `add_provider(cid, peer_id)` and `complete_get_providers(cid)`
    /// methods.
    GetProviders(Cid),
    /// A get or sync query completed.
    QueryComplete(Query, QueryResult),
    /// Have request.
    HaveBlock(Channel, Cid),
    /// Want request.
    WantBlock(Channel, Cid),
    /// Received block.
    ReceivedBlock(Block<P>),
    /// Missing blocks.
    MissingBlocks(Cid),
}

/// Bitswap configuration.
pub struct BitswapConfig {
    /// Timeout of a request.
    pub request_timeout: Duration,
    /// Time a connection is kept alive.
    pub connection_keep_alive: Duration,
    /// The number of concurrent requests per peer.
    pub receive_limit: NonZeroU16,
}

impl BitswapConfig {
    /// Creates a new `BitswapConfig`.
    pub fn new() -> Self {
        Self {
            request_timeout: Duration::from_secs(10),
            connection_keep_alive: Duration::from_secs(10),
            receive_limit: NonZeroU16::new(20).expect("20 > 0"),
        }
    }
}

impl Default for BitswapConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Network behaviour that handles sending and receiving blocks.
pub struct Bitswap<P: StoreParams> {
    /// Inner behaviour.
    inner: Throttled<BitswapCodec<P>>,
    /// Query manager.
    query_manager: QueryManager,
    /// Pending requests.
    pending_requests: VecDeque<(PeerId, BitswapRequest)>,
    /// Requests.
    requests: FnvHashMap<RequestId, Cid>,
    /// Stats.
    stats: BitswapStats,
}

impl<P: StoreParams> Bitswap<P> {
    /// Creates a new `Bitswap` behaviour.
    pub fn new(config: BitswapConfig) -> Self {
        let mut rr_config = RequestResponseConfig::default();
        rr_config.set_connection_keep_alive(config.connection_keep_alive);
        rr_config.set_request_timeout(config.request_timeout);
        let protocols = std::iter::once((BitswapProtocol, ProtocolSupport::Full));
        let mut inner = Throttled::new(BitswapCodec::<P>::default(), protocols, rr_config);
        inner.set_receive_limit(config.receive_limit);
        Self {
            inner,
            query_manager: Default::default(),
            pending_requests: Default::default(),
            requests: Default::default(),
            stats: Default::default(),
        }
    }

    /// Adds an address for a peer.
    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.inner.add_address(peer_id, addr);
    }

    /// Starts a get query.
    pub fn get(&mut self, cid: Cid) {
        self.stats.num_get.inc();
        self.query_manager.get(cid);
    }

    /// Cancels an in progress get query.
    pub fn cancel_get(&mut self, cid: Cid) {
        self.stats.num_get_cancel.inc();
        self.query_manager.cancel_get(cid);
    }

    /// Starts a sync query.
    pub fn sync(&mut self, cid: Cid, missing: FnvHashSet<Cid>) {
        self.query_manager.sync(cid, missing);
        self.stats.num_sync.inc();
    }

    /// Cancels an in progress sync query.
    pub fn cancel_sync(&mut self, cid: Cid) {
        self.stats.num_sync_cancel.inc();
        self.query_manager.cancel_sync(cid);
    }

    /// Adds a provider for a cid. Used for handling the `GetProviders` event.
    pub fn add_provider(&mut self, cid: Cid, peer_id: PeerId) {
        self.query_manager.add_provider(cid, peer_id);
    }

    /// All providers where added. Used for handling the `GetProviders` event.
    pub fn complete_get_providers(&mut self, cid: Cid) {
        self.query_manager.complete_get_providers(cid);
    }

    /// Add missing blocks.
    pub fn add_missing(&mut self, cid: Cid, missing: FnvHashSet<Cid>) {
        self.query_manager.add_missing(cid, missing);
    }

    /// Send have block.
    pub fn send_have(&mut self, channel: Channel, have: bool) {
        if have {
            self.stats.num_tx_have_yes.inc();
        } else {
            self.stats.num_tx_have_no.inc();
        }
        let response = BitswapResponse::Have(have);
        tracing::trace!("have {}", have);
        self.inner.send_response(channel, response).ok();
    }

    /// Send block.
    pub fn send_block(&mut self, channel: Channel, block: Option<Vec<u8>>) {
        let response = if let Some(data) = block {
            self.stats.num_tx_block_count.inc();
            self.stats.num_tx_block_bytes.inc_by(data.len() as u64);
            tracing::trace!("send {}", data.len());
            BitswapResponse::Block(data)
        } else {
            self.stats.num_tx_have_no.inc();
            tracing::trace!("have false");
            BitswapResponse::Have(false)
        };
        self.inner.send_response(channel, response).unwrap();
    }

    /// Returns bitswap stats.
    pub fn stats(&self) -> &BitswapStats {
        &self.stats
    }
}

impl<P: StoreParams> NetworkBehaviour for Bitswap<P> {
    type ProtocolsHandler = <Throttled<BitswapCodec<P>> as NetworkBehaviour>::ProtocolsHandler;
    type OutEvent = BitswapEvent<P>;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        self.inner.new_handler()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.inner.addresses_of_peer(peer_id)
    }

    fn inject_connected(&mut self, peer_id: &PeerId) {
        self.inner.inject_connected(peer_id)
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId) {
        self.inner.inject_disconnected(peer_id)
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        conn: &ConnectionId,
        endpoint: &ConnectedPoint,
    ) {
        self.inner
            .inject_connection_established(peer_id, conn, endpoint)
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        conn: &ConnectionId,
        endpoint: &ConnectedPoint,
    ) {
        self.inner.inject_connection_closed(peer_id, conn, endpoint)
    }

    fn inject_addr_reach_failure(
        &mut self,
        peer_id: Option<&PeerId>,
        addr: &Multiaddr,
        error: &dyn Error,
    ) {
        self.inner.inject_addr_reach_failure(peer_id, addr, error)
    }

    fn inject_dial_failure(&mut self, peer_id: &PeerId) {
        self.inner.inject_dial_failure(peer_id)
    }

    fn inject_new_listen_addr(&mut self, addr: &Multiaddr) {
        self.inner.inject_new_listen_addr(addr)
    }

    fn inject_expired_listen_addr(&mut self, addr: &Multiaddr) {
        self.inner.inject_expired_listen_addr(addr)
    }

    fn inject_new_external_addr(&mut self, addr: &Multiaddr) {
        self.inner.inject_new_external_addr(addr)
    }

    fn inject_listener_error(&mut self, id: ListenerId, err: &(dyn Error + 'static)) {
        self.inner.inject_listener_error(id, err)
    }

    fn inject_listener_closed(&mut self, id: ListenerId, reason: Result<(), &std::io::Error>) {
        self.inner.inject_listener_closed(id, reason)
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        conn: ConnectionId,
        event: <Self::ProtocolsHandler as ProtocolsHandler>::OutEvent,
    ) {
        self.inner.inject_event(peer_id, conn, event)
    }

    fn poll(
        &mut self,
        cx: &mut Context,
        pp: &mut impl PollParameters,
    ) -> Poll<
        NetworkBehaviourAction<
            <Self::ProtocolsHandler as ProtocolsHandler>::InEvent,
            Self::OutEvent,
        >,
    > {
        while let Some(query) = self.query_manager.next() {
            match query {
                QueryEvent::Request(peer, cid, ty) => {
                    let request = BitswapRequest { ty, cid };
                    match ty {
                        RequestType::Have => self.stats.num_tx_have.inc(),
                        RequestType::Block => self.stats.num_tx_block.inc(),
                    }
                    self.pending_requests.push_back((peer, request));
                }
                QueryEvent::GetProviders(cid) => {
                    self.stats.num_get_providers.inc();
                    let event = BitswapEvent::GetProviders(cid);
                    return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                }
                QueryEvent::MissingBlocks(cid) => {
                    self.stats.num_missing_blocks.inc();
                    let event = BitswapEvent::MissingBlocks(cid);
                    return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                }
                QueryEvent::Complete(query, res) => {
                    match query.ty {
                        QueryType::Get => {
                            if res.is_ok() {
                                self.stats.num_get_ok.inc();
                            } else {
                                self.stats.num_get_err.inc();
                            }
                        }
                        QueryType::Sync => {
                            if res.is_ok() {
                                self.stats.num_get_ok.inc();
                            } else {
                                self.stats.num_sync_err.inc();
                            }
                        }
                    };
                    let event = BitswapEvent::QueryComplete(query, res);
                    return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                }
            }
        }
        loop {
            for _ in 0..self.pending_requests.len() {
                if let Some((peer, request)) = self.pending_requests.pop_front() {
                    match self.inner.send_request(&peer, request) {
                        Ok(id) => {
                            self.requests.insert(id, request.cid);
                        }
                        Err(request) => {
                            self.pending_requests.push_back((peer, request));
                        }
                    }
                }
            }
            let event = match self.inner.poll(cx, pp) {
                Poll::Ready(NetworkBehaviourAction::GenerateEvent(event)) => event,
                Poll::Ready(NetworkBehaviourAction::DialAddress { address }) => {
                    return Poll::Ready(NetworkBehaviourAction::DialAddress { address });
                }
                Poll::Ready(NetworkBehaviourAction::DialPeer { peer_id, condition }) => {
                    return Poll::Ready(NetworkBehaviourAction::DialPeer { peer_id, condition });
                }
                Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                    peer_id,
                    handler,
                    event,
                }) => {
                    return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                        peer_id,
                        handler,
                        event,
                    });
                }
                Poll::Ready(NetworkBehaviourAction::ReportObservedAddr { address, score }) => {
                    return Poll::Ready(NetworkBehaviourAction::ReportObservedAddr {
                        address,
                        score,
                    });
                }
                Poll::Pending => return Poll::Pending,
            };
            let event = match event {
                ThrottledEvent::Event(event) => event,
                ThrottledEvent::ResumeSending(peer_id) => {
                    self.stats.num_throttled_resume_send.inc();
                    tracing::trace!("resume sending {}", peer_id);
                    continue;
                }
                ThrottledEvent::TooManyInboundRequests(peer_id) => {
                    self.stats.num_throttled_too_many_inbound.inc();
                    tracing::error!("too many inbound requests from {}", peer_id);
                    continue;
                }
            };
            match event {
                RequestResponseEvent::Message { peer, message } => match message {
                    RequestResponseMessage::Request {
                        request_id: _,
                        request,
                        channel,
                    } => match request {
                        BitswapRequest {
                            ty: RequestType::Have,
                            cid,
                        } => {
                            self.stats.num_rx_have.inc();
                            let event = BitswapEvent::HaveBlock(channel, cid);
                            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                        }
                        BitswapRequest {
                            ty: RequestType::Block,
                            cid,
                        } => {
                            self.stats.num_rx_block.inc();
                            let event = BitswapEvent::WantBlock(channel, cid);
                            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                        }
                    },
                    RequestResponseMessage::Response {
                        request_id,
                        response,
                    } => match response {
                        BitswapResponse::Have(have) => {
                            if have {
                                self.stats.num_rx_have_yes.inc();
                            } else {
                                self.stats.num_rx_have_no.inc();
                            }
                            let cid = self.requests.remove(&request_id).unwrap();
                            self.query_manager.complete_request(cid, peer, have);
                        }
                        BitswapResponse::Block(data) => {
                            tracing::trace!("received block");
                            let cid = self.requests.remove(&request_id).unwrap();
                            let len = data.len();
                            if let Ok(block) = Block::new(cid, data) {
                                self.stats.num_rx_block_count.inc();
                                self.stats.num_rx_block_bytes.inc_by(len as u64);
                                self.query_manager.complete_request(cid, peer, true);
                                let event = BitswapEvent::ReceivedBlock(block);
                                return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                            } else {
                                self.stats.num_rx_block_count_invalid.inc();
                                self.stats.num_rx_block_bytes_invalid.inc_by(len as u64);
                                tracing::trace!("received invalid block {}", cid);
                                self.query_manager.complete_request(cid, peer, false);
                            }
                        }
                    },
                },
                RequestResponseEvent::ResponseSent { .. } => {}
                RequestResponseEvent::OutboundFailure {
                    peer,
                    request_id,
                    error,
                } => {
                    tracing::trace!(
                        "bitswap outbound failure {} {} {:?}",
                        peer,
                        request_id,
                        error
                    );
                    if let Some(cid) = self.requests.remove(&request_id) {
                        self.query_manager.complete_request(cid, peer, false);
                    }
                    match error {
                        OutboundFailure::DialFailure => {
                            self.stats.num_outbound_dial_failure.inc();
                        }
                        OutboundFailure::Timeout => {
                            self.stats.num_outbound_timeout.inc();
                        }
                        OutboundFailure::ConnectionClosed => {
                            self.stats.num_outbound_connection_closed.inc();
                        }
                        OutboundFailure::UnsupportedProtocols => {
                            self.stats.num_outbound_unsupported_protocols.inc();
                        }
                    }
                }
                RequestResponseEvent::InboundFailure {
                    peer,
                    request_id,
                    error,
                } => {
                    tracing::trace!(
                        "bitswap inbound failure {} {} {:?}",
                        peer,
                        request_id,
                        error
                    );
                    match error {
                        InboundFailure::Timeout => {
                            self.stats.num_inbound_timeout.inc();
                        }
                        InboundFailure::ConnectionClosed => {
                            self.stats.num_inbound_connection_closed.inc();
                        }
                        InboundFailure::UnsupportedProtocols => {
                            self.stats.num_inbound_unsupported_protocols.inc();
                        }
                        InboundFailure::ResponseOmission => {
                            self.stats.num_inbound_response_omission.inc();
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::QueryType;
    use async_std::task;
    use fnv::FnvHashSet;
    use futures::prelude::*;
    use libipld::block::Block;
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::ipld::Ipld;
    use libipld::multihash::Code;
    use libipld::store::DefaultParams;
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::upgrade::Version;
    use libp2p::core::transport::Boxed;
    use libp2p::identity;
    use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
    use libp2p::tcp::TcpConfig;
    use libp2p::yamux::YamuxConfig;
    use libp2p::{PeerId, Swarm, Transport};
    use std::time::Duration;

    fn mk_transport() -> (PeerId, Boxed<(PeerId, StreamMuxerBox)>) {
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
            .boxed();
        (peer_id, transport)
    }

    fn create_block(ipld: Ipld) -> Block<DefaultParams> {
        Block::encode(DagCborCodec, Code::Blake3_256, &ipld).unwrap()
    }

    struct Peer {
        peer_id: PeerId,
        addr: Multiaddr,
        store: FnvHashMap<Cid, Vec<u8>>,
        swarm: Swarm<Bitswap<DefaultParams>>,
    }

    impl Peer {
        fn new() -> Self {
            let (peer_id, trans) = mk_transport();
            let store = Default::default();
            let mut swarm = Swarm::new(trans, Bitswap::new(BitswapConfig::new()), peer_id);
            Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
            while swarm.next().now_or_never().is_some() {}
            let addr = Swarm::listeners(&swarm).next().unwrap().clone();
            Self {
                peer_id,
                addr,
                store,
                swarm,
            }
        }

        fn add_address(&mut self, peer: &Peer) {
            self.swarm.add_address(&peer.peer_id, peer.addr.clone());
        }

        fn store(&mut self) -> &mut FnvHashMap<Cid, Vec<u8>> {
            &mut self.store
        }

        fn swarm(&mut self) -> &mut Swarm<Bitswap<DefaultParams>> {
            &mut self.swarm
        }

        fn spawn(mut self, name: &'static str) -> PeerId {
            let peer_id = self.peer_id;
            task::spawn(async move {
                loop {
                    match self.swarm.next().await {
                        BitswapEvent::HaveBlock(channel, cid) => {
                            self.swarm.send_have(channel, self.store.contains_key(&cid))
                        }
                        BitswapEvent::WantBlock(channel, cid) => {
                            let data = self.store.get(&cid).cloned();
                            self.swarm.send_block(channel, data);
                        }
                        e => tracing::debug!("{}: {:?}", name, e),
                    }
                }
            });
            peer_id
        }
    }

    #[async_std::test]
    async fn test_bitswap_get() {
        env_logger::try_init().ok();
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let block = create_block(ipld!(&b"hello world"[..]));
        peer1.store().insert(*block.cid(), block.data().to_vec());
        let peer1 = peer1.spawn("peer1");

        peer2.swarm().get(*block.cid());
        assert!(matches!(
            peer2.swarm().next().await,
            BitswapEvent::GetProviders(_)
        ));
        peer2.swarm().add_provider(*block.cid(), peer1);
        peer2.swarm().complete_get_providers(*block.cid());
        match peer2.swarm().next().await {
            BitswapEvent::ReceivedBlock(block2) => {
                assert_eq!(block, block2);
            }
            e => panic!("{:?}", e),
        }
        assert!(matches!(
            peer2.swarm().next().await,
            BitswapEvent::QueryComplete(
                Query {
                    ty: QueryType::Get,
                    cid: _,
                },
                Ok(()),
            )
        ));
    }

    #[async_std::test]
    async fn test_bitswap_cancel_get() {
        env_logger::try_init().ok();
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let block = create_block(ipld!(&b"hello world"[..]));
        peer1.store().insert(*block.cid(), block.data().to_vec());
        peer1.spawn("peer1");

        peer2.swarm().get(*block.cid());
        peer2.swarm().cancel_get(*block.cid());
        assert!(peer2.swarm().next().now_or_never().is_none());
    }

    #[async_std::test]
    async fn test_bitswap_sync() {
        env_logger::try_init().ok();
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let b0 = create_block(ipld!({
            "n": 0,
        }));
        let b1 = create_block(ipld!({
            "prev": b0.cid(),
            "n": 1,
        }));
        let b2 = create_block(ipld!({
            "prev": b1.cid(),
            "n": 2,
        }));
        peer1.store().insert(*b0.cid(), b0.data().to_vec());
        peer1.store().insert(*b1.cid(), b1.data().to_vec());
        peer1.store().insert(*b2.cid(), b2.data().to_vec());
        let peer1 = peer1.spawn("peer1");

        let mut missing = FnvHashSet::default();
        missing.insert(*b2.cid());
        peer2.swarm().sync(*b2.cid(), missing);
        assert!(matches!(
            peer2.swarm().next().await,
            BitswapEvent::GetProviders(cid) if &cid == b2.cid()
        ));
        peer2.swarm().add_provider(*b2.cid(), peer1);
        peer2.swarm().complete_get_providers(*b2.cid());

        match peer2.swarm().next().await {
            BitswapEvent::ReceivedBlock(b22) => {
                assert_eq!(b2, b22);
            }
            e => panic!("{:?}", e),
        }
        match peer2.swarm().next().await {
            BitswapEvent::MissingBlocks(cid) => {
                assert_eq!(b2.cid(), &cid);
            }
            e => panic!("{:?}", e),
        }
        let mut missing = FnvHashSet::default();
        missing.insert(*b1.cid());
        peer2.swarm().add_missing(*b2.cid(), missing);

        match peer2.swarm().next().await {
            BitswapEvent::ReceivedBlock(b12) => {
                assert_eq!(b1, b12);
            }
            e => panic!("{:?}", e),
        }
        match peer2.swarm().next().await {
            BitswapEvent::MissingBlocks(cid) => {
                assert_eq!(b2.cid(), &cid);
            }
            e => panic!("{:?}", e),
        }
        let mut missing = FnvHashSet::default();
        missing.insert(*b0.cid());
        peer2.swarm().add_missing(*b2.cid(), missing);

        match peer2.swarm().next().await {
            BitswapEvent::ReceivedBlock(b02) => {
                assert_eq!(b0, b02);
            }
            e => panic!("{:?}", e),
        }
        match peer2.swarm().next().await {
            BitswapEvent::MissingBlocks(cid) => {
                assert_eq!(b2.cid(), &cid);
            }
            e => panic!("{:?}", e),
        }
        let missing = FnvHashSet::default();
        peer2.swarm().add_missing(*b2.cid(), missing);

        assert!(matches!(
            peer2.swarm().next().await,
            BitswapEvent::QueryComplete(
                Query {
                    ty: QueryType::Sync,
                    cid: _,
                },
                Ok(()),
            )
        ));
    }

    #[async_std::test]
    async fn test_bitswap_cancel_sync() {
        env_logger::try_init().ok();
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let block = create_block(ipld!(&b"hello world"[..]));
        peer1.store().insert(*block.cid(), block.data().to_vec());
        peer1.spawn("peer1");

        let mut missing = FnvHashSet::default();
        missing.insert(*block.cid());
        peer2.swarm().sync(*block.cid(), missing);
        peer2.swarm().cancel_sync(*block.cid());
        assert!(peer2.swarm().next().now_or_never().is_none());
    }
}
