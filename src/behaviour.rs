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
use crate::query::{QueryEvent, QueryId, QueryManager, QueryType, Request, Response};
use crate::stats::BitswapStats;
use fnv::FnvHashMap;
use futures::task::{Context, Poll};
use libipld::block::Block;
use libipld::cid::Cid;
use libipld::error::BlockNotFound;
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
    Providers(QueryId, Cid),
    /// Missing blocks.
    MissingBlocks(QueryId, Cid),
    /// Received block.
    ReceivedBlock(QueryId, PeerId, Block<P>),
    /// A get or sync query completed.
    Complete(QueryId, Result<(), BlockNotFound>),
    /// Have request.
    HaveBlock(Channel, PeerId, Cid),
    /// Want request.
    WantBlock(Channel, PeerId, Cid),
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
    pending_requests: VecDeque<(QueryId, PeerId, BitswapRequest)>,
    /// Requests.
    requests: FnvHashMap<RequestId, QueryId>,
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
    pub fn get(&mut self, cid: Cid, initial: impl Iterator<Item = PeerId>) -> QueryId {
        self.stats.num_get.inc();
        self.query_manager.get(None, cid, initial)
    }

    /// Starts a sync query.
    pub fn sync(&mut self, cid: Cid, missing: impl Iterator<Item = Cid>) -> QueryId {
        self.stats.num_sync.inc();
        self.query_manager.sync(cid, missing)
    }

    /// Cancels an in progress query.
    pub fn cancel(&mut self, id: QueryId) -> bool {
        match self.query_manager.cancel(id) {
            Some(QueryType::Get) => {
                self.stats.num_get_cancel.inc();
                true
            }
            Some(QueryType::Sync) => {
                self.stats.num_sync_cancel.inc();
                true
            }
            None => false,
        }
    }

    /// Adds a provider for a cid. Used for handling the `GetProviders` event.
    pub fn inject_providers(&mut self, id: QueryId, providers: Vec<PeerId>) {
        self.query_manager
            .inject_response(id, Response::Providers(providers));
    }

    /// Add missing blocks.
    pub fn inject_missing_blocks(&mut self, id: QueryId, missing: Vec<Cid>) {
        self.query_manager
            .inject_response(id, Response::MissingBlocks(missing));
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
            tracing::trace!("block {}", data.len());
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
                QueryEvent::Request(id, req) => match req {
                    Request::Have(peer_id, cid) => {
                        self.stats.num_tx_have.inc();
                        let req = BitswapRequest {
                            ty: RequestType::Have,
                            cid,
                        };
                        self.pending_requests.push_back((id, peer_id, req));
                    }
                    Request::Block(peer_id, cid) => {
                        self.stats.num_tx_block.inc();
                        let req = BitswapRequest {
                            ty: RequestType::Block,
                            cid,
                        };
                        self.pending_requests.push_back((id, peer_id, req));
                    }
                    Request::Providers(cid) => {
                        self.stats.num_get_providers.inc();
                        let event = BitswapEvent::Providers(id, cid);
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                    }
                    Request::MissingBlocks(cid) => {
                        self.stats.num_missing_blocks.inc();
                        let event = BitswapEvent::MissingBlocks(id, cid);
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                    }
                },
                QueryEvent::Complete(id, ty, res) => {
                    match ty {
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
                    let event = BitswapEvent::Complete(id, res.map_err(BlockNotFound));
                    return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                }
            }
        }
        loop {
            for _ in 0..self.pending_requests.len() {
                if let Some((qid, peer, request)) = self.pending_requests.pop_front() {
                    match self.inner.send_request(&peer, request) {
                        Ok(rid) => {
                            self.requests.insert(rid, qid);
                        }
                        Err(request) => {
                            self.pending_requests.push_back((qid, peer, request));
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
                    tracing::trace!("too many inbound requests from {}", peer_id);
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
                            let event = BitswapEvent::HaveBlock(channel, peer, cid);
                            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                        }
                        BitswapRequest {
                            ty: RequestType::Block,
                            cid,
                        } => {
                            self.stats.num_rx_block.inc();
                            let event = BitswapEvent::WantBlock(channel, peer, cid);
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
                            let id = self.requests.remove(&request_id).unwrap();
                            self.query_manager
                                .inject_response(id, Response::Have(peer, have));
                        }
                        BitswapResponse::Block(data) => {
                            let id = self.requests.remove(&request_id).unwrap();
                            let (cid, root) = if let Some(info) = self.query_manager.query_info(id)
                            {
                                (&info.cid, info.root)
                            } else {
                                continue;
                            };
                            let len = data.len();
                            if let Ok(block) = Block::new(*cid, data) {
                                self.stats.num_rx_block_count.inc();
                                self.stats.num_rx_block_bytes.inc_by(len as u64);
                                self.query_manager
                                    .inject_response(id, Response::Block(peer, true));
                                let event = BitswapEvent::ReceivedBlock(root, peer, block);
                                return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                            } else {
                                self.stats.num_rx_block_count_invalid.inc();
                                self.stats.num_rx_block_bytes_invalid.inc_by(len as u64);
                                self.query_manager
                                    .inject_response(id, Response::Block(peer, false));
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
                    if let Some(id) = self.requests.remove(&request_id) {
                        self.query_manager
                            .inject_response(id, Response::Have(peer, false));
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
    use async_std::task;
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

    fn tracing_try_init() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init()
            .ok();
    }

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
                        BitswapEvent::HaveBlock(channel, _peer_id, cid) => {
                            self.swarm.send_have(channel, self.store.contains_key(&cid))
                        }
                        BitswapEvent::WantBlock(channel, _peer_id, cid) => {
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

    fn assert_providers(event: BitswapEvent<DefaultParams>, cid: Cid) -> QueryId {
        if let BitswapEvent::Providers(id, cid2) = event {
            assert_eq!(cid2, cid);
            id
        } else {
            panic!("{:?} is not a provider request", event);
        }
    }

    fn assert_missing(event: BitswapEvent<DefaultParams>, cid: Cid) -> QueryId {
        if let BitswapEvent::MissingBlocks(id, cid2) = event {
            assert_eq!(cid2, cid);
            id
        } else {
            panic!("{:?} is not a missing blocks request", event);
        }
    }

    fn assert_block(event: BitswapEvent<DefaultParams>, id: QueryId, block: &Block<DefaultParams>) {
        if let BitswapEvent::ReceivedBlock(id2, _peer, block2) = event {
            assert_eq!(id2, id);
            assert_eq!(&block2, block);
        } else {
            panic!("{:?} is not a block", event);
        }
    }

    fn assert_complete_ok(event: BitswapEvent<DefaultParams>, id: QueryId) {
        if let BitswapEvent::Complete(id2, Ok(())) = event {
            assert_eq!(id2, id);
        } else {
            panic!("{:?} is not a complete event", event);
        }
    }

    #[async_std::test]
    async fn test_bitswap_get() {
        tracing_try_init();
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let block = create_block(ipld!(&b"hello world"[..]));
        peer1.store().insert(*block.cid(), block.data().to_vec());
        let peer1 = peer1.spawn("peer1");

        let id = peer2.swarm().get(*block.cid(), std::iter::empty());

        let id1 = assert_providers(peer2.swarm().next().await, *block.cid());
        peer2.swarm().inject_providers(id1, vec![peer1]);

        assert_block(peer2.swarm().next().await, id, &block);
        assert_complete_ok(peer2.swarm().next().await, id);
    }

    #[async_std::test]
    async fn test_bitswap_cancel_get() {
        tracing_try_init();
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let block = create_block(ipld!(&b"hello world"[..]));
        peer1.store().insert(*block.cid(), block.data().to_vec());
        peer1.spawn("peer1");

        let id = peer2.swarm().get(*block.cid(), std::iter::empty());
        peer2.swarm().cancel(id);
        let res = peer2.swarm().next().now_or_never();
        println!("{:?}", res);
        assert!(res.is_none());
    }

    #[async_std::test]
    async fn test_bitswap_sync() {
        tracing_try_init();
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

        let id = peer2.swarm().sync(*b2.cid(), std::iter::once(*b2.cid()));

        let id1 = assert_providers(peer2.swarm().next().await, *b2.cid());
        peer2.swarm().inject_providers(id1, vec![peer1]);

        assert_block(peer2.swarm().next().await, id, &b2);
        let id1 = assert_missing(peer2.swarm().next().await, *b2.cid());
        peer2.swarm().inject_missing_blocks(id1, vec![*b1.cid()]);

        assert_block(peer2.swarm().next().await, id, &b1);
        let id1 = assert_missing(peer2.swarm().next().await, *b1.cid());
        peer2.swarm().inject_missing_blocks(id1, vec![*b0.cid()]);

        assert_block(peer2.swarm().next().await, id, &b0);
        let id1 = assert_missing(peer2.swarm().next().await, *b0.cid());
        peer2.swarm().inject_missing_blocks(id1, vec![]);

        assert_complete_ok(peer2.swarm().next().await, id);
    }

    #[async_std::test]
    async fn test_bitswap_cancel_sync() {
        tracing_try_init();
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let block = create_block(ipld!(&b"hello world"[..]));
        peer1.store().insert(*block.cid(), block.data().to_vec());
        peer1.spawn("peer1");

        let id = peer2
            .swarm()
            .sync(*block.cid(), std::iter::once(*block.cid()));
        peer2.swarm().cancel(id);
        let res = peer2.swarm().next().now_or_never();
        println!("{:?}", res);
        assert!(res.is_none());
    }
}
