//! Handles the `/ipfs/bitswap/1.0.0` and `/ipfs/bitswap/1.1.0` protocols. This
//! allows exchanging IPFS blocks.
//!
//! # Usage
//!
//! The `Bitswap` struct implements the `NetworkBehaviour` trait. When used, it
//! will allow providing and reciving IPFS blocks.
#[cfg(feature = "compat")]
use crate::compat::{CompatMessage, CompatProtocol, InboundMessage};
use crate::protocol::{
    BitswapCodec, BitswapProtocol, BitswapRequest, BitswapResponse, RequestType,
};
use crate::query::{QueryEvent, QueryId, QueryManager, Request, Response};
use crate::stats::*;
use fnv::FnvHashMap;
#[cfg(feature = "compat")]
use fnv::FnvHashSet;
use futures::task::{Context, Poll};
use libipld::error::BlockNotFound;
use libipld::store::StoreParams;
use libipld::{Block, Cid, Result};
use libp2p::core::connection::{ConnectionId, ListenerId};
#[cfg(feature = "compat")]
use libp2p::core::either::EitherOutput;
use libp2p::core::{ConnectedPoint, Multiaddr, PeerId};
use libp2p::request_response::{
    InboundFailure, OutboundFailure, ProtocolSupport, RequestId, RequestResponse, RequestResponseConfig,
    RequestResponseEvent, RequestResponseMessage, ResponseChannel,
};
use libp2p::swarm::{NetworkBehaviour, NetworkBehaviourAction, PollParameters, ProtocolsHandler};
#[cfg(feature = "compat")]
use libp2p::swarm::{NotifyHandler, OneShotHandler, ProtocolsHandlerSelect};
use prometheus::Registry;
#[cfg(feature = "compat")]
use std::collections::VecDeque;
use std::error::Error;
use std::num::NonZeroU16;
use std::time::Duration;

/// Bitswap response channel.
pub type Channel = ResponseChannel<BitswapResponse>;

/// Event emitted by the bitswap behaviour.
#[derive(Debug)]
pub enum BitswapEvent {
    /// A get query needs a list of providers to make progress. Once the new set of
    /// providers is determined the get query can be notified using the `inject_providers`
    /// method.
    Providers(QueryId, Cid),
    /// Received a block from a peer. Includes the number of known missing blocks for a
    /// sync query. When a block is received and missing blocks is not empty the counter
    /// is increased. If missing blocks is empty the counter is decremented.
    Progress(QueryId, usize),
    /// A get or sync query completed.
    Complete(QueryId, Result<()>),
}

/// Trait implemented by a block store.
pub trait BitswapStore: Send + Sync + 'static {
    /// The store params.
    type Params: StoreParams;
    /// A have query needs to know if the block store contains the block.
    fn contains(&mut self, cid: &Cid) -> Result<bool>;
    /// A block query needs to retrieve the block from the store.
    fn get(&mut self, cid: &Cid) -> Result<Option<Vec<u8>>>;
    /// A block response needs to insert the block into the store.
    fn insert(&mut self, block: &Block<Self::Params>) -> Result<()>;
    /// A block response needs to insert the block into the store.
    fn insert_many(&mut self, blocks: &[Block<Self::Params>]) -> Result<()> {
        for block in blocks {
            self.insert(block)?;
        }
        Ok(())
    }
    /// A sync query needs a list of missing blocks to make progress.
    fn missing_blocks(&mut self, cid: &Cid) -> Result<Vec<Cid>>;
}

/// Bitswap configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum BitswapId {
    Bitswap(RequestId),
    #[cfg(feature = "compat")]
    Compat(Cid),
}

enum BitswapChannel {
    Bitswap(Channel),
    #[cfg(feature = "compat")]
    Compat(PeerId),
}

/// Network behaviour that handles sending and receiving blocks.
pub struct Bitswap<P: StoreParams> {
    /// Inner behaviour.
    inner: RequestResponse<BitswapCodec<P>>,
    /// Query manager.
    query_manager: QueryManager,
    /// Requests.
    requests: FnvHashMap<BitswapId, QueryId>,
    /// Bitswap store.
    store: Box<dyn BitswapStore<Params = P>>,
    /// Compat peers.
    #[cfg(feature = "compat")]
    compat: FnvHashSet<PeerId>,
    /// Compat queue.
    #[cfg(feature = "compat")]
    compat_queue: VecDeque<(PeerId, CompatMessage)>,
    /// Insert batch.
    batch: Vec<Block<P>>,
}

impl<P: StoreParams> Bitswap<P> {
    /// Creates a new `Bitswap` behaviour.
    pub fn new<S: BitswapStore<Params = P>>(config: BitswapConfig, store: S) -> Self {
        let mut rr_config = RequestResponseConfig::default();
        rr_config.set_connection_keep_alive(config.connection_keep_alive);
        rr_config.set_request_timeout(config.request_timeout);
        let protocols = std::iter::once((BitswapProtocol, ProtocolSupport::Full));
        let inner = RequestResponse::new(BitswapCodec::<P>::default(), protocols, rr_config);
        Self {
            inner,
            query_manager: Default::default(),
            requests: Default::default(),
            store: Box::new(store),
            #[cfg(feature = "compat")]
            compat: Default::default(),
            #[cfg(feature = "compat")]
            compat_queue: Default::default(),
            batch: Vec::with_capacity(100),
        }
    }

    /// Adds an address for a peer.
    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.inner.add_address(peer_id, addr);
    }

    /// Removes an address for a peer.
    pub fn remove_address(&mut self, peer_id: &PeerId, addr: &Multiaddr) {
        self.inner.remove_address(peer_id, addr);
    }

    /// Starts a get query with an initial guess of providers.
    pub fn get(&mut self, cid: Cid, initial: impl Iterator<Item = PeerId>) -> QueryId {
        self.query_manager.get(None, cid, initial)
    }

    /// Starts a sync query with an the initial set of missing blocks.
    pub fn sync(&mut self, cid: Cid, missing: impl Iterator<Item = Cid>) -> QueryId {
        self.query_manager.sync(cid, missing)
    }

    /// Cancels an in progress query. Returns true if a query was cancelled.
    pub fn cancel(&mut self, id: QueryId) -> bool {
        let res = self.query_manager.cancel(id);
        if res {
            REQUESTS_CANCELED.inc();
        }
        res
    }

    /// Adds a provider for a cid. Used for handling the `Providers` event.
    pub fn inject_providers(&mut self, id: QueryId, providers: Vec<PeerId>) {
        PROVIDERS_TOTAL.inc_by(providers.len() as u64);
        self.query_manager
            .inject_response(id, Response::Providers(providers));
    }

    /// Registers prometheus metrics.
    pub fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(REQUESTS_TOTAL.clone()))?;
        registry.register(Box::new(REQUEST_DURATION_SECONDS.clone()))?;
        registry.register(Box::new(REQUESTS_CANCELED.clone()))?;
        registry.register(Box::new(BLOCK_NOT_FOUND.clone()))?;
        registry.register(Box::new(PROVIDERS_TOTAL.clone()))?;
        registry.register(Box::new(MISSING_BLOCKS_TOTAL.clone()))?;
        registry.register(Box::new(RECEIVED_BLOCK_BYTES.clone()))?;
        registry.register(Box::new(RECEIVED_INVALID_BLOCK_BYTES.clone()))?;
        registry.register(Box::new(SENT_BLOCK_BYTES.clone()))?;
        registry.register(Box::new(RESPONSES_TOTAL.clone()))?;
        registry.register(Box::new(THROTTLED_INBOUND.clone()))?;
        registry.register(Box::new(THROTTLED_OUTBOUND.clone()))?;
        registry.register(Box::new(OUTBOUND_FAILURE.clone()))?;
        registry.register(Box::new(INBOUND_FAILURE.clone()))?;
        Ok(())
    }
}

impl<P: StoreParams> Bitswap<P> {
    /// Processes an incoming bitswap request.
    fn inject_request(&mut self, channel: BitswapChannel, request: BitswapRequest) {
        let response = match request.ty {
            RequestType::Have => {
                let have = self.store.contains(&request.cid).ok().unwrap_or_default();
                if have {
                    RESPONSES_TOTAL.with_label_values(&["have"]).inc();
                } else {
                    RESPONSES_TOTAL.with_label_values(&["dont_have"]).inc();
                }
                tracing::trace!("have {}", have);
                BitswapResponse::Have(have)
            }
            RequestType::Block => {
                let block = self.store.get(&request.cid).ok().unwrap_or_default();
                if let Some(data) = block {
                    RESPONSES_TOTAL.with_label_values(&["block"]).inc();
                    SENT_BLOCK_BYTES.inc_by(data.len() as u64);
                    tracing::trace!("block {}", data.len());
                    BitswapResponse::Block(data)
                } else {
                    RESPONSES_TOTAL.with_label_values(&["dont_have"]).inc();
                    tracing::trace!("have false");
                    BitswapResponse::Have(false)
                }
            }
        };
        match channel {
            BitswapChannel::Bitswap(channel) => {
                self.inner.send_response(channel, response).ok();
            }
            #[cfg(feature = "compat")]
            BitswapChannel::Compat(peer_id) => {
                let msg = CompatMessage::Response(request.cid, response);
                self.compat_queue.push_back((peer_id, msg));
            }
        }
    }

    /// Processes an incoming bitswap response.
    fn inject_response(
        &mut self,
        id: BitswapId,
        peer: PeerId,
        response: BitswapResponse,
    ) -> Option<BitswapEvent> {
        if let Some(id) = self.requests.remove(&id) {
            match response {
                BitswapResponse::Have(have) => {
                    self.query_manager
                        .inject_response(id, Response::Have(peer, have));
                }
                BitswapResponse::Block(data) => {
                    if let Some(info) = self.query_manager.query_info(id) {
                        let len = data.len();
                        if let Ok(block) = Block::new(info.cid, data) {
                            RECEIVED_BLOCK_BYTES.inc_by(len as u64);
                            self.batch.push(block);
                            self.query_manager
                                .inject_response(id, Response::Block(peer, true));
                        } else {
                            tracing::trace!("received invalid block");
                            RECEIVED_INVALID_BLOCK_BYTES.inc_by(len as u64);
                            self.query_manager
                                .inject_response(id, Response::Block(peer, false));
                        }
                    }
                }
            }
        }
        None
    }

    fn insert_batch(&mut self) {
        if !self.batch.is_empty() {
            if let Err(err) = self.store.insert_many(&self.batch) {
                tracing::error!("error inserting blocks {}", err);
            }
            self.batch.clear();
        }
    }
}

impl<P: StoreParams> NetworkBehaviour for Bitswap<P> {
    #[cfg(not(feature = "compat"))]
    type ProtocolsHandler = <RequestResponse<BitswapCodec<P>> as NetworkBehaviour>::ProtocolsHandler;

    #[cfg(feature = "compat")]
    #[allow(clippy::type_complexity)]
    type ProtocolsHandler = ProtocolsHandlerSelect<
        <RequestResponse<BitswapCodec<P>> as NetworkBehaviour>::ProtocolsHandler,
        OneShotHandler<CompatProtocol, CompatMessage, InboundMessage>,
    >;
    type OutEvent = BitswapEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        #[cfg(not(feature = "compat"))]
        return self.inner.new_handler();
        #[cfg(feature = "compat")]
        self.inner.new_handler().select(OneShotHandler::default())
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.inner.addresses_of_peer(peer_id)
    }

    fn inject_connected(&mut self, peer_id: &PeerId) {
        self.inner.inject_connected(peer_id)
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId) {
        self.inner.inject_disconnected(peer_id);
        #[cfg(feature = "compat")]
        self.compat.remove(peer_id);
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
        #[cfg(not(feature = "compat"))]
        return self.inner.inject_event(peer_id, conn, event);
        #[cfg(feature = "compat")]
        match event {
            EitherOutput::First(event) => self.inner.inject_event(peer_id, conn, event),
            EitherOutput::Second(msg) => {
                for msg in msg.0 {
                    match msg {
                        CompatMessage::Request(req) => {
                            tracing::trace!("received compat request");
                            self.inject_request(BitswapChannel::Compat(peer_id), req);
                        }
                        CompatMessage::Response(cid, res) => {
                            tracing::trace!("received compat response");
                            self.inject_response(BitswapId::Compat(cid), peer_id, res);
                            // TODO ignores complete error if insert failed
                        }
                    }
                }
            }
        }
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
                        let req = BitswapRequest {
                            ty: RequestType::Have,
                            cid,
                        };
                        let rid = self.inner.send_request(&peer_id, req);
                        self.requests.insert(BitswapId::Bitswap(rid), id);
                    }
                    Request::Block(peer_id, cid) => {
                        let req = BitswapRequest {
                            ty: RequestType::Block,
                            cid,
                        };
                        let rid = self.inner.send_request(&peer_id, req);
                        self.requests.insert(BitswapId::Bitswap(rid), id);
                    }
                    Request::Providers(cid) => {
                        let event = BitswapEvent::Providers(id, cid);
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                    }
                    Request::MissingBlocks(cid) => {
                        self.insert_batch();
                        match self.store.missing_blocks(&cid) {
                            Ok(missing) => {
                                MISSING_BLOCKS_TOTAL.inc_by(missing.len() as u64);
                                self.query_manager
                                    .inject_response(id, Response::MissingBlocks(missing));
                            }
                            Err(err) => {
                                self.query_manager.cancel(id);
                                let event = BitswapEvent::Complete(id, Err(err));
                                return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                            }
                        }
                    },
                },
                QueryEvent::Progress(id, missing) => {
                    let event = BitswapEvent::Progress(id, missing);
                    return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                }
                QueryEvent::Complete(id, res) => {
                    if res.is_err() {
                        BLOCK_NOT_FOUND.inc();
                    }
                    let event =
                        BitswapEvent::Complete(id, res.map_err(|cid| BlockNotFound(cid).into()));
                    return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                }
            }
        }
        loop {
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
                        #[cfg(not(feature = "compat"))]
                        event,
                        #[cfg(feature = "compat")]
                        event: EitherOutput::First(event),
                    });
                }
                Poll::Ready(NetworkBehaviourAction::ReportObservedAddr { address, score }) => {
                    return Poll::Ready(NetworkBehaviourAction::ReportObservedAddr {
                        address,
                        score,
                    });
                }
                Poll::Pending => break,
            };
            match event {
                RequestResponseEvent::Message { peer, message } => match message {
                    RequestResponseMessage::Request {
                        request_id: _,
                        request,
                        channel,
                    } => self.inject_request(BitswapChannel::Bitswap(channel), request),
                    RequestResponseMessage::Response {
                        request_id,
                        response,
                    } => {
                        if let Some(event) =
                            self.inject_response(BitswapId::Bitswap(request_id), peer, response)
                        {
                            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                        }
                    }
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
                    match error {
                        OutboundFailure::DialFailure => {
                            OUTBOUND_FAILURE.with_label_values(&["dial_failure"]).inc();
                        }
                        OutboundFailure::Timeout => {
                            OUTBOUND_FAILURE.with_label_values(&["timeout"]).inc();
                        }
                        OutboundFailure::ConnectionClosed => {
                            OUTBOUND_FAILURE
                                .with_label_values(&["connection_closed"])
                                .inc();
                        }
                        OutboundFailure::UnsupportedProtocols => {
                            OUTBOUND_FAILURE
                                .with_label_values(&["unsupported_protocols"])
                                .inc();
                            #[cfg(feature = "compat")]
                            if let Some(id) = self.requests.remove(&BitswapId::Bitswap(request_id))
                            {
                                if let Some(info) = self.query_manager.query_info(id) {
                                    let ty = match info.label {
                                        "have" => RequestType::Have,
                                        "block" => RequestType::Block,
                                        _ => unreachable!(),
                                    };
                                    let request = BitswapRequest { ty, cid: info.cid };
                                    self.requests.insert(BitswapId::Compat(info.cid), id);
                                    tracing::trace!("adding compat peer {}", peer);
                                    self.compat.insert(peer);
                                    return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                                        peer_id: peer,
                                        handler: NotifyHandler::Any,
                                        event: EitherOutput::Second(CompatMessage::Request(request)),
                                    });
                                }
                            }
                        }
                    }
                    if let Some(id) = self.requests.remove(&BitswapId::Bitswap(request_id)) {
                        self.query_manager
                            .inject_response(id, Response::Have(peer, false));
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
                            INBOUND_FAILURE.with_label_values(&["timeout"]).inc();
                        }
                        InboundFailure::ConnectionClosed => {
                            INBOUND_FAILURE
                                .with_label_values(&["connection_closed"])
                                .inc();
                        }
                        InboundFailure::UnsupportedProtocols => {
                            INBOUND_FAILURE
                                .with_label_values(&["unsupported_protocols"])
                                .inc();
                        }
                        InboundFailure::ResponseOmission => {
                            INBOUND_FAILURE
                                .with_label_values(&["response_omission"])
                                .inc();
                        }
                    }
                }
            }
        }
        #[cfg(feature = "compat")]
        if let Some((peer_id, compat)) = self.compat_queue.pop_front() {
            return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                peer_id,
                handler: NotifyHandler::Any,
                event: EitherOutput::Second(compat),
            });
        }
        Poll::Pending
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
    use std::sync::{Arc, Mutex};
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

    #[derive(Clone, Default)]
    struct Store(Arc<Mutex<FnvHashMap<Cid, Vec<u8>>>>);

    impl BitswapStore for Store {
        type Params = DefaultParams;
        fn contains(&mut self, cid: &Cid) -> Result<bool> {
            Ok(self.0.lock().unwrap().contains_key(cid))
        }
        fn get(&mut self, cid: &Cid) -> Result<Option<Vec<u8>>> {
            Ok(self.0.lock().unwrap().get(cid).cloned())
        }
        fn insert(&mut self, block: &Block<Self::Params>) -> Result<()> {
            self.0
                .lock()
                .unwrap()
                .insert(*block.cid(), block.data().to_vec());
            Ok(())
        }
        fn missing_blocks(&mut self, cid: &Cid) -> Result<Vec<Cid>> {
            let mut stack = vec![*cid];
            let mut missing = vec![];
            while let Some(cid) = stack.pop() {
                if let Some(data) = self.get(&cid)? {
                    let block = Block::<Self::Params>::new_unchecked(cid, data);
                    block.references(&mut stack)?;
                } else {
                    missing.push(cid);
                }
            }
            Ok(missing)
        }
    }

    struct Peer {
        peer_id: PeerId,
        addr: Multiaddr,
        store: Store,
        swarm: Swarm<Bitswap<DefaultParams>>,
    }

    impl Peer {
        fn new() -> Self {
            let (peer_id, trans) = mk_transport();
            let store = Store::default();
            let mut swarm = Swarm::new(
                trans,
                Bitswap::new(BitswapConfig::new(), store.clone()),
                peer_id,
            );
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

        fn store(&mut self) -> impl std::ops::DerefMut<Target = FnvHashMap<Cid, Vec<u8>>> + '_ {
            self.store.0.lock().unwrap()
        }

        fn swarm(&mut self) -> &mut Swarm<Bitswap<DefaultParams>> {
            &mut self.swarm
        }

        fn spawn(mut self, name: &'static str) -> PeerId {
            let peer_id = self.peer_id;
            task::spawn(async move {
                loop {
                    let event = self.swarm.next().await;
                    tracing::debug!("{}: {:?}", name, event);
                }
            });
            peer_id
        }
    }

    fn assert_providers(event: BitswapEvent, cid: Cid) -> QueryId {
        if let BitswapEvent::Providers(id, cid2) = event {
            assert_eq!(cid2, cid);
            id
        } else {
            panic!("{:?} is not a provider request", event);
        }
    }

    fn assert_progress(event: BitswapEvent, id: QueryId, missing: usize) {
        if let BitswapEvent::Progress(id2, missing2) = event {
            assert_eq!(id2, id);
            assert_eq!(missing2, missing);
        } else {
            panic!("{:?} is not a progress event", event);
        }
    }

    fn assert_complete_ok(event: BitswapEvent, id: QueryId) {
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

        assert_progress(peer2.swarm().next().await, id, 1);
        assert_progress(peer2.swarm().next().await, id, 1);

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

    #[cfg(feature = "compat")]
    #[async_std::test]
    async fn compat_test() {
        tracing_try_init();
        let cid: Cid = "QmXQsqVRpp2W7fbYZHi4aB2Xkqfd3DpwWskZoLVEYigMKC"
            .parse()
            .unwrap();
        let peer_id: PeerId = "QmRSGx67Kq8w7xSBDia7hQfbfuvauMQGgxcwSWw976x4BS"
            .parse()
            .unwrap();
        let multiaddr: Multiaddr = "/ip4/54.173.33.96/tcp/4001".parse().unwrap();

        let mut peer = Peer::new();
        peer.swarm().add_address(&peer_id, multiaddr);
        let id = peer.swarm().get(cid, std::iter::once(peer_id));
        assert_complete_ok(peer.swarm().next().await, id);
    }
}
