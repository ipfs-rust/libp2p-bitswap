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
use crate::query::{BitswapSync, Query, QueryEvent, QueryManager, QueryResult};
use fnv::FnvHashMap;
use futures::task::{Context, Poll};
use libipld::cid::Cid;
use libipld::multihash::MultihashCode;
use libipld::store::StoreParams;
use libp2p::core::connection::{ConnectionId, ListenerId};
use libp2p::core::{ConnectedPoint, Multiaddr, PeerId};
use libp2p::request_response::{
    throttled::Event as ThrottledEvent, ProtocolSupport, RequestId, RequestResponseConfig,
    RequestResponseEvent, RequestResponseMessage, Throttled,
};
use libp2p::swarm::ProtocolsHandler;
use libp2p::swarm::{NetworkBehaviour, NetworkBehaviourAction, PollParameters};
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::error::Error;
use std::marker::PhantomData;
use std::num::NonZeroU16;
use std::time::Duration;

pub trait BitswapStore: Clone + Send + Sync + 'static {
    fn contains(&self, cid: &Cid) -> bool;
    fn get(&self, cid: &Cid) -> Option<Vec<u8>>;
    fn insert(&self, cid: Cid, data: Vec<u8>);
}

#[derive(Clone, Debug)]
pub enum BitswapEvent {
    GetProviders(Cid),
    QueryComplete(Query, QueryResult),
}

/// Bitswap configuration.
pub struct BitswapConfig<P, S> {
    _marker: PhantomData<P>,
    store: S,
    pub credit_timeout: Duration,
    pub request_timeout: Duration,
    pub connection_keep_alive: Duration,
    pub receive_limit: NonZeroU16,
}

impl<P: StoreParams, S: BitswapStore> BitswapConfig<P, S> {
    pub fn new(store: S) -> Self {
        Self {
            _marker: PhantomData,
            store,
            credit_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(3),
            connection_keep_alive: Duration::from_secs(10),
            receive_limit: NonZeroU16::new(20).expect("20 > 0"),
        }
    }
}

/// Network behaviour that handles sending and receiving IPFS blocks.
pub struct Bitswap<P: StoreParams, S: BitswapStore> {
    /// Bitswap config.
    config: BitswapConfig<P, S>,
    /// Inner behaviour.
    inner: Throttled<BitswapCodec<P>>,
    /// Query manager.
    query_manager: QueryManager,
    /// Pending requests.
    pending_requests: VecDeque<(PeerId, BitswapRequest)>,
    /// Requests.
    requests: FnvHashMap<RequestId, Cid>,
}

impl<P: StoreParams, S: BitswapStore> Bitswap<P, S> {
    /// Creates a new `Bitswap`.
    pub fn new(config: BitswapConfig<P, S>) -> Self {
        let mut rr_config = RequestResponseConfig::default();
        rr_config.set_connection_keep_alive(config.connection_keep_alive);
        rr_config.set_request_timeout(config.request_timeout);
        let protocols = std::iter::once((BitswapProtocol, ProtocolSupport::Full));
        let mut inner = Throttled::new(BitswapCodec::<P>::default(), protocols, rr_config);
        inner.set_receive_limit(config.receive_limit);
        Self {
            config,
            inner,
            query_manager: Default::default(),
            pending_requests: Default::default(),
            requests: Default::default(),
        }
    }

    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.inner.add_address(peer_id, addr);
    }

    pub fn get(&mut self, cid: Cid) {
        self.query_manager.get(cid);
    }

    pub fn sync(&mut self, cid: Cid, syncer: Box<dyn BitswapSync>) {
        self.query_manager.sync(cid, syncer);
    }

    pub fn add_provider(&mut self, cid: Cid, peer_id: PeerId) {
        self.query_manager.add_provider(cid, peer_id);
    }

    pub fn complete_get_providers(&mut self, cid: Cid) {
        self.query_manager.complete_get_providers(cid);
    }
}

impl<P: StoreParams, S: BitswapStore> NetworkBehaviour for Bitswap<P, S> {
    type ProtocolsHandler = <Throttled<BitswapCodec<P>> as NetworkBehaviour>::ProtocolsHandler;
    type OutEvent = BitswapEvent;

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
                    self.pending_requests.push_back((peer, request));
                }
                QueryEvent::GetProviders(cid) => {
                    let event = BitswapEvent::GetProviders(cid);
                    return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                }
                QueryEvent::Complete(query, res) => {
                    let event = BitswapEvent::QueryComplete(query, res);
                    return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                }
            }
        }
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
                        event,
                    });
                }
                Poll::Ready(NetworkBehaviourAction::ReportObservedAddr { address }) => {
                    return Poll::Ready(NetworkBehaviourAction::ReportObservedAddr { address });
                }
                Poll::Pending => return Poll::Pending,
            };
            let event = match event {
                ThrottledEvent::Event(event) => event,
                ThrottledEvent::ResumeSending(_peer_id) => continue,
                ThrottledEvent::TooManyInboundRequests(peer_id) => {
                    log::info!("too many inbound requests from {}", peer_id);
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
                            let have = self.config.store.contains(&cid);
                            let response = BitswapResponse::Have(have);
                            self.inner.send_response(channel, response);
                        }
                        BitswapRequest {
                            ty: RequestType::Block,
                            cid,
                        } => {
                            let response = if let Some(data) = self.config.store.get(&cid) {
                                BitswapResponse::Block(data)
                            } else {
                                BitswapResponse::Have(false)
                            };
                            self.inner.send_response(channel, response);
                        }
                    },
                    RequestResponseMessage::Response {
                        request_id,
                        response,
                    } => match response {
                        BitswapResponse::Have(have) => {
                            let cid = self.requests.remove(&request_id).unwrap();
                            self.query_manager.complete_request(cid, peer, have);
                        }
                        BitswapResponse::Block(data) => {
                            let cid = self.requests.remove(&request_id).unwrap();
                            if verify_cid::<P>(&cid, &data) {
                                self.config.store.insert(cid, data);
                                self.query_manager.complete_request(cid, peer, true);
                            } else {
                                self.query_manager.complete_request(cid, peer, false);
                            }
                        }
                    },
                },
                RequestResponseEvent::OutboundFailure {
                    peer,
                    request_id,
                    error,
                } => {
                    log::error!(
                        "bitswap outbound failure {} {} {:?}",
                        peer,
                        request_id,
                        error
                    );
                    let cid = self.requests.remove(&request_id).unwrap();
                    self.query_manager.complete_request(cid, peer, false);
                }
                RequestResponseEvent::InboundFailure {
                    peer,
                    request_id,
                    error,
                } => {
                    log::error!(
                        "bitswap inbound failure {} {} {:?}",
                        peer,
                        request_id,
                        error
                    );
                }
            }
        }
    }
}

fn verify_cid<P: StoreParams>(cid: &Cid, data: &[u8]) -> bool {
    if let Ok(code) = P::Hashes::try_from(cid.hash().code()) {
        if code.digest(&data).digest() == cid.hash().digest() {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::QueryType;
    use async_std::task;
    use futures::prelude::*;
    use libipld::block::Block;
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::ipld::Ipld;
    use libipld::multihash::Code;
    use libipld::store::DefaultParams;
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::boxed::Boxed;
    use libp2p::core::transport::upgrade::Version;
    use libp2p::identity;
    use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
    use libp2p::tcp::TcpConfig;
    use libp2p::yamux::Config as YamuxConfig;
    use libp2p::{PeerId, Swarm, Transport};
    use std::io::{Error, ErrorKind};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

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

    fn create_block(ipld: Ipld) -> Block<DefaultParams> {
        Block::encode(DagCborCodec, Code::Blake3_256, &ipld).unwrap()
    }

    #[derive(Clone, Default)]
    struct Store(Arc<Mutex<FnvHashMap<Cid, Vec<u8>>>>);

    impl BitswapStore for Store {
        fn contains(&self, cid: &Cid) -> bool {
            self.0.lock().unwrap().contains_key(cid)
        }

        fn get(&self, cid: &Cid) -> Option<Vec<u8>> {
            self.0.lock().unwrap().get(cid).cloned()
        }

        fn insert(&self, cid: Cid, data: Vec<u8>) {
            self.0.lock().unwrap().insert(cid, data);
        }
    }

    struct Peer {
        peer_id: PeerId,
        addr: Multiaddr,
        store: Store,
        swarm: Swarm<Bitswap<DefaultParams, Store>>,
    }

    impl Peer {
        fn new() -> Self {
            let (peer_id, trans) = mk_transport();
            let store = Store::default();
            let mut swarm = Swarm::new(
                trans,
                Bitswap::new(BitswapConfig::new(store.clone())),
                peer_id.clone(),
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

        fn store(&self) -> &Store {
            &self.store
        }

        fn swarm(&mut self) -> &mut Swarm<Bitswap<DefaultParams, Store>> {
            &mut self.swarm
        }

        fn spawn(mut self, name: &'static str) -> PeerId {
            let peer_id = self.peer_id.clone();
            task::spawn(async move {
                let e = self.swarm.next().await;
                log::debug!("{}: {:?}", name, e);
            });
            peer_id
        }
    }

    struct Syncer(Store);

    impl BitswapSync for Syncer {
        fn references(&self, cid: &Cid) -> Box<dyn Iterator<Item = Cid>> {
            if let Some(data) = self.0.get(cid) {
                let block = Block::<DefaultParams>::new_unchecked(*cid, data);
                if let Ok(refs) = block.references() {
                    return Box::new(refs.into_iter());
                }
            }
            Box::new(std::iter::empty())
        }
    }

    #[async_std::test]
    async fn test_bitswap_get() {
        env_logger::try_init().ok();
        let peer1 = Peer::new();
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
        assert_eq!(peer2.store().get(block.cid()), Some(block.data().to_vec()));
    }

    #[async_std::test]
    async fn test_bitswap_sync() {
        env_logger::try_init().ok();
        let peer1 = Peer::new();
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

        let syncer = Box::new(Syncer(peer2.store().clone()));
        peer2.swarm().sync(*b2.cid(), syncer);
        assert!(matches!(
            peer2.swarm().next().await,
            BitswapEvent::GetProviders(cid) if &cid == b2.cid()
        ));
        peer2.swarm().add_provider(*b2.cid(), peer1.clone());
        peer2.swarm().complete_get_providers(*b2.cid());

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
        assert_eq!(peer2.store().get(b0.cid()), Some(b0.data().to_vec()));
        assert_eq!(peer2.store().get(b1.cid()), Some(b1.data().to_vec()));
        assert_eq!(peer2.store().get(b2.cid()), Some(b2.data().to_vec()));
    }
}
