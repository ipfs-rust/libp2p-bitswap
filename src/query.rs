use crate::protocol::RequestType;
use fnv::FnvHashSet;
use libipld::cid::Cid;
use libipld::error::BlockNotFound;
use libp2p::PeerId;
use std::collections::VecDeque;
use std::sync::Arc;

/// Bitswap sync trait for customizing the syncing behaviour.
pub trait BitswapSync: Send + Sync + 'static {
    /// Returns the list of blocks that need to be synced.
    fn references(&self, cid: &Cid) -> Box<dyn Iterator<Item = Cid>>;

    /// Returns if a cid needs to be synced.
    fn contains(&self, cid: &Cid) -> bool;
}

/// Query type.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QueryType {
    /// Get query.
    Get,
    /// Sync query.
    Sync,
}

impl std::fmt::Display for QueryType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Get => write!(f, "get"),
            Self::Sync => write!(f, "sync"),
        }
    }
}

/// Query.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Query {
    /// Query type.
    pub ty: QueryType,
    /// Cid.
    pub cid: Cid,
}

impl std::fmt::Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} {}", self.ty, self.cid)
    }
}

/// The result of a `Query`.
pub type QueryResult = core::result::Result<(), BlockNotFound>;

/// Event emitted by a query.
#[derive(Debug)]
pub enum QueryEvent {
    /// The query needs to know the providers of a cid.
    GetProviders(Cid),
    /// The query wants to make a have or block request to a peer for cid.
    Request(PeerId, Cid, RequestType),
    /// The query completed with a result.
    Complete(Query, QueryResult),
}

impl std::fmt::Display for QueryEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Request(peer, cid, RequestType::Have) => write!(f, "have {} {}", cid, peer),
            Self::Request(peer, cid, RequestType::Block) => write!(f, "block {} {}", cid, peer),
            Self::GetProviders(cid) => write!(f, "providers {}", cid),
            Self::Complete(query, Ok(())) => write!(f, "{} ok", query),
            Self::Complete(query, Err(BlockNotFound(cid))) => write!(f, "{} err {}", query, cid),
        }
    }
}

#[derive(Debug)]
enum GetQueryState {
    InitialProviderSet,
    ProviderQuery,
    ProviderSet,
}

#[derive(Debug)]
enum GetQueryEvent {
    GetProviders(Cid),
    Request(PeerId, Cid, RequestType),
    Complete(Result<FnvHashSet<PeerId>, ()>),
}

#[derive(Debug)]
struct GetQuery {
    cid: Cid,
    state: GetQueryState,
    requests: FnvHashSet<PeerId>,
    block_request: Option<PeerId>,
    have_set: Option<FnvHashSet<PeerId>>,
    complete: bool,
    events: VecDeque<GetQueryEvent>,
}

impl std::borrow::Borrow<Cid> for GetQuery {
    fn borrow(&self) -> &Cid {
        &self.cid
    }
}

impl std::hash::Hash for GetQuery {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        std::hash::Hash::hash(&self.cid, h);
    }
}

impl PartialEq for GetQuery {
    fn eq(&self, other: &Self) -> bool {
        self.cid == other.cid
    }
}

impl Eq for GetQuery {}

impl GetQuery {
    pub fn new(cid: Cid, initial_set: FnvHashSet<PeerId>) -> Self {
        let mut me = Self {
            cid,
            state: GetQueryState::InitialProviderSet,
            requests: Default::default(),
            block_request: None,
            have_set: Some(Default::default()),
            complete: false,
            events: VecDeque::with_capacity(initial_set.len()),
        };
        for peer in initial_set {
            me.start_request(peer);
        }
        me
    }

    fn start_request(&mut self, peer: PeerId) {
        if self.requests.contains(&peer) {
            return;
        }
        let ty = if self.block_request.is_some() {
            RequestType::Have
        } else {
            RequestType::Block
        };
        self.requests.insert(peer.clone());
        if ty == RequestType::Block {
            self.block_request = Some(peer.clone());
        }
        self.events
            .push_back(GetQueryEvent::Request(peer, self.cid, ty))
    }

    pub fn complete_request(&mut self, peer: PeerId, have: bool) {
        self.requests.remove(&peer);
        if let Some(peer_id) = self.block_request.as_ref() {
            if *peer_id == peer {
                self.block_request = None;
                self.complete = have;
            }
        }
        let have_set = self.have_set.as_mut().unwrap();
        if have {
            have_set.insert(peer);
        } else {
            have_set.remove(&peer);
        }
        if !self.complete && self.block_request.is_none() {
            let peer = have_set.iter().next().map(|peer| peer.to_owned());
            if let Some(peer) = peer {
                have_set.remove(&peer);
                self.start_request(peer);
            }
        }
    }

    pub fn add_provider(&mut self, provider: PeerId) {
        self.start_request(provider);
    }

    pub fn complete_get_providers(&mut self) {
        self.state = GetQueryState::ProviderSet;
    }

    pub fn next(&mut self) -> Option<GetQueryEvent> {
        if let Some(event) = self.events.pop_front() {
            return Some(event);
        }
        match self.state {
            GetQueryState::InitialProviderSet if self.requests.is_empty() => {
                if self.complete {
                    Some(GetQueryEvent::Complete(Ok(self
                        .have_set
                        .take()
                        .unwrap_or_default())))
                } else {
                    self.state = GetQueryState::ProviderQuery;
                    Some(GetQueryEvent::GetProviders(self.cid))
                }
            }
            GetQueryState::ProviderSet if self.requests.is_empty() => {
                let res = if self.complete {
                    Ok(self.have_set.take().unwrap_or_default())
                } else {
                    Err(())
                };
                Some(GetQueryEvent::Complete(res))
            }
            _ => None,
        }
    }
}

#[derive(Debug)]
enum SyncQueryEvent {
    Query(GetQuery),
    Complete(QueryResult),
}

struct SyncQuery {
    cid: Cid,
    syncer: Arc<dyn BitswapSync>,
    requests: FnvHashSet<Cid>,
    events: VecDeque<SyncQueryEvent>,
    visited: FnvHashSet<Cid>,
    num_requests: usize,
    num_calls_start_request: usize,
}

impl std::borrow::Borrow<Cid> for SyncQuery {
    fn borrow(&self) -> &Cid {
        &self.cid
    }
}

impl std::hash::Hash for SyncQuery {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        std::hash::Hash::hash(&self.cid, h);
    }
}

impl PartialEq for SyncQuery {
    fn eq(&self, other: &Self) -> bool {
        self.cid == other.cid
    }
}

impl Eq for SyncQuery {}

impl SyncQuery {
    pub fn new(cid: Cid, syncer: Arc<dyn BitswapSync>) -> Self {
        let mut me = Self {
            cid,
            syncer,
            requests: Default::default(),
            events: Default::default(),
            visited: Default::default(),
            num_requests: 0,
            num_calls_start_request: 0,
        };
        me.start_request(&cid, Default::default());
        me
    }

    fn start_request(&mut self, cid: &Cid, initial_set: FnvHashSet<PeerId>) {
        if self.visited.contains(cid) {
            return;
        }
        self.num_calls_start_request += 1;
        if self.syncer.contains(cid) {
            self.visited.insert(*cid);
            for cid in self.syncer.references(cid) {
                self.start_request(&cid, initial_set.clone());
            }
        } else if self.requests.insert(*cid) {
            let req = GetQuery::new(*cid, initial_set);
            self.events.push_back(SyncQueryEvent::Query(req));
            self.num_requests += 1;
        }
    }

    pub fn complete_request(&mut self, cid: &Cid, res: &Result<FnvHashSet<PeerId>, ()>) -> bool {
        if self.requests.remove(&cid) {
            match res {
                Ok(initial_set) => {
                    self.start_request(cid, initial_set.clone());
                }
                Err(()) => {
                    self.events
                        .push_back(SyncQueryEvent::Complete(Err(BlockNotFound(*cid))));
                }
            }
            true
        } else {
            false
        }
    }

    pub fn next(&mut self) -> Option<SyncQueryEvent> {
        if let Some(event) = self.events.pop_front() {
            return Some(event);
        }
        if self.requests.is_empty() {
            log::trace!("num requests {}", self.num_requests);
            log::trace!("num calls {}", self.num_calls_start_request);
            return Some(SyncQueryEvent::Complete(Ok(())));
        }
        None
    }
}

#[derive(Default)]
pub struct QueryManager {
    get: FnvHashSet<GetQuery>,
    sync: FnvHashSet<SyncQuery>,
    progress: FnvHashSet<Query>,
    user: FnvHashSet<Query>,
}

impl QueryManager {
    pub fn get(&mut self, cid: Cid) {
        if !self.get.contains(&cid) {
            let query = Query {
                ty: QueryType::Get,
                cid,
            };
            log::trace!("{}", query);
            let get = GetQuery::new(cid, Default::default());
            self.get.insert(get);
            self.progress.insert(query);
            self.user.insert(query);
        }
    }

    pub fn cancel_get(&mut self, cid: Cid) {
        self._cancel_get(cid, true);
    }

    fn _cancel_get(&mut self, cid: Cid, user: bool) {
        let query = Query {
            ty: QueryType::Get,
            cid,
        };
        if self.user.remove(&query) == user {
            self.get.remove(&cid);
            self.progress.remove(&query);
            log::trace!("cancel {}", query);
        }
    }

    pub fn sync(&mut self, cid: Cid, syncer: Arc<dyn BitswapSync>) {
        if !self.sync.contains(&cid) {
            let query = Query {
                ty: QueryType::Sync,
                cid,
            };
            log::trace!("{}", query);
            let sync = SyncQuery::new(cid, syncer);
            self.sync.insert(sync);
            self.progress.insert(query);
            self.user.insert(query);
        }
    }

    pub fn cancel_sync(&mut self, cid: Cid) {
        let query = Query {
            ty: QueryType::Sync,
            cid,
        };
        if self.user.remove(&query) {
            if let Some(sync) = self.sync.take(&cid) {
                for cid in sync.requests {
                    self._cancel_get(cid, false);
                }
            }
            self.progress.remove(&query);
            log::trace!("cancel {}", query);
        }
    }

    pub fn complete_request(&mut self, cid: Cid, peer_id: PeerId, have: bool) {
        if let Some(mut query) = self.get.take(&cid) {
            query.complete_request(peer_id, have);
            self.get.insert(query);
            self.progress.insert(Query {
                ty: QueryType::Get,
                cid,
            });
        }
    }

    pub fn add_provider(&mut self, cid: Cid, provider: PeerId) {
        if let Some(mut query) = self.get.take(&cid) {
            query.add_provider(provider);
            self.get.insert(query);
            self.progress.insert(Query {
                ty: QueryType::Get,
                cid,
            });
        }
    }

    pub fn complete_get_providers(&mut self, cid: Cid) {
        if let Some(mut query) = self.get.take(&cid) {
            query.complete_get_providers();
            self.get.insert(query);
            self.progress.insert(Query {
                ty: QueryType::Get,
                cid,
            });
        }
    }

    fn complete_get(&mut self, cid: &Cid, res: &Result<FnvHashSet<PeerId>, ()>) {
        let sync = std::mem::take(&mut self.sync);
        self.sync = sync
            .into_iter()
            .map(|mut sync| {
                if sync.complete_request(&cid, &res) {
                    self.progress.insert(Query {
                        ty: QueryType::Sync,
                        cid: sync.cid,
                    });
                }
                sync
            })
            .collect();
    }

    pub fn next(&mut self) -> Option<QueryEvent> {
        loop {
            if let Some(query) = self.progress.iter().next().cloned() {
                match query {
                    Query {
                        ty: QueryType::Get,
                        cid,
                    } => {
                        if let Some(mut get) = self.get.take(&cid) {
                            match get.next() {
                                Some(GetQueryEvent::Request(peer_id, cid, ty)) => {
                                    let event = QueryEvent::Request(peer_id, cid, ty);
                                    log::trace!("{}", event);
                                    self.get.insert(get);
                                    return Some(event);
                                }
                                Some(GetQueryEvent::GetProviders(cid)) => {
                                    let event = QueryEvent::GetProviders(cid);
                                    log::trace!("{}", event);
                                    self.get.insert(get);
                                    return Some(event);
                                }
                                Some(GetQueryEvent::Complete(res)) => {
                                    self.complete_get(&cid, &res);
                                    let query = Query {
                                        ty: QueryType::Get,
                                        cid,
                                    };
                                    let query_res = res.map(|_| ()).map_err(|_| BlockNotFound(cid));
                                    let event = QueryEvent::Complete(query, query_res);
                                    log::trace!("{}", event);
                                    if self.user.remove(&query) {
                                        return Some(event);
                                    }
                                }
                                None => {
                                    self.get.insert(get);
                                }
                            }
                        }
                        self.progress.remove(&Query {
                            ty: QueryType::Get,
                            cid,
                        });
                    }
                    Query {
                        ty: QueryType::Sync,
                        cid,
                    } => {
                        if let Some(mut sync) = self.sync.take(&cid) {
                            match sync.next() {
                                Some(SyncQueryEvent::Query(get)) => {
                                    let query = Query {
                                        ty: QueryType::Get,
                                        cid: get.cid,
                                    };
                                    log::trace!("{}", query);
                                    self.progress.insert(query);
                                    self.get.insert(get);
                                    self.sync.insert(sync);
                                }
                                Some(SyncQueryEvent::Complete(res)) => {
                                    let query = Query {
                                        ty: QueryType::Sync,
                                        cid,
                                    };
                                    if res.is_ok() {
                                        log::trace!("sync ok {}", cid);
                                    } else {
                                        log::trace!("sync err {}", cid);
                                    }
                                    if self.user.remove(&query) {
                                        return Some(QueryEvent::Complete(query, res));
                                    }
                                }
                                None => {
                                    self.sync.insert(sync);
                                }
                            }
                        }
                        self.progress.remove(&Query {
                            ty: QueryType::Sync,
                            cid,
                        });
                    }
                }
            } else {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod assert_query {
        use super::*;

        pub(super) fn is_pending(query: &mut GetQuery) {
            assert!(matches!(query.next(), None));
        }

        pub(super) fn is_completed(query: &mut GetQuery, expected_set: FnvHashSet<PeerId>) {
            assert!(
                matches!(query.next(), Some(GetQueryEvent::Complete(Ok(set))) if set == expected_set)
            );
        }

        pub(super) fn wants_have(query: &mut GetQuery, cid: &Cid) -> PeerId {
            assert_request(query, cid, RequestType::Have)
        }

        pub(super) fn wants_block(query: &mut GetQuery, cid: &Cid) -> PeerId {
            assert_request(query, cid, RequestType::Block)
        }

        pub(super) fn wants_block_from(query: &mut GetQuery, cid: &Cid, peer: &PeerId) {
            assert_eq!(assert_request(query, cid, RequestType::Block), *peer);
        }

        fn assert_request(
            query: &mut GetQuery,
            expected_cid: &Cid,
            expected_type: RequestType,
        ) -> PeerId {
            let next_ = query.next();
            let result = match &next_ {
                Some(GetQueryEvent::Request(peer_id, cid, type_))
                    if cid == expected_cid && *type_ == expected_type =>
                {
                    Some(peer_id.to_owned())
                }
                _ => None,
            };
            assert!(result.is_some(), format!("actual: {:?}", next_));
            result.unwrap()
        }
    }

    #[test]
    fn test_get_query_block_not_found() {
        let mut initial_set = FnvHashSet::default();
        for _ in 0..3 {
            initial_set.insert(PeerId::random());
        }
        let mut provider_set = FnvHashSet::default();
        for _ in 0..3 {
            provider_set.insert(PeerId::random());
        }
        let cid = Cid::default();
        let mut query = GetQuery::new(cid, initial_set.clone());
        assert!(matches!(
            query.next(),
            Some(GetQueryEvent::Request(_, _, RequestType::Block))
        ));
        assert!(matches!(
            query.next(),
            Some(GetQueryEvent::Request(_, _, RequestType::Have))
        ));
        assert!(matches!(
            query.next(),
            Some(GetQueryEvent::Request(_, _, RequestType::Have))
        ));
        for peer in initial_set {
            assert!(matches!(query.next(), None));
            query.complete_request(peer.clone(), false);
        }
        assert!(matches!(query.next(), Some(GetQueryEvent::GetProviders(_))));
        for provider in &provider_set {
            query.add_provider(provider.clone());
        }
        query.complete_get_providers();
        assert!(matches!(
            query.next(),
            Some(GetQueryEvent::Request(_, _, RequestType::Block))
        ));
        assert!(matches!(
            query.next(),
            Some(GetQueryEvent::Request(_, _, RequestType::Have))
        ));
        assert!(matches!(
            query.next(),
            Some(GetQueryEvent::Request(_, _, RequestType::Have))
        ));
        assert!(matches!(query.next(), None));
        for peer in provider_set {
            assert!(matches!(query.next(), None));
            query.complete_request(peer, false);
        }
        assert!(matches!(
            query.next(),
            Some(GetQueryEvent::Complete(Err(())))
        ));
    }

    #[test]
    fn test_cid_query_block_initial_set() {
        let mut initial_set = FnvHashSet::default();
        for _ in 0..3 {
            initial_set.insert(PeerId::random());
        }
        let cid = Cid::default();
        let mut query = GetQuery::new(cid, initial_set.clone());
        assert!(matches!(
            query.next(),
            Some(GetQueryEvent::Request(_, _, RequestType::Block))
        ));
        assert!(matches!(
            query.next(),
            Some(GetQueryEvent::Request(_, _, RequestType::Have))
        ));
        assert!(matches!(
            query.next(),
            Some(GetQueryEvent::Request(_, _, RequestType::Have))
        ));
        for peer in initial_set.clone() {
            assert!(matches!(query.next(), None));
            query.complete_request(peer, true);
        }
        assert!(
            matches!(query.next(), Some(GetQueryEvent::Complete(Ok(set))) if set == initial_set)
        );
    }

    #[test]
    fn test_get_query_block_provider_set() {
        let mut provider_set = FnvHashSet::default();
        for _ in 0..3 {
            provider_set.insert(PeerId::random());
        }
        let cid = Cid::default();
        let mut query = GetQuery::new(cid, Default::default());
        assert!(matches!(query.next(), Some(GetQueryEvent::GetProviders(_))));
        assert!(matches!(query.next(), None));
        for provider in &provider_set {
            query.add_provider(provider.clone());
        }
        query.complete_get_providers();
        assert!(matches!(
            query.next(),
            Some(GetQueryEvent::Request(_, _, RequestType::Block))
        ));
        assert!(matches!(
            query.next(),
            Some(GetQueryEvent::Request(_, _, RequestType::Have))
        ));
        assert!(matches!(
            query.next(),
            Some(GetQueryEvent::Request(_, _, RequestType::Have))
        ));
        assert!(matches!(query.next(), None));
        for peer in provider_set.clone() {
            assert!(matches!(query.next(), None));
            query.complete_request(peer, true);
        }
        assert!(
            matches!(query.next(), Some(GetQueryEvent::Complete(Ok(set))) if set == provider_set)
        );
    }

    #[test]
    fn test_gets_block_from_spare_if_first_request_fails_before_have_is_received() {
        let initial_set = {
            let mut set = FnvHashSet::default();
            set.insert(PeerId::random());
            set.insert(PeerId::random());
            set
        };
        let cid = Cid::default();
        let mut query = GetQuery::new(cid, initial_set);

        let first_peer = assert_query::wants_block(&mut query, &cid);
        let second_peer = assert_query::wants_have(&mut query, &cid);

        query.complete_request(first_peer, false);
        assert_query::is_pending(&mut query);

        query.complete_request(second_peer.to_owned(), true);
        assert_query::wants_block_from(&mut query, &cid, &second_peer);

        query.complete_request(second_peer.to_owned(), true);
        assert_query::is_completed(&mut query, {
            let mut set = FnvHashSet::default();
            set.insert(second_peer);
            set
        });
    }

    #[test]
    fn test_gets_block_from_spare_if_first_request_fails_after_have_is_received() {
        let initial_set = {
            let mut set = FnvHashSet::default();
            set.insert(PeerId::random());
            set.insert(PeerId::random());
            set
        };
        let cid = Cid::default();
        let mut query = GetQuery::new(cid, initial_set);

        let first_peer = assert_query::wants_block(&mut query, &cid);
        let second_peer = assert_query::wants_have(&mut query, &cid);

        query.complete_request(second_peer.to_owned(), true);
        assert_query::is_pending(&mut query);

        query.complete_request(first_peer, false);
        assert_query::wants_block_from(&mut query, &cid, &second_peer);

        query.complete_request(second_peer.to_owned(), true);
        assert_query::is_completed(&mut query, {
            let mut set = FnvHashSet::default();
            set.insert(second_peer);
            set
        });
    }
}
