use crate::protocol::RequestType;
use fnv::FnvHashSet;
use libipld::cid::Cid;
use libipld::error::BlockNotFound;
use libp2p::PeerId;
use std::collections::VecDeque;

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
    /// Missing blocks.
    MissingBlocks(Cid),
}

impl std::fmt::Display for QueryEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Request(peer, cid, RequestType::Have) => write!(f, "have {} {}", cid, peer),
            Self::Request(peer, cid, RequestType::Block) => write!(f, "block {} {}", cid, peer),
            Self::GetProviders(cid) => write!(f, "providers {}", cid),
            Self::Complete(query, Ok(())) => write!(f, "{} ok", query),
            Self::Complete(query, Err(BlockNotFound(cid))) => write!(f, "{} err {}", query, cid),
            Self::MissingBlocks(cid) => write!(f, "missing blocks {}", cid),
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
        self.requests.insert(peer);
        if ty == RequestType::Block {
            self.block_request = Some(peer);
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
            let peer = have_set.iter().cloned().next();
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
    MissingBlocks(Cid),
    Complete(QueryResult),
}

struct SyncQuery {
    cid: Cid,
    initial_set: FnvHashSet<PeerId>,
    missing: FnvHashSet<Cid>,
    not_missing: FnvHashSet<Cid>,
    events: VecDeque<SyncQueryEvent>,
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
    pub fn new(cid: Cid, missing: FnvHashSet<Cid>) -> Self {
        let mut me = Self {
            cid,
            initial_set: Default::default(),
            missing: Default::default(),
            not_missing: Default::default(),
            events: Default::default(),
        };
        me.add_missing(missing);
        me
    }

    pub fn add_missing(&mut self, missing: FnvHashSet<Cid>) {
        if missing.is_empty() {
            self.events.push_back(SyncQueryEvent::Complete(Ok(())));
            return;
        }
        for cid in missing {
            if !self.not_missing.contains(&cid) {
                let req = GetQuery::new(cid, self.initial_set.clone());
                self.events.push_back(SyncQueryEvent::Query(req));
                self.missing.insert(cid);
            }
        }
    }

    pub fn complete_request(&mut self, cid: &Cid, res: &Result<FnvHashSet<PeerId>, ()>) -> bool {
        if !self.missing.remove(&cid) {
            return false;
        }
        match res {
            Ok(initial_set) => {
                for peer in initial_set {
                    self.initial_set.insert(*peer);
                }
                self.not_missing.insert(*cid);
                self.events
                    .push_back(SyncQueryEvent::MissingBlocks(self.cid));
            }
            Err(()) => {
                self.events
                    .push_back(SyncQueryEvent::Complete(Err(BlockNotFound(*cid))));
            }
        }
        true
    }

    pub fn next(&mut self) -> Option<SyncQueryEvent> {
        if let Some(event) = self.events.pop_front() {
            return Some(event);
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
            tracing::trace!("{}", query);
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
            tracing::trace!("cancel {}", query);
        }
    }

    pub fn sync(&mut self, cid: Cid, missing: FnvHashSet<Cid>) {
        if !self.sync.contains(&cid) {
            let query = Query {
                ty: QueryType::Sync,
                cid,
            };
            tracing::trace!("{}", query);
            let sync = SyncQuery::new(cid, missing);
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
                for cid in sync.missing {
                    self._cancel_get(cid, false);
                }
            }
            self.progress.remove(&query);
            tracing::trace!("cancel {}", query);
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

    pub fn add_missing(&mut self, cid: Cid, missing: FnvHashSet<Cid>) {
        if let Some(mut query) = self.sync.take(&cid) {
            query.add_missing(missing);
            self.sync.insert(query);
            self.progress.insert(Query {
                ty: QueryType::Sync,
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
                                    tracing::trace!("{}", event);
                                    self.get.insert(get);
                                    return Some(event);
                                }
                                Some(GetQueryEvent::GetProviders(cid)) => {
                                    let event = QueryEvent::GetProviders(cid);
                                    tracing::trace!("{}", event);
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
                                    tracing::trace!("{}", event);
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
                                    tracing::trace!("{}", query);
                                    self.progress.insert(query);
                                    self.get.insert(get);
                                    self.sync.insert(sync);
                                }
                                Some(SyncQueryEvent::MissingBlocks(cid)) => {
                                    self.sync.insert(sync);
                                    return Some(QueryEvent::MissingBlocks(cid));
                                }
                                Some(SyncQueryEvent::Complete(res)) => {
                                    let query = Query {
                                        ty: QueryType::Sync,
                                        cid,
                                    };
                                    if res.is_ok() {
                                        tracing::trace!("sync ok {}", cid);
                                    } else {
                                        tracing::trace!("sync err {}", cid);
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
            query.complete_request(peer, false);
        }
        assert!(matches!(query.next(), Some(GetQueryEvent::GetProviders(_))));
        for provider in &provider_set {
            query.add_provider(*provider);
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
            query.add_provider(*provider);
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
    fn test_get_query_gets_from_spare_if_block_request_fails() {
        let cid = Cid::default();
        let mut query = GetQuery::new(cid, {
            let mut set = FnvHashSet::default();
            set.insert(PeerId::random());
            set.insert(PeerId::random());
            set
        });

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
    fn test_get_query_gets_from_spare_if_block_request_fails_after_have_is_received() {
        let cid = Cid::default();
        let mut query = GetQuery::new(cid, {
            let mut set = FnvHashSet::default();
            set.insert(PeerId::random());
            set.insert(PeerId::random());
            set
        });

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
}
