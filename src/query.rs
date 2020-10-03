use crate::protocol::RequestType;
use fnv::FnvHashSet;
use libipld::cid::Cid;
use libipld::error::BlockNotFound;
use libp2p::PeerId;
use std::collections::VecDeque;

pub trait BitswapSync: Send + Sync + 'static {
    fn references(&self, cid: &Cid) -> Box<dyn Iterator<Item = Cid>>;
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QueryType {
    Get,
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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Query {
    pub ty: QueryType,
    pub cid: Cid,
}

impl std::fmt::Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} {}", self.ty, self.cid)
    }
}

pub type QueryResult = core::result::Result<(), BlockNotFound>;

#[derive(Debug)]
pub enum QueryEvent {
    GetProviders(Cid),
    Request(PeerId, Cid, RequestType),
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
        if have {
            self.have_set.as_mut().unwrap().insert(peer.clone());
        } else {
            self.have_set.as_mut().unwrap().remove(&peer);
        }
        if !self.complete && have && self.block_request.is_none() {
            self.start_request(peer);
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
    syncer: Box<dyn BitswapSync>,
    requests: FnvHashSet<Cid>,
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
    pub fn new(cid: Cid, syncer: Box<dyn BitswapSync>) -> Self {
        let mut me = Self {
            cid,
            syncer,
            requests: Default::default(),
            events: Default::default(),
        };
        me.start_request(cid, Default::default());
        me
    }

    fn start_request(&mut self, cid: Cid, initial_set: FnvHashSet<PeerId>) {
        if self.requests.insert(cid) {
            let req = GetQuery::new(cid, initial_set);
            self.events.push_back(SyncQueryEvent::Query(req));
        }
    }

    pub fn complete_request(&mut self, cid: &Cid, res: &Result<FnvHashSet<PeerId>, ()>) -> bool {
        if self.requests.remove(&cid) {
            match res {
                Ok(initial_set) => {
                    for cid in self.syncer.references(cid) {
                        self.start_request(cid, initial_set.clone());
                    }
                    if self.requests.is_empty() {
                        self.events.push_back(SyncQueryEvent::Complete(Ok(())));
                    }
                }
                Err(()) => {
                    self.events
                        .push_back(SyncQueryEvent::Complete(Err(BlockNotFound(*cid))));
                }
            }
        }
        !self.events.is_empty()
    }

    pub fn next(&mut self) -> Option<SyncQueryEvent> {
        self.events.pop_front()
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

    pub fn sync(&mut self, cid: Cid, syncer: Box<dyn BitswapSync>) {
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
}
