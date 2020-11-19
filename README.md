# libp2p-bitswap
Implementation of the bitswap protocol.

## Efficiently syncing dags of blocks

Bitswap is a very simple protocol. It was adapted and simplified for ipfs-embed. The message
format can be represented by the following enums.

```rust
pub enum BitswapRequest {
    Have(Cid),
    Block(Cid),
}

pub enum BitswapResponse {
    Have(bool),
    Block(Vec<u8>),
}
```

The mechanism for locating providers can be abstracted. A dht can be plugged in or a centralized
db query. The bitswap api looks as follows:

```rust
pub enum Query {
    Get(Cid),
    Sync(Cid),
}

pub enum BitswapEvent {
    GetProviders(Cid),
    QueryComplete(Query, Result<()>),
}

impl Bitswap {
    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) { .. }
    pub fn get(&mut self, cid: Cid) { .. }
    pub fn cancel_get(&mut self, cid: Cid) { .. }
    pub fn add_provider(&mut self, cid: Cid, peer_id: PeerId) { .. }
    pub fn complete_get_providers(&mut self, cid: Cid) { .. }
    pub fn poll(&mut self, cx: &mut Context) -> BitswapEvent { .. }
}
```

So what happens when you create a get request? First all the providers in the initial set
are queried with the have request. As an optimization, in every batch of queries a block
request is sent instead. If the get query finds a block it returns a query complete. If the
block wasn't found in the initial set, a `GetProviders(Cid)` event is emitted. This is where
the bitswap consumer tries to locate providers by for example performing a dht lookup. These
providers are registered by calling the `add_provider` method. After the locating of providers
completes, it is signaled by calling `complete_get_providers`. The query manager then performs
bitswap requests using the new provider set which results in the block being found or a block
not found error.

Often we want to sync an entire dag of blocks. We can efficiently sync dags of blocks by adding
a sync query that runs get queries in parallel for all the references of a block. The set of
providers that had a block is used as the initial set in a reference query. For this we extend
our api with the following calls.

```rust
/// Bitswap sync trait for customizing the syncing behaviour.
pub trait BitswapSync {
    /// Returns the list of blocks that need to be synced.
    fn references(&self, cid: &Cid) -> Box<dyn Iterator<Item = Cid>>;
    /// Returns if a cid needs to be synced.
    fn contains(&self, cid: &Cid) -> bool;
}

impl Bitswap {
    pub fn sync(&mut self, cid: Cid, syncer: Arc<dyn BitswapSync>) { .. }
    pub fn cancel_sync(&mut self, cid: Cid) { .. }
}
```

## License
MIT OR Apache-2.0
