//! Bitswap protocol implementation
#![deny(missing_docs)]
#![deny(warnings)]

mod behaviour;
mod protocol;
mod query;

pub use crate::behaviour::{Bitswap, BitswapConfig, BitswapEvent, BitswapStore};
pub use crate::query::{BitswapSync, Query, QueryResult, QueryType};
