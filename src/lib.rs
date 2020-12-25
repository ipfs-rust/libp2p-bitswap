//! Bitswap protocol implementation
#![deny(missing_docs)]
#![deny(warnings)]

mod behaviour;
mod protocol;
mod query;

pub use crate::behaviour::{Channel, Bitswap, BitswapConfig, BitswapEvent};
pub use crate::query::{Query, QueryResult, QueryType};
