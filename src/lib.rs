//! Bitswap protocol implementation
#![deny(missing_docs)]
#![deny(warnings)]

mod behaviour;
mod protocol;
mod query;
mod stats;

pub use crate::behaviour::{Bitswap, BitswapConfig, BitswapEvent, BitswapStore, Channel};
pub use crate::query::QueryId;
