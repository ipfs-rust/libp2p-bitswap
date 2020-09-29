//! Bitswap protocol implementation
mod behaviour;
mod error;
mod handler;
mod message;
mod prefix;
mod protocol;

pub use crate::behaviour::{Bitswap, BitswapConfig, BitswapEvent};
pub use crate::error::BitswapError;

// Undocumented, but according to JS we our messages have a max size of 512*1024
// https://github.com/ipfs/js-ipfs-bitswap/blob/d8f80408aadab94c962f6b88f343eb9f39fa0fcc/src/decision-engine/index.js#L16
pub const DEFAULT_MAX_PACKET_SIZE: usize = 1_048_575;
