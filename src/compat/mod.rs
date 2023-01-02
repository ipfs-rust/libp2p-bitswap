mod message;
mod prefix;
mod protocol;

pub use message::CompatMessage;
pub use protocol::{
    CompatProtocolInbound, CompatProtocolOutbound, InboundMessage, DEFAULT_COMPAT_PROTOCOL_NAME,
};

fn other<E: std::error::Error + Send + Sync + 'static>(e: E) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, e)
}
