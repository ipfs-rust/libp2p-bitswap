/// Bitswap wire protocol.
use crate::error::BitswapError;
use crate::message::BitswapMessage;
use core::iter;
use futures::io::{AsyncRead, AsyncWrite};
use futures::sink::SinkExt;
use futures::stream::TryStreamExt;
use futures::{future, sink, stream};
use futures_codec::{BytesMut, Framed};
use libp2p::core::{InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use std::io;
use std::marker::PhantomData;
use tiny_multihash::{MultihashCode, U64};
use unsigned_varint::codec::UviBytes;

#[derive(Clone, Copy, Debug)]
pub struct BitswapProtocolConfig<MH> {
    pub _marker: PhantomData<MH>,
    /// Maximum allowed size of a packet.
    pub max_packet_size: usize,
}

impl<MH> UpgradeInfo for BitswapProtocolConfig<MH> {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/ipfs/bitswap/2.0.0")
    }
}

impl<MH, C> InboundUpgrade<C> for BitswapProtocolConfig<MH>
where
    MH: MultihashCode<AllocSize = U64>,
    C: AsyncRead + AsyncWrite + Unpin,
{
    type Output = BitswapStreamSink<C>;
    type Error = BitswapError;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, incoming: C, _: Self::Info) -> Self::Future {
        let mut codec = UviBytes::default();
        codec.set_max_len(self.max_packet_size);

        future::ok(
            Framed::new(incoming, codec)
                .err_into()
                .with::<_, _, fn(_) -> _, _>(|message: BitswapMessage| {
                    future::ready(Ok(io::Cursor::new(message.into_bytes())))
                })
                .and_then::<_, fn(_) -> _>(|bytes| {
                    future::ready(BitswapMessage::from_bytes::<MH>(&bytes))
                }),
        )
    }
}

impl<MH, C> OutboundUpgrade<C> for BitswapProtocolConfig<MH>
where
    MH: MultihashCode<AllocSize = U64>,
    C: AsyncRead + AsyncWrite + Unpin,
{
    type Output = BitswapStreamSink<C>;
    type Error = BitswapError;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    #[inline]
    fn upgrade_outbound(self, outgoing: C, _: Self::Info) -> Self::Future {
        let mut codec = UviBytes::default();
        codec.set_max_len(self.max_packet_size);

        future::ok(
            Framed::new(outgoing, codec)
                .err_into()
                .with::<_, _, fn(_) -> _, _>(|message: BitswapMessage| {
                    future::ready(Ok(io::Cursor::new(message.into_bytes())))
                })
                .and_then::<_, fn(_) -> _>(|bytes| {
                    future::ready(BitswapMessage::from_bytes::<MH>(&bytes))
                }),
        )
    }
}

pub type BitswapStreamSink<C> = stream::AndThen<
    sink::With<
        stream::ErrInto<Framed<C, UviBytes<io::Cursor<Vec<u8>>>>, BitswapError>,
        io::Cursor<Vec<u8>>,
        BitswapMessage,
        future::Ready<Result<io::Cursor<Vec<u8>>, BitswapError>>,
        fn(BitswapMessage) -> future::Ready<Result<io::Cursor<Vec<u8>>, BitswapError>>,
    >,
    future::Ready<Result<BitswapMessage, BitswapError>>,
    fn(BytesMut) -> future::Ready<Result<BitswapMessage, BitswapError>>,
>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::tests::create_cid;
    use async_std::net::{TcpListener, TcpStream};
    use futures::prelude::*;
    use libp2p::core::upgrade;
    use tiny_multihash::Code;

    #[async_std::test]
    async fn test_upgrade() {
        env_logger::try_init().ok();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_addr = listener.local_addr().unwrap();

        let protocol_config = BitswapProtocolConfig {
            _marker: PhantomData::<Code>,
            max_packet_size: crate::DEFAULT_MAX_PACKET_SIZE,
        };
        let data = vec![42u8; crate::DEFAULT_MAX_PACKET_SIZE - 14];
        let cid = create_cid(&data);
        let message = BitswapMessage::BlockResponse(cid, data);
        let message2 = message.clone();

        let server = async move {
            let incoming = listener.incoming().into_future().await.0.unwrap().unwrap();
            let mut channel = upgrade::apply_inbound(incoming, protocol_config)
                .await
                .unwrap();
            let message = channel.next().await.unwrap().unwrap();
            assert_eq!(message, message2);
        };

        let client = async move {
            let stream = TcpStream::connect(&listener_addr).await.unwrap();
            let mut channel =
                upgrade::apply_outbound(stream, protocol_config, upgrade::Version::V1)
                    .await
                    .unwrap();
            channel.send(message).await.unwrap();
        };

        future::select(Box::pin(server), Box::pin(client)).await;
    }

    #[async_std::test]
    #[should_panic]
    async fn test_max_message_size() {
        env_logger::try_init().ok();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_addr = listener.local_addr().unwrap();

        let protocol_config = BitswapProtocolConfig {
            _marker: PhantomData::<Code>,
            max_packet_size: crate::DEFAULT_MAX_PACKET_SIZE,
        };
        let data = vec![42u8; crate::DEFAULT_MAX_PACKET_SIZE - 13];
        let cid = create_cid(&data);
        let message = BitswapMessage::BlockResponse(cid, data);

        let server = async move {
            let incoming = listener.incoming().into_future().await.0.unwrap().unwrap();
            let mut channel = upgrade::apply_inbound(incoming, protocol_config)
                .await
                .unwrap();
            channel.next().await.unwrap().unwrap();
        };

        let client = async move {
            let stream = TcpStream::connect(&listener_addr).await.unwrap();
            let mut channel =
                upgrade::apply_outbound(stream, protocol_config, upgrade::Version::V1)
                    .await
                    .unwrap();
            channel.send(message).await.unwrap();
        };

        future::select(Box::pin(server), Box::pin(client)).await;
    }
}
