use crate::compat::{other, CompatMessage};
use futures::future::BoxFuture;
use futures::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::core::{upgrade, InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use std::{io, iter};

// 2MB Block Size according to the specs at https://github.com/ipfs/specs/blob/main/BITSWAP.md
const MAX_BUF_SIZE: usize = 2_097_152;

#[derive(Clone, Debug, Default)]
pub struct CompatProtocol;

impl UpgradeInfo for CompatProtocol {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/ipfs/bitswap/1.2.0")
    }
}

impl<TSocket> InboundUpgrade<TSocket> for CompatProtocol
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = InboundMessage;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            tracing::trace!("upgrading inbound");
            let packet = upgrade::read_length_prefixed(&mut socket, MAX_BUF_SIZE)
                .await
                .map_err(|err| {
                    tracing::debug!(%err, "inbound upgrade error");
                    other(err)
                })?;
            socket.close().await?;
            tracing::trace!("inbound upgrade done, closing");
            let message = CompatMessage::from_bytes(&packet).map_err(|e| {
                tracing::debug!(%e, "inbound upgrade error");
                e
            })?;
            tracing::trace!("inbound upgrade closed");
            Ok(InboundMessage(message))
        })
    }
}

impl UpgradeInfo for CompatMessage {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/ipfs/bitswap/1.2.0")
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for CompatMessage
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = ();
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let bytes = self.to_bytes()?;
            upgrade::write_length_prefixed(&mut socket, bytes).await?;
            socket.close().await?;
            Ok(())
        })
    }
}

#[derive(Debug)]
pub struct InboundMessage(pub Vec<CompatMessage>);

impl From<()> for InboundMessage {
    fn from(_: ()) -> Self {
        Self(Default::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{BitswapRequest, RequestType};
    use async_std::net::{TcpListener, TcpStream};
    use futures::prelude::*;
    use libipld::Cid;
    use libp2p::core::upgrade;

    #[async_std::test]
    async fn test_upgrade() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_addr = listener.local_addr().unwrap();

        let server = async move {
            let incoming = listener.incoming().into_future().await.0.unwrap().unwrap();
            upgrade::apply_inbound(incoming, CompatProtocol)
                .await
                .unwrap();
        };

        let client = async move {
            let stream = TcpStream::connect(&listener_addr).await.unwrap();
            upgrade::apply_outbound(
                stream,
                CompatMessage::Request(BitswapRequest {
                    ty: RequestType::Have,
                    cid: Cid::default(),
                }),
                upgrade::Version::V1,
            )
            .await
            .unwrap();
        };

        future::select(Box::pin(server), Box::pin(client)).await;
    }
}
