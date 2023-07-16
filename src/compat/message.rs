use crate::compat::other;
use crate::compat::prefix::Prefix;
use crate::protocol::{BitswapRequest, BitswapResponse, RequestType};
use libipld::Cid;
use quick_protobuf::{BytesReader, MessageRead, MessageWrite, Writer};
use std::convert::TryFrom;
use std::io;

mod bitswap_pb {
    pub use super::super::pb::bitswap_pb::Message;
    pub mod message {
        use super::super::super::pb::bitswap_pb::mod_Message as message;
        pub use message::mod_Wantlist as wantlist;
        pub use message::Wantlist;
        pub use message::{Block, BlockPresence, BlockPresenceType};
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompatMessage {
    Request(BitswapRequest),
    Response(Cid, BitswapResponse),
}

impl CompatMessage {
    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let mut msg = bitswap_pb::Message::default();
        match self {
            CompatMessage::Request(BitswapRequest { ty, cid }) => {
                let mut wantlist = bitswap_pb::message::Wantlist::default();
                let entry = bitswap_pb::message::wantlist::Entry {
                    block: cid.to_bytes().into(),
                    wantType: match ty {
                        RequestType::Have => bitswap_pb::message::wantlist::WantType::Have,
                        RequestType::Block => bitswap_pb::message::wantlist::WantType::Block,
                    } as _,
                    sendDontHave: true,
                    cancel: false,
                    priority: 1,
                };
                wantlist.entries.push(entry);
                msg.wantlist = Some(wantlist);
            }
            CompatMessage::Response(cid, BitswapResponse::Have(have)) => {
                let block_presence = bitswap_pb::message::BlockPresence {
                    cid: cid.to_bytes().into(),
                    type_pb: if *have {
                        bitswap_pb::message::BlockPresenceType::Have
                    } else {
                        bitswap_pb::message::BlockPresenceType::DontHave
                    } as _,
                };
                msg.blockPresences.push(block_presence);
            }
            CompatMessage::Response(cid, BitswapResponse::Block(bytes)) => {
                let payload = bitswap_pb::message::Block {
                    prefix: Prefix::from(cid).to_bytes().into(),
                    data: bytes.into(),
                };
                msg.payload.push(payload);
            }
        }

        let mut bytes = Vec::with_capacity(msg.get_size());
        let mut writer = Writer::new(&mut bytes);
        msg.write_message(&mut writer).map_err(other)?;
        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> io::Result<Vec<Self>> {
        let mut reader = BytesReader::from_bytes(bytes);
        let msg = bitswap_pb::Message::from_reader(&mut reader, bytes).map_err(other)?;

        let mut parts = vec![];
        for entry in msg.wantlist.unwrap_or_default().entries {
            if !entry.sendDontHave {
                tracing::error!("message hasn't set `send_dont_have`: skipping");
                continue;
            }
            let cid = Cid::try_from(&*entry.block).map_err(other)?;
            let ty = match entry.wantType {
                ty if bitswap_pb::message::wantlist::WantType::Have == ty => RequestType::Have,
                ty if bitswap_pb::message::wantlist::WantType::Block == ty => RequestType::Block,
                _ => {
                    tracing::error!("invalid request type: skipping");
                    continue;
                }
            };
            parts.push(CompatMessage::Request(BitswapRequest { ty, cid }));
        }
        for payload in msg.payload {
            let prefix = Prefix::new(&payload.prefix)?;
            let cid = prefix.to_cid(&payload.data)?;
            parts.push(CompatMessage::Response(
                cid,
                BitswapResponse::Block(payload.data.to_vec()),
            ));
        }
        for presence in msg.blockPresences {
            let cid = Cid::try_from(&*presence.cid).map_err(other)?;
            let have = match presence.type_pb {
                ty if bitswap_pb::message::BlockPresenceType::Have == ty => true,
                ty if bitswap_pb::message::BlockPresenceType::DontHave == ty => false,
                _ => {
                    tracing::error!("invalid block presence type: skipping");
                    continue;
                }
            };
            parts.push(CompatMessage::Response(cid, BitswapResponse::Have(have)));
        }
        Ok(parts)
    }
}
