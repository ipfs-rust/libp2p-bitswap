use crate::error::BitswapError;
use crate::prefix::Prefix;
use core::convert::TryFrom;
use prost::Message;
use tiny_cid::Cid;
use tiny_multihash::MultihashDigest;

mod bitswap_pb {
    include!(concat!(env!("OUT_DIR"), "/bitswap_pb.rs"));
}

/// A bitswap message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BitswapMessage {
    HaveRequest(Cid),
    BlockRequest(Cid),
    HaveResponse(Cid, bool),
    BlockResponse(Cid, Vec<u8>),
}

impl BitswapMessage {
    pub fn encode(self) -> bitswap_pb::Message {
        let mut msg = bitswap_pb::Message::default();
        match self {
            Self::HaveRequest(cid) => {
                let mut entry = bitswap_pb::message::wantlist::Entry::default();
                entry.block = cid.to_bytes();
                entry.want_type = 1;
                let mut wantlist = bitswap_pb::message::Wantlist::default();
                wantlist.entries.push(entry);
                msg.wantlist = Some(wantlist);
            }
            Self::BlockRequest(cid) => {
                let mut entry = bitswap_pb::message::wantlist::Entry::default();
                entry.block = cid.to_bytes();
                let mut wantlist = bitswap_pb::message::Wantlist::default();
                wantlist.entries.push(entry);
                msg.wantlist = Some(wantlist);
            }
            Self::HaveResponse(cid, have) => {
                let mut presence = bitswap_pb::message::BlockPresence::default();
                presence.cid = cid.to_bytes();
                presence.r#type = if have { 0 } else { 1 };
                msg.block_presences.push(presence);
            }
            Self::BlockResponse(cid, data) => {
                let mut block = bitswap_pb::message::Block::default();
                block.prefix = Prefix::from(&cid).to_bytes();
                block.data = data;
                msg.payload.push(block);
            }
        };
        msg
    }

    pub fn into_bytes(self) -> Vec<u8> {
        let msg = self.encode();
        let mut buf = Vec::with_capacity(msg.encoded_len());
        msg.encode(&mut buf)
            .expect("Vec<u8> provides capacity as needed");
        buf
    }

    pub fn decode<M: MultihashDigest>(mut msg: bitswap_pb::Message) -> Result<Self, BitswapError> {
        let mut wantlist = msg.wantlist.unwrap_or_default();
        if wantlist.entries.len() + msg.payload.len() + msg.block_presences.len() != 1 {
            return Err(BitswapError::InvalidMessage);
        }
        if let Some(entry) = wantlist.entries.pop() {
            let cid = Cid::try_from(entry.block)?;
            let req = match entry.want_type {
                0 => Self::BlockRequest(cid),
                1 => Self::HaveRequest(cid),
                _ => return Err(BitswapError::InvalidMessage),
            };
            return Ok(req);
        }
        if let Some(block) = msg.payload.pop() {
            let cid = Prefix::new(&block.prefix)?.to_cid::<M>(&block.data)?;
            let data = block.data;
            return Ok(Self::BlockResponse(cid, data));
        }
        if let Some(presence) = msg.block_presences.pop() {
            let cid = Cid::try_from(presence.cid)?;
            let have = presence.r#type == 0;
            return Ok(Self::HaveResponse(cid, have));
        }
        unreachable!();
    }

    pub fn from_bytes<M: MultihashDigest>(bytes: &[u8]) -> Result<Self, BitswapError> {
        let pb = bitswap_pb::Message::decode(bytes)?;
        Self::decode::<M>(pb)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use tiny_multihash::Multihash;

    pub fn create_cid(bytes: &[u8]) -> Cid {
        use tiny_cid::RAW;
        use tiny_multihash::SHA2_256;
        let digest = Multihash::new(SHA2_256, bytes).unwrap().to_raw().unwrap();
        Cid::new_v1(RAW, digest)
    }

    #[test]
    fn test_message_encode_decode() {
        let messages = [
            BitswapMessage::HaveRequest(create_cid(&b"have_request"[..])),
            BitswapMessage::BlockRequest(create_cid(&b"block_request"[..])),
            BitswapMessage::HaveResponse(create_cid(&b"have_response"[..]), true),
            BitswapMessage::HaveResponse(create_cid(&b"have_response"[..]), false),
            BitswapMessage::BlockResponse(
                create_cid(&b"block_response"[..]),
                b"block_response".to_vec(),
            ),
        ];
        for message in &messages {
            assert_eq!(
                &BitswapMessage::decode::<Multihash>(message.clone().encode()).unwrap(),
                message
            );
        }
    }
}
