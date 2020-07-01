use crate::*;
use bytes::{Bytes, Buf};

/// Acknowledgement to pubrec
#[derive(Debug, Clone, PartialEq)]
pub struct PubRel {
    pub pkid: u16,
}

impl PubRel {
    pub fn new(pkid: u16) -> PubRel {
        PubRel { pkid }
    }

    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        if fixed_header.remaining_len != 2 {
            return Err(Error::PayloadSizeIncorrect);
        }

        let variable_header_index = fixed_header.header_len;
        bytes.advance(variable_header_index);
        let pkid = bytes.get_u16();
        let pubrel = PubRel { pkid };

        Ok(pubrel)
    }
}


