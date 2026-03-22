
use super::consts::*;
use byteorder::{BigEndian, ByteOrder};
use std::io::{self, Cursor, Write};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct FrameHeader {
    pub version: u8,
    pub typ: u8,
    pub flags: u16,
    pub stream_id: u32,
    pub length: u32,
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub header: FrameHeader,
    pub payload: Vec<u8>,
}

#[derive(Error, Debug)]
pub enum FrameError {
    #[error("Buffer too short for header")]
    IncompleteHeader,
    #[error("Invalid version")]
    InvalidVersion,
}

impl Frame {
    pub fn new(typ: u8, flags: u16, stream_id: u32, payload: Vec<u8>) -> Self {
        Self {
            header: FrameHeader {
                version: PROTOCOL_VERSION,
                typ,
                flags,
                stream_id,
                length: payload.len() as u32,
            },
            payload,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_LENGTH + self.payload.len());
        buf.push(self.header.version);
        buf.push(self.header.typ);
        let mut flags_bytes = [0u8; 2];
        BigEndian::write_u16(&mut flags_bytes, self.header.flags);
        buf.extend_from_slice(&flags_bytes);
        
        let mut id_bytes = [0u8; 4];
        BigEndian::write_u32(&mut id_bytes, self.header.stream_id);
        buf.extend_from_slice(&id_bytes);
        
        let mut len_bytes = [0u8; 4];
        BigEndian::write_u32(&mut len_bytes, self.header.length);
        buf.extend_from_slice(&len_bytes);
        
        buf.extend_from_slice(&self.payload);
        buf
    }

    pub fn decode_header(buf: &[u8]) -> Result<FrameHeader, FrameError> {
        if buf.len() < HEADER_LENGTH {
            return Err(FrameError::IncompleteHeader);
        }
        
        let version = buf[0];
        // if version != PROTOCOL_VERSION { return Err(FrameError::InvalidVersion); } // lenient for now
        
        let typ = buf[1];
        let flags = BigEndian::read_u16(&buf[2..4]);
        let stream_id = BigEndian::read_u32(&buf[4..8]);
        let length = BigEndian::read_u32(&buf[8..12]);
        
        Ok(FrameHeader {
            version,
            typ,
            flags,
            stream_id,
            length,
        })
    }
}
