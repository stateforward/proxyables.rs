pub const PROTOCOL_VERSION: u8 = 0;

pub const TYPE_DATA: u8 = 0;
pub const TYPE_WINDOW_UPDATE: u8 = 1;
pub const TYPE_PING: u8 = 2;
pub const TYPE_GO_AWAY: u8 = 3;

pub const FLAG_SYN: u16 = 1;
pub const FLAG_ACK: u16 = 2;
pub const FLAG_FIN: u16 = 4;
pub const FLAG_RST: u16 = 8;

pub const HEADER_LENGTH: usize = 12;
pub const WINDOW_SIZE: u32 = 256 * 1024;

pub const ERR_NORMAL: u32 = 0;
pub const ERR_PROTOCOL: u32 = 1;
pub const ERR_INTERNAL: u32 = 2;
