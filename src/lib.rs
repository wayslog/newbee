//! Streamed RDB Rust Parser

extern crate lzf;

#[macro_use]
pub mod com;
pub mod consts;
pub mod codec;
pub mod types;

pub use com::*;
pub use codec::*;
pub use types::*;
pub use consts::*;

pub trait RdbParser {
    fn read_to_local(&mut self) -> Result<usize>;
    fn local_buf(&self) -> &[u8];

    fn header(&mut self) -> Result<RdbEntry> {
        let src = self.local_buf();
        more!(src.len() < REDIS_MAGIC_STRING.len() + 4);
        let version = &src[REDIS_MAGIC_STRING.len()..REDIS_MAGIC_STRING.len() + 4];
        Ok(RdbEntry::Version(buf_to_u32(version)))
    }

    fn sector(&mut self) -> Result<RdbEntry> {
        let src = self.local_buf();
        more!(src.len() < 2);
        faild!(src[0] != REDIS_RDB_OPCODE_SELECTDB,
               "can't find redis_db_selector");
        Ok(RdbEntry::Sector(src[1]))
    }

    fn data(&mut self) -> Result<RdbEntry> {
        let src = self.local_buf();
        let expire = ExpireTime::from_buf(src)?;
        let data = RedisData::from_buf(src)?;
        Ok(RdbEntry::Data {
            expire: expire,
            data: data,
        })
    }
}


#[derive(Debug)]
pub enum State {
    Header,
    Sector,
    Body,
}

#[derive(Debug)]
pub enum RdbEntry {
    Version(u32),
    Sector(u8),
    Data { expire: ExpireTime, data: RedisData },
}
impl RdbEntry {
    fn is_data(&self) -> bool {
        match self {
            &RdbEntry::Data { .. } => true,
            _ => false,
        }
    }
}

impl Shift for RdbEntry {
    #[inline]
    fn shift(&self) -> usize {
        match self {
            // len('REDIS') + version_number
            &RdbEntry::Version(_) => 5 + 4,
            // 0xFE + u8
            &RdbEntry::Sector(_) => 2,
            &RdbEntry::Data { ref expire, ref data } => expire.shift() + data.shift(),
        }
    }
}


pub trait Dumps {
    fn dumps(&self) -> Result<Vec<Vec<u8>>>;
}

impl Dumps for RdbEntry {
    fn dumps(&self) -> Result<Vec<Vec<u8>>> {
        match self {
            &RdbEntry::Data { ref expire, ref data } => {
                1;
            }
            _ => unreachable!(),
        };
        Err(Error::More)
    }
}
