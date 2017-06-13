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
        let key = RedisString::from_buf(src)?;
        let data = RedisType::from_buf(src)?;

        // TODO: done for it
        Err(Error::More)
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
    Data {
        expire: ExpireTime,
        rtype: RedisType,
        key: RedisString,
        value: RedisType,
    },
}

impl Shift for RdbEntry {
    #[inline]
    fn shift(&self) -> usize {
        match self {
            // len('REDIS') + version_number
            &RdbEntry::Version(_) => 5 + 4,
            // 0xFE + u8
            &RdbEntry::Sector(_) => 2,
            &RdbEntry::Data { ref expire, ref rtype, ref key, ref value } => {
                expire.shift() + rtype.shift() + key.shift() + value.shift()
            }
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum ExpireTime {
    Ms(i64),
    Sec(i32),
    None,
}

impl Shift for ExpireTime {
    #[inline]
    fn shift(&self) -> usize {
        match self {
            &ExpireTime::Ms(_) => 8 + 1,
            &ExpireTime::Sec(_) => 4 + 1,
            _ => 0,
        }
    }
}

impl FromBuf for ExpireTime {
    fn from_buf(src: &[u8]) -> Result<ExpireTime> {
        choice!(ExpireTime::expire_in_ms(src));
        choice!(ExpireTime::expire_in_sec(src));
        Ok(ExpireTime::None)
    }
}

impl ExpireTime {
    #[inline]
    pub fn expire_in_ms(src: &[u8]) -> Result<ExpireTime> {
        other!(src[0] != REDIS_RDB_OPCODE_EXPIRETIME_MS);
        more!(src.len() < REDIS_RDB_OPCODE_EXPIRETIME_MS_LEN + 1);
        Ok(ExpireTime::Ms(buf_to_i64(&src[1..])))
    }

    #[inline]
    pub fn expire_in_sec(src: &[u8]) -> Result<ExpireTime> {
        other!(src[0] != REDIS_RDB_OPCODE_EXPIRETIME);
        more!(src.len() < REDIS_RDB_OPCODE_EXPIRETIME_LEN + 1);
        Ok(ExpireTime::Sec(buf_to_i32(&src[1..])))
    }
}
