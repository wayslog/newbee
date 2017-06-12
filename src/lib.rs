//! Streamed RDB Rust Parser

extern crate lzf;

pub use com::*;

use std::io::Read;

pub const REDIS_RDB_6BITLEN: u8 = 0;
pub const REDIS_RDB_14BITLEN: u8 = 1;
pub const REDIS_RDB_32BITLEN: u8 = 2;
pub const REDIS_RDB_ENCVAL: u8 = 3;

// pub const REDIS_RDB_LENERR: u32 = 111;

pub const REDIS_RDB_ENC_INT8: u8 = 0;        /* 8 bit signed integer */
pub const REDIS_RDB_ENC_INT16: u8 = 1;       /* 16 bit signed integer */
pub const REDIS_RDB_ENC_INT32: u8 = 2;       /* 32 bit signed integer */
pub const REDIS_RDB_ENC_LZF: u8 = 3;         /* string compressed with FASTLZ */

pub const REDIS_RDB_TYPE_STRING: u8 = 0;
pub const REDIS_RDB_TYPE_LIST: u8 = 1;
pub const REDIS_RDB_TYPE_SET: u8 = 2;
pub const REDIS_RDB_TYPE_ZSET: u8 = 3;
pub const REDIS_RDB_TYPE_HASH: u8 = 4;

// Object types for encoded objects.
pub const REDIS_RDB_TYPE_HASH_ZIPMAP: u8 = 9;
pub const REDIS_RDB_TYPE_LIST_ZIPLIST: u8 = 10;
pub const REDIS_RDB_TYPE_SET_INTSET: u8 = 11;
pub const REDIS_RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
pub const REDIS_RDB_TYPE_HASH_ZIPLIST: u8 = 13;

// Special RDB opcodes (saved/loaded with rdbSaveType/rdbLoadType).
pub const REDIS_RDB_OPCODE_EXPIRETIME_MS: u8 = 252;
pub const REDIS_RDB_OPCODE_EXPIRETIME_MS_LEN: usize = 8;

pub const REDIS_RDB_OPCODE_EXPIRETIME: u8 = 253;
pub const REDIS_RDB_OPCODE_EXPIRETIME_LEN: usize = 4;

pub const REDIS_RDB_OPCODE_SELECTDB: u8 = 254;
pub const REDIS_RDB_OPCODE_EOF: u8 = 255;
pub const REDIS_MAGIC_STRING: &str = "REDIS";


macro_rules! more{
    ($e: expr) => {
        if $e {
            return Err(Error::More);
        }
    }
}

pub trait RdbParser: Read {
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
        if src[0] != REDIS_RDB_OPCODE_SELECTDB {
            return Err(Error::Faild("can't find redis_db_selector"));
        }
        Ok(RdbEntry::Sector(src[1]))
    }

    fn data(&mut self) -> Result<RdbEntry> {
        let src = self.local_buf();
        let expire = ExpireTime::from_buf(src)?;
        Err(Error::More)
    }
}

pub trait Shift {
    #[inline]
    fn shift(&self) -> usize;
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
        // always redis string
        key: RType,
        value: RType,
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
            &RdbEntry::Data { ref expire, ref key, ref value } => {
                expire.shift() + key.shift() + value.shift()
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

impl ExpireTime {
    pub fn from_buf(src: &[u8]) -> Result<ExpireTime> {
        match ExpireTime::expire_in_ms(src) {
            Ok(v) => return Ok(ExpireTime::Ms(v)),
            Err(Error::Other) => {}
            Err(Error::More) => {
                return Err(Error::More);
            }
            _ => panic!("Get an Unexcept error in parse: expire_in_ms"),
        };

        match ExpireTime::expire_in_sec(src) {
            Ok(v) => return Ok(ExpireTime::Sec(v)),
            Err(Error::Other) => {}
            Err(Error::More) => {
                return Err(Error::More);
            }
            _ => panic!("Get an Unexcept error in parse: expire_in_sec"),
        }
        Ok(ExpireTime::None)
    }


    #[inline]
    pub fn expire_in_ms(src: &[u8]) -> Result<i64> {
        if src[0] != REDIS_RDB_OPCODE_EXPIRETIME_MS {
            return Err(Error::Other);
        }
        if src.len() < REDIS_RDB_OPCODE_EXPIRETIME_MS_LEN + 1 {
            return Err(Error::More);
        }
        Ok(buf_to_i64(&src[1..]))
    }


    #[inline]
    pub fn expire_in_sec(src: &[u8]) -> Result<i32> {
        if src[0] != REDIS_RDB_OPCODE_EXPIRETIME {
            return Err(Error::Other);
        }
        if src.len() < REDIS_RDB_OPCODE_EXPIRETIME_LEN + 1 {
            return Err(Error::More);
        }
        Ok(buf_to_i32(&src[1..]))
    }
}

#[derive(Debug)]
pub enum RType {
    String(String),
    StrInt(i64),
    StringLZF(String),
    List,
    Set,
    ZSet,
    Hash,
    ZipList,
    IntSet,
    ZipSet,
    ZipHash,
}

impl RType {
    fn string(src: &[u8]) -> Result<Self> {
        Err(Error::More)
    }
}

impl Shift for RType {
    #[inline]
    fn shift(&self) -> usize {
        unimplemented!();
    }
}

#[derive(Debug, Clone)]
pub enum Length {
    Small(u8),
    Normal(u16),
    Large(u32),
}

impl Length {
    pub fn length(&self) -> usize {
        match self {
            &Length::Small(val) => val as usize,
            &Length::Normal(val) => val as usize,
            &Length::Large(val) => val as usize,
        }
    }
    pub fn from_buf(src: &[u8]) -> Result<Length> {
        let ltype = src[0] & 0b11;
        match ltype {
            REDIS_RDB_6BITLEN => Ok(Length::Small(ltype & 0x3f)),
            REDIS_RDB_14BITLEN => {
                more!(src.len() < 2);
                let value = buf_to_u16(src);
                Ok(Length::Normal(value & 0x3fff))
            }
            REDIS_RDB_32BITLEN => {
                more!(src.len() < 4);
                let value = buf_to_u32(src);
                Ok(Length::Large(value & 0x3fff_ffff))
            }
            _ => Err(Error::Faild("wrong length encode prefix")),
        }
    }
}

impl Shift for Length {
    #[inline]
    fn shift(&self) -> usize {
        match self {
            &Length::Small(_) => 1,
            &Length::Normal(_) => 2,
            &Length::Large(_) => 5,
        }
    }
}

pub enum RedisString {
    LengthPrefix { len: Length, data: String },
    StrInt(StrInt),
    LZF,
}

impl RedisString {
    pub fn from_buf(src: &[u8]) -> Result<RedisString> {
        unimplemented!();
    }
}

#[derive(Debug, Clone)]
pub struct LZFString {
    compressed_len: Length,
    original_len: Length,
    buf: Vec<usize>,
}


impl LZFString {
    pub fn from_buf(src: &[u8]) -> Result<LZFString> {
        let ltype = src[0] & 0b11;
        if ltype != REDIS_RDB_ENC_LZF {
            return Err(Error::Faild("LZF flag not found"));
        }

        let compressed_len = Length::from_buf(&src[1..])?;
        let original_len = Length::from_buf(&src[(1 + compressed_len.shift())..])?;
        more!(src.len() < 1 + compressed_len.shift() + original_len.shift());
        // let mut lzf_content = Vec::with_capacity(original_len.length());
        // TODO

        Err(Error::More)
    }
}

impl Shift for LZFString {
    #[inline]
    fn shift(&self) -> usize {
        1 + self.compressed_len.shift() + self.original_len.shift() + self.compressed_len.length()
    }
}


pub enum StrInt {
    Small(i8),
    Normal(i16),
    Large(i32),
}

impl Shift for StrInt {
    #[inline]
    fn shift(&self) -> usize {
        match self {
            &StrInt::Small(_) => 1 + 1,
            &StrInt::Normal(_) => 1 + 2,
            &StrInt::Large(_) => 1 + 4,
        }
    }
}

impl StrInt {
    pub fn from_buf(src: &[u8]) -> Result<StrInt> {
        let ltype = src[0] & 0x3f;
        match ltype {
            REDIS_RDB_ENC_INT8 => {
                more!(src.len() < 1 + 1);
                Ok(StrInt::Small(src[0] as i8))
            }
            REDIS_RDB_ENC_INT16 => {
                more!(src.len() < 1 + 2);
                Ok(StrInt::Normal(buf_to_i16(&src[1..])))
            }
            REDIS_RDB_ENC_INT32 => {
                more!(src.len() < 1 + 4);
                Ok(StrInt::Large(buf_to_i32(&src[1..])))
            }
            REDIS_RDB_ENC_LZF => Err(Error::Other),
            _ => {
                panic!("not exact str int type: \n\texpect: [0, 1, 2, 3]\n\tgot:{}",
                       ltype)
            }
        }
    }
}

mod com {
    use std::result;
    use lzf;
    use std::io;
    use std::convert::From;

    pub type Result<T> = result::Result<T, Error>;

    #[derive(Debug)]
    pub enum Error {
        ParserError(String),
        More,
        Faild(&'static str),
        Other,
        LzfError(lzf::LzfError),
        IoError(io::Error),
    }

    impl From<lzf::LzfError> for Error {
        fn from(oe: lzf::LzfError) -> Error {
            Error::LzfError(oe)
        }
    }

    #[inline]
    pub fn is_rdb_obj_type(t: u8) -> bool {
        (t <= 4) || (t >= 9 && t <= 13)
    }

    #[inline]
    pub fn buf_to_i32(src: &[u8]) -> i32 {
        let mut vi32 = 0i32;
        vi32 |= (src[0] as i32) << 24;
        vi32 |= (src[1] as i32) << 16;
        vi32 |= (src[2] as i32) << 8;
        vi32 |= (src[3] as i32) << 0;
        vi32
    }

    #[inline]
    pub fn buf_to_u32(src: &[u8]) -> u32 {
        let mut vu32 = 0u32;
        vu32 |= (src[0] as u32) << 24;
        vu32 |= (src[1] as u32) << 16;
        vu32 |= (src[2] as u32) << 8;
        vu32 |= (src[3] as u32) << 0;
        vu32
    }

    #[inline]
    pub fn buf_to_u64(src: &[u8]) -> u64 {
        let mut vu64 = 0u64;
        vu64 |= (src[0] as u64) << 56;
        vu64 |= (src[1] as u64) << 48;
        vu64 |= (src[2] as u64) << 40;
        vu64 |= (src[3] as u64) << 32;
        vu64 |= (src[4] as u64) << 24;
        vu64 |= (src[5] as u64) << 16;
        vu64 |= (src[6] as u64) << 8;
        vu64 |= (src[7] as u64) << 0;
        vu64
    }

    #[inline]
    pub fn buf_to_i64(src: &[u8]) -> i64 {
        let mut vi64 = 0i64;
        vi64 |= (src[0] as i64) << 56;
        vi64 |= (src[1] as i64) << 48;
        vi64 |= (src[2] as i64) << 40;
        vi64 |= (src[3] as i64) << 32;
        vi64 |= (src[4] as i64) << 24;
        vi64 |= (src[5] as i64) << 16;
        vi64 |= (src[6] as i64) << 8;
        vi64 |= (src[7] as i64) << 0;
        vi64
    }

    #[inline]
    pub fn buf_to_u16(src: &[u8]) -> u16 {
        let mut vu16 = 0u16;
        vu16 |= (src[0] as u16) << 8;
        vu16 |= (src[1] as u16) << 0;
        vu16
    }

    #[inline]
    pub fn buf_to_i16(src: &[u8]) -> i16 {
        let mut vi16 = 0i16;
        vi16 |= (src[0] as i16) << 8;
        vi16 |= (src[1] as i16) << 0;
        vi16
    }
}
