use lzf;
use com::*;
use consts::*;
use self::super::{FromBuf, Shift};

#[derive(Debug, Clone)]
pub enum Length {
    Small(u8),
    Normal(u16),
    Large(u32),
}

impl FromBuf for Length {
    /// judge by prefix two bits.
    fn from_buf(src: &[u8]) -> Result<Self> {
        let ltype = src[0] >> 6;
        match ltype {
            REDIS_RDB_6BITLEN => Ok(Length::Small(src[0] & 0x3f)),
            REDIS_RDB_14BITLEN => {
                more!(src.len() < 1 + 1);
                let value = buf_to_u16(src);
                Ok(Length::Normal(value & 0x3fff))
            }
            REDIS_RDB_32BITLEN => {
                more!(src.len() < 1 + 3);
                let value = buf_to_u32(src);
                Ok(Length::Large(value & 0x3fff_ffff))
            }
            REDIS_RDB_ENCVAL => Err(Error::Other),
            _ => Err(Error::Faild("wrong length encode prefix")),
        }
    }
}

impl Length {
    pub fn length(&self) -> usize {
        match self {
            &Length::Small(val) => val as usize,
            &Length::Normal(val) => val as usize,
            &Length::Large(val) => val as usize,
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


#[derive(Debug)]
pub enum RedisString {
    LengthPrefix { len: Length, data: Vec<u8> },
    StrInt(StrInt),
    LZF(LZFString),
}

impl Shift for RedisString {
    fn shift(&self) -> usize {
        match self {
            &RedisString::LengthPrefix { ref len, ref data } => len.shift() + data.len(),
            &RedisString::StrInt(ref ival) => ival.shift(),
            &RedisString::LZF(ref lzf) => lzf.shift(),
        }
    }
}

impl FromBuf for RedisString {
    fn from_buf(src: &[u8]) -> Result<RedisString> {
        choice!(RedisString::length_prefix(src));
        choice!(RedisString::str_int(src));
        choice!(RedisString::lzf(src));
        Err(Error::Faild("can't parse buffer as RedisString"))
    }
}

impl RedisString {
    // FIXME: Maybe Error ? I don't know if that's right
    pub fn to_float(&self) -> Result<f64> {
        match self {
            &RedisString::LengthPrefix { ref data, .. } => data_to_float(data.clone()),
            &RedisString::StrInt(ref str_int) => Ok(str_int.value() as f64),
            &RedisString::LZF(ref lzf_str) => data_to_float(lzf_str.buf.clone()),
        }
    }
}


impl RedisString {
    fn length_prefix(src: &[u8]) -> Result<RedisString> {
        let length = Length::from_buf(src)?;
        let mut data: Vec<u8> = Vec::with_capacity(length.length());
        data.extend_from_slice(&src[length.shift()..(length.shift() + length.length())]);
        Ok(RedisString::LengthPrefix {
            len: length,
            data: data,
        })
    }

    fn str_int(src: &[u8]) -> Result<RedisString> {
        let sint = StrInt::from_buf(src)?;
        Ok(RedisString::StrInt(sint))
    }

    fn lzf(src: &[u8]) -> Result<RedisString> {
        let lzf = LZFString::from_buf(src)?;
        Ok(RedisString::LZF(lzf))
    }
}


#[derive(Debug, Clone)]
pub struct LZFString {
    compressed_len: Length,
    original_len: Length,
    buf: Vec<u8>,
}

impl FromBuf for LZFString {
    fn from_buf(src: &[u8]) -> Result<LZFString> {
        let ltype = src[0] & 0b11;
        faild!(ltype != REDIS_RDB_ENC_LZF, "LZF flag not found");
        let compressed_len = Length::from_buf(&src[1..])?;
        let original_len = Length::from_buf(&src[(1 + compressed_len.shift())..])?;
        let shifted = 1 + compressed_len.shift() + original_len.shift();
        more!(src.len() < shifted + compressed_len.length());
        let src = &src[shifted..(shifted + compressed_len.length())];
        let buf = lzf::decompress(src, original_len.length())?;
        Ok(LZFString {
            compressed_len: compressed_len,
            original_len: original_len,
            buf: buf,
        })
    }
}

impl Shift for LZFString {
    #[inline]
    fn shift(&self) -> usize {
        1 + self.compressed_len.shift() + self.original_len.shift() + self.compressed_len.length()
    }
}


#[derive(Debug, Clone, Copy)]
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

impl FromBuf for StrInt {
    fn from_buf(src: &[u8]) -> Result<StrInt> {
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
            _ => panic!("not exact str int type but got:{}", ltype),
        }
    }
}

impl StrInt {
    pub fn value(&self) -> i32 {
        match self {
            &StrInt::Small(value) => value as i32,
            &StrInt::Normal(value) => value as i32,
            &StrInt::Large(value) => value as i32,
        }
    }
}



// Base series container of redis list type
pub struct RedisList<I>
    where I: Shift + FromBuf
{
    length: Length,
    items: Vec<I>,
}

impl<I> FromBuf for RedisList<I>
    where I: Shift + FromBuf
{
    fn from_buf(src: &[u8]) -> Result<Self> {
        let length = Length::from_buf(src)?;
        let mut pos = length.shift();

        let mut items = Vec::new();
        for _ in 0..length.length() {
            let item: I = FromBuf::from_buf(&src[pos..])?;
            pos += item.shift();
            items.push(item);
        }
        Ok(RedisList {
            length: length,
            items: items,
        })
    }
}

impl<I> Shift for RedisList<I>
    where I: Shift + FromBuf
{
    fn shift(&self) -> usize {
        self.length.shift() + self.items.iter().map(|x| x.shift()).fold(0, |acc, x| acc + x)
    }
}

// for List
pub struct LinkedListItem(RedisString);

impl Shift for LinkedListItem {
    fn shift(&self) -> usize {
        self.0.shift()
    }
}

impl FromBuf for LinkedListItem {
    fn from_buf(src: &[u8]) -> Result<Self> {
        let rstr = RedisString::from_buf(src)?;
        Ok(LinkedListItem(rstr))
    }
}

// for zset list
pub struct ZSetItem {
    member: RedisString,
    score: RedisString,
}

impl Shift for ZSetItem {
    fn shift(&self) -> usize {
        self.member.shift() + self.score.shift()
    }
}

impl FromBuf for ZSetItem {
    fn from_buf(src: &[u8]) -> Result<ZSetItem> {
        let member = RedisString::from_buf(src)?;
        let score = RedisString::from_buf(&src[member.shift()..])?;
        Ok(ZSetItem {
            member: member,
            score: score,
        })
    }
}


// for Hash
pub struct HashItem {
    key: RedisString,
    value: RedisString,
}

impl Shift for HashItem {
    fn shift(&self) -> usize {
        self.key.shift() + self.value.shift()
    }
}

impl FromBuf for HashItem {
    fn from_buf(src: &[u8]) -> Result<Self> {
        let key = RedisString::from_buf(src)?;
        let value = RedisString::from_buf(&src[key.shift()..])?;
        Ok(HashItem {
            key: key,
            value: value,
        })
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ZipListTail(u32);

impl Shift for ZipListTail {
    fn shift(&self) -> usize {
        4
    }
}

impl FromBuf for ZipListTail {
    fn from_buf(src: &[u8]) -> Result<Self> {
        more!(src.len() < 4);
        let val = buf_to_u32_little_endian(src);
        Ok(ZipListTail(val))
    }
}


#[derive(Copy, Clone, Debug)]
pub struct ZipListItemLen(u16);

impl Shift for ZipListItemLen {
    fn shift(&self) -> usize {
        2
    }
}

impl FromBuf for ZipListItemLen {
    fn from_buf(src: &[u8]) -> Result<Self> {
        more!(src.len() < 2);
        let val = buf_to_u16_little_endian(src);
        Ok(ZipListItemLen(val))
    }
}


#[derive(Clone, Debug)]
pub struct ZipList {
    zlbyets: Length,
    zltails: ZipListTail, // TODO: ziplist
}
