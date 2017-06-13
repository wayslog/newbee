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


pub struct LinkedList<I>
    where I: Shift + FromBuf
{
    length: Length,
    items: Vec<I>,
}

impl<I> FromBuf for LinkedList<I>
    where I: Shift + FromBuf
{
    fn from_buf(src: &[u8]) -> Result<Self> {
        unimplemented!();
    }
}

impl<I> Shift for LinkedList<I>
    where I: Shift + FromBuf
{
    fn shift(&self) -> usize {
        self.length.shift() + self.items.iter().map(|x| x.shift()).fold(0, |acc, x| acc + x)
    }
}

pub struct ZipList {
}
