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
                let value = buf_to_u16_big(src);
                Ok(Length::Normal(value & 0x3fff))
            }
            REDIS_RDB_32BITLEN => {
                more!(src.len() < 1 + 3);
                let value = buf_to_u32_big(src);
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


#[derive(Debug, Clone)]
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
    pub fn into_data(self) -> Vec<u8> {
        match self {
            RedisString::LengthPrefix { data, .. } => data,
            RedisString::StrInt(v) => {
                let strv = format!("{}", v.value());
                strv.into_bytes()
            }
            RedisString::LZF(LZFString { buf, .. }) => buf,
        }
    }

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
#[derive(Clone, Debug)]
pub struct RedisList<I>
    where I: Shift + FromBuf
{
    pub length: Length,
    pub items: Vec<I>,
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
#[derive(Clone, Debug)]
pub struct LinkedListItem(pub RedisString);

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
#[derive(Clone, Debug)]
pub struct ZSetItem {
    pub member: RedisString,
    pub score: RedisString,
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
#[derive(Clone, Debug)]
pub struct HashItem {
    pub key: RedisString,
    pub value: RedisString,
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
        let val = buf_to_u32(src);
        Ok(ZipListTail(val))
    }
}


#[derive(Copy, Clone, Debug)]
pub struct ZipListLen(u16);

impl Shift for ZipListLen {
    fn shift(&self) -> usize {
        2
    }
}

impl FromBuf for ZipListLen {
    fn from_buf(src: &[u8]) -> Result<Self> {
        more!(src.len() < 2);
        let val = buf_to_u16_little_endian(src);
        Ok(ZipListLen(val))
    }
}


#[derive(Clone, Debug)]
pub enum ZLELen {
    Small(u8),
    Large(u32),
}

impl Shift for ZLELen {
    fn shift(&self) -> usize {
        match self {
            &ZLELen::Small(_) => 1,
            &ZLELen::Large(_) => 5,
        }
    }
}

impl FromBuf for ZLELen {
    fn from_buf(src: &[u8]) -> Result<Self> {
        more!(src.len() < 1);
        let flag = src[0];
        if flag <= REDIS_RDB_FLAG_ZIPLIST_ENTRY_LEN_MAX {
            return Ok(ZLELen::Small(flag));
        }
        more!(src.len() < 1 + 4);
        let value = buf_to_u32(&src[1..]);
        Ok(ZLELen::Large(value))
    }
}

#[derive(Clone, Debug)]
pub enum ZLESpData {
    SmallStr(Vec<u8>),
    NormalStr(Vec<u8>),
    LargeStr(Vec<u8>),
    ExSmallInt(u8),
    SmallInt(i8),
    NormalInt(i16),
    LargeTrimInt(i32),
    LargeInt(i32),
    ExLargeInt(i64),
}

impl ZLESpData {
    pub fn into_data(self) -> Vec<u8> {
        match self {
            ZLESpData::SmallStr(v) => v,
            ZLESpData::NormalStr(v) => v,
            ZLESpData::LargeStr(v) => v,
            ZLESpData::ExSmallInt(v) => format!("{}", v).into_bytes(),
            ZLESpData::SmallInt(v) => format!("{}", v).into_bytes(),
            ZLESpData::NormalInt(v) => format!("{}", v).into_bytes(),
            ZLESpData::LargeTrimInt(v) => format!("{}", v).into_bytes(),
            ZLESpData::LargeInt(v) => format!("{}", v).into_bytes(),
            ZLESpData::ExLargeInt(v) => format!("{}", v).into_bytes(),
        }
    }
}

impl Shift for ZLESpData {
    fn shift(&self) -> usize {
        match self {
            &ZLESpData::SmallStr(ref v) => 1 + v.len(),
            &ZLESpData::NormalStr(ref v) => 2 + v.len(),
            &ZLESpData::LargeStr(ref v) => 1 + 4 + v.len(),
            &ZLESpData::ExSmallInt(_) => 1,
            &ZLESpData::SmallInt(_) => 1 + 1,
            &ZLESpData::NormalInt(_) => 1 + 2,
            &ZLESpData::LargeTrimInt(_) => 1 + 3,
            &ZLESpData::LargeInt(_) => 1 + 4,
            &ZLESpData::ExLargeInt(_) => 1 + 8,
        }
    }
}

impl ZLESpData {
    fn to_special_int(src: &[u8]) -> Result<ZLESpData> {
        let flag = src[0] & 0x0f;
        match flag {
            REDIS_RDB_FLAG_ZIPLIST_ENTRY_LARGE_TRIM_INT => {
                more!(src.len() < 1 + 3);
                Ok(ZLESpData::LargeTrimInt(buf_to_i32_trim(&src[1..])))
            }
            REDIS_RDB_FLAG_ZIPLIST_ENTRY_SMALL_INT => {
                more!(src.len() < 1 + 1);
                Ok(ZLESpData::SmallInt(src[1] as i8))
            }
            val if 1 <= val && val <= 13 => Ok(ZLESpData::ExSmallInt(val - 1)),
            _ => Err(Error::Other),
        }
    }

    fn to_usual_int(src: &[u8]) -> Result<ZLESpData> {
        let flag = (src[0] << 2) >> 6;
        match flag {
            REDIS_RDB_FLAG_ZIPLIST_ENTRY_NORMAL_INT => {
                let req = 1 + 2;
                more!(src.len() < req);
                Ok(ZLESpData::NormalInt(buf_to_i16(&src[1..])))
            }

            REDIS_RDB_FLAG_ZIPLIST_ENTRY_LARGE_INT => {
                more!(src.len() < 1 + 4);
                Ok(ZLESpData::LargeInt(buf_to_i32(&src[1..])))
            }
            REDIS_RDB_FLAG_ZIPLIST_ENTRY_EXLARGE_INT => {
                more!(src.len() < 1 + 8);
                Ok(ZLESpData::ExLargeInt(buf_to_i64(&src[1..])))
            }
            _ => Err(Error::Other),
        }
    }

    fn to_str(src: &[u8]) -> Result<ZLESpData> {
        let flag = src[0] >> 6;
        match flag {
            REDIS_RDB_FLAG_ZIPLIST_ENTRY_SMALL_STR => {
                let req = 1;
                let len = (src[0] & 0x3f) as usize;
                more!(src.len() < req + len);
                Ok(ZLESpData::SmallStr((&src[req..req + len]).to_vec()))
            }
            REDIS_RDB_FLAG_ZIPLIST_ENTRY_NORMAL_STR => {
                let req = 1 + 1;
                more!(src.len() < req);
                let len = (buf_to_u16(&src[1..]) & 0x3fff) as usize;
                more!(src.len() < req + len);
                Ok(ZLESpData::NormalStr(src[req..req + len].to_vec()))
            }
            REDIS_RDB_FLAG_ZIPLIST_ENTRY_LARGE_STR => {
                let req = 1 + 4;
                more!(src.len() < req);
                let len = (buf_to_u64(&src[1..]) & 0x3fff_ffff) as usize;
                more!(src.len() < req + len);
                Ok(ZLESpData::LargeStr(src[req..req + len].to_vec()))
            }
            _ => Err(Error::Other),
        }
    }
}

impl FromBuf for ZLESpData {
    fn from_buf(src: &[u8]) -> Result<ZLESpData> {
        more!(src.len() == 0);
        choice!(ZLESpData::to_str(src));
        choice!(ZLESpData::to_usual_int(src));
        choice!(ZLESpData::to_special_int(src));
        Err(Error::Faild("not regular ZipListSpecialFlag"))
    }
}

#[derive(Clone, Debug)]
pub struct ZipListEntry {
    pub prev_len: ZLELen,
    pub sp: ZLESpData,
}

impl Shift for ZipListEntry {
    fn shift(&self) -> usize {
        self.prev_len.shift() + self.sp.shift()
    }
}

impl FromBuf for ZipListEntry {
    fn from_buf(src: &[u8]) -> Result<Self> {
        let len = ZLELen::from_buf(src)?;
        let sp = ZLESpData::from_buf(&src[len.shift()..])?;
        Ok(ZipListEntry {
            prev_len: len,
            sp: sp,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ZipList {
    zlbytes: Length,
    zltails: ZipListTail,
    zllen: ZipListLen,
    pub entries: Vec<ZipListEntry>,
    zlend: u8,
}

impl Shift for ZipList {
    fn shift(&self) -> usize {
        self.zlbytes.shift() + self.zltails.shift() + self.zllen.shift() + self.zlend.shift() +
        self.entries.iter().map(|x| x.shift()).fold(0, |acc, x| acc + x)
    }
}


impl FromBuf for ZipList {
    fn from_buf(src: &[u8]) -> Result<Self> {
        let zlbytes = Length::from_buf(src)?;
        let zltails = ZipListTail::from_buf(&src[zlbytes.shift()..])?;
        let zllen = ZipListLen::from_buf(&src[zlbytes.shift() + zltails.shift()..])?;
        more!(src.len() < zlbytes.length());
        let mut entries = Vec::new();
        let mut pos = zlbytes.shift() + zltails.shift() + zllen.shift();

        for _ in 0..zllen.0 as usize {
            let entry = ZipListEntry::from_buf(&src[pos..])?;
            pos += entry.shift();
            entries.push(entry);
        }
        let zlend = src[pos];
        assert_eq!(zlend, 0xff);
        Ok(ZipList {
            zlbytes: zlbytes,
            zltails: zltails,
            zllen: zllen,
            entries: entries,
            zlend: zlend,
        })
    }
}



pub trait To<T>
    where T: Shift + FromBuf
{
    fn to(&mut self) -> Result<RedisList<T>>;
}

impl To<LinkedListItem> for ZipList {
    fn to(&mut self) -> Result<RedisList<LinkedListItem>> {
        unimplemented!()
    }
}


#[derive(Debug, Clone)]
pub enum IntSetEncoding {
    Normal,
    Large,
    ExLarge,
}

impl Shift for IntSetEncoding {
    fn shift(&self) -> usize {
        4
    }
}

impl FromBuf for IntSetEncoding {
    fn from_buf(src: &[u8]) -> Result<Self> {
        more!(src.len() < 4);
        match src[0] {
            2 => Ok(IntSetEncoding::Normal),
            4 => Ok(IntSetEncoding::Large),
            8 => Ok(IntSetEncoding::ExLarge),
            _ => Err(Error::Faild("wrong IntSet encoding")),
        }
    }
}

impl IntSetEncoding {
    pub fn encoding(&self) -> usize {
        match self {
            &IntSetEncoding::Normal => 2,
            &IntSetEncoding::Large => 4,
            &IntSetEncoding::ExLarge => 8,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IntSetCount(u32);

impl Shift for IntSetCount {
    fn shift(&self) -> usize {
        4
    }
}

impl FromBuf for IntSetCount {
    fn from_buf(src: &[u8]) -> Result<Self> {
        more!(src.len() < 4);
        Ok(IntSetCount(buf_to_u32(src)))
    }
}

#[derive(Debug, Clone)]
pub enum IntSetValue {
    Normal(i16),
    Large(i32),
    ExLarge(i64),
}


#[derive(Debug, Clone)]
pub struct IntSet {
    pub encoding: IntSetEncoding,
    pub count: IntSetCount,
    pub ints: Vec<i64>,
}

impl Shift for IntSet {
    fn shift(&self) -> usize {
        self.encoding.shift() + self.count.shift() +
        self.encoding.encoding() * (self.count.0 as usize)
    }
}

impl FromBuf for IntSet {
    fn from_buf(src: &[u8]) -> Result<Self> {
        let encoding = IntSetEncoding::from_buf(&src[0..])?;
        let count = IntSetCount::from_buf(&src[encoding.shift()..])?;
        let mut ints = Vec::new();
        let e = encoding.encoding();
        let mut pos = encoding.shift() + count.shift();
        let encoding_func: fn(&[u8]) -> i64 = if e == 4 {
            |src| {
                let uv = buf_to_u16_little_endian(src);
                (uv as i32) as i64
            }
        } else if e == 2 {
            |src| {
                let uv = buf_to_u16_little_endian(src);
                (uv as i16) as i64
            }
        } else if e == 8 {
            |src| {
                let uv = buf_to_u64(src);
                uv as i64
            }
        } else {
            panic!("not valid encoding")
        };

        for _ in 0..count.0 as usize {
            more!(src.len() < pos + e);
            let val = encoding_func(&src[pos..]);
            ints.push(val);
            pos += e;
        }

        Err(Error::Faild("Fuck"))
    }
}
