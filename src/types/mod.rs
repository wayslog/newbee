use com::*;
#[allow(unused_imports)]
use consts::*;
use codec::*;
use std::time::{self, SystemTime, Duration};

pub type Key = RedisString;

pub type RedisLinkedList = RedisList<LinkedListItem>;
pub type RedisSet = RedisList<LinkedListItem>;
pub type RedisZSet = RedisList<ZSetItem>;
pub type RedisHash = RedisList<HashItem>;

#[derive(Debug, Clone)]
pub enum RedisData {
    // general type
    String(Key, RedisString),
    List(Key, RedisLinkedList),
    // Sets
    Set(Key, RedisSet),
    // ZSet
    ZSet(Key, RedisZSet),
    /// this should never support in reids 3.0
    Hash(Key, RedisHash),

    // special type
    ListZipList(RedisString, RedisString),
    ZSetZipList(RedisString, RedisString),
    HashZipList(RedisString, RedisString),
    SetIntSet(RedisString, RedisString),
}

impl RedisData {
    pub fn redis_data_fmt(&self, expire: &ExpireTime) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        Ok(buf)
    }
}

impl FromBuf for RedisData {
    /// always right otherwise panic forever
    fn from_buf(src: &[u8]) -> Result<Self> {
        let ltype = src[0];
        let key = RedisString::from_buf(&src[1..])?;

        let src = &src[1 + key.shift()..];
        match ltype {
            REDIS_RDB_TYPE_STRING => {
                let rs = RedisString::from_buf(src)?;
                Ok(RedisData::String(key, rs))
            }
            REDIS_RDB_TYPE_LIST => {
                let rls = RedisList::from_buf(src)?;
                Ok(RedisData::List(key, rls))
            }
            REDIS_RDB_TYPE_SET => {
                let rls = RedisList::from_buf(src)?;
                Ok(RedisData::Set(key, rls))
            }
            REDIS_RDB_TYPE_ZSET => {
                let rzls = RedisList::from_buf(src)?;
                Ok(RedisData::ZSet(key, rzls))
            }
            REDIS_RDB_TYPE_HASH => {
                let rhls = RedisList::from_buf(src)?;
                Ok(RedisData::Hash(key, rhls))
            }
            REDIS_RDB_TYPE_HASH_ZIPMAP => panic!("not support zipmap"),
            REDIS_RDB_TYPE_LIST_ZIPLIST => {
                let rs = RedisString::from_buf(src)?;
                Ok(RedisData::ListZipList(key, rs))
            }
            REDIS_RDB_TYPE_SET_INTSET => {
                let rs = RedisString::from_buf(src)?;
                Ok(RedisData::SetIntSet(key, rs))
            }
            REDIS_RDB_TYPE_ZSET_ZIPLIST => {
                let rs = RedisString::from_buf(src)?;
                Ok(RedisData::ZSetZipList(key, rs))
            }
            REDIS_RDB_TYPE_HASH_ZIPLIST => {
                let rs = RedisString::from_buf(src)?;
                Ok(RedisData::HashZipList(key, rs))
            }
            _ => unreachable!(),
        }
    }
}

impl Shift for RedisData {
    #[inline]
    fn shift(&self) -> usize {
        let suffix_len = match self {
            &RedisData::String(ref key, ref v) => key.shift() + v.shift(),
            &RedisData::List(ref key, ref v) => key.shift() + v.shift(),
            &RedisData::Set(ref key, ref v) => key.shift() + v.shift(),
            &RedisData::ZSet(ref key, ref v) => key.shift() + v.shift(),
            &RedisData::Hash(ref key, ref v) => key.shift() + v.shift(),
            &RedisData::ListZipList(ref key, ref v) => key.shift() + v.shift(),
            &RedisData::SetIntSet(ref key, ref v) => key.shift() + v.shift(),
            &RedisData::HashZipList(ref key, ref v) => key.shift() + v.shift(),
            &RedisData::ZSetZipList(ref key, ref v) => key.shift() + v.shift(),
        };
        1 + suffix_len
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

    pub fn expire_str(&self, buf: &mut Vec<u8>) {
        let now = SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap();

        match self {
            &ExpireTime::Ms(iv) => {
                let dur = Duration::from_millis(iv as u64);
                let px = dur - now;
                let sec = px.as_secs();
                let ns = px.subsec_nanos();
                let v = format!("PX {}", sec * 1000 + (ns as u64) / 1000000);
                buf.extend_from_slice(v.as_bytes());
            }
            &ExpireTime::Sec(iv) => {
                let dur = Duration::from_secs(iv as u64);
                let ex = dur - now;
                let sec = ex.as_secs();
                let v = format!("EX {}", sec);
                buf.extend_from_slice(v.as_bytes());
            }
            &ExpireTime::None => {}
        }
    }
}
