use com::*;
#[allow(unused_imports)]
use consts::*;
use codec::RedisString;


#[derive(Debug)]
pub enum RedisType {
    // general type
    String,
    List,
    // Sets
    Set,
    // ZSet
    ZSet,
    /// this should never support in reids 3.0
    Hash,

    // special type
    ListZipList,
    SetIntSet,
    ZSetZipList,
    HashZipList,
}

impl FromBuf for RedisType {
    /// always right otherwise panic forever
    fn from_buf(src: &[u8]) -> Result<Self> {
        let ltype = src[0];
        Ok(match ltype {
            REDIS_RDB_TYPE_STRING => RedisType::String,
            REDIS_RDB_TYPE_LIST => RedisType::List,
            REDIS_RDB_TYPE_SET => RedisType::Set,
            REDIS_RDB_TYPE_ZSET => RedisType::ZSet,
            REDIS_RDB_TYPE_HASH => RedisType::Hash,
            REDIS_RDB_TYPE_HASH_ZIPMAP => panic!("not support zipmap"),
            REDIS_RDB_TYPE_LIST_ZIPLIST => RedisType::ListZipList,
            REDIS_RDB_TYPE_SET_INTSET => RedisType::SetIntSet,
            REDIS_RDB_TYPE_ZSET_ZIPLIST => RedisType::ZSetZipList,
            REDIS_RDB_TYPE_HASH_ZIPLIST => RedisType::HashZipList,
            _ => unreachable!(),
        })
    }
}

impl Shift for RedisType {
    #[inline]
    fn shift(&self) -> usize {
        1
    }
}

#[derive(Debug)]
pub enum RedisData {}

impl FromBuf for RedisData {
    fn from_buf(_src: &[u8]) -> Result<Self> {
        unimplemented!();
    }
}

impl Shift for RedisData {
    fn shift(&self) -> usize {
        unimplemented!();
    }
}
