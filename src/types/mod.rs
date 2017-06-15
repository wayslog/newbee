use com::*;
use consts::*;
use codec::*;
use fmt::*;
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
    ListZipList(Key, RedisString),
    ZSetZipList(Key, RedisString),
    HashZipList(Key, RedisString),
    SetIntSet(Key, RedisString),
}

impl RedisData {
    pub fn copy_key(&self) -> RedisString {
        match self {
            &RedisData::String(ref key, _) => key.clone(),
            &RedisData::List(ref key, _) => key.clone(),
            &RedisData::Set(ref key, _) => key.clone(),
            &RedisData::ZSet(ref key, _) => key.clone(),
            &RedisData::Hash(ref key, _) => key.clone(),
            &RedisData::ListZipList(ref key, _) => key.clone(),
            &RedisData::ZSetZipList(ref key, _) => key.clone(),
            &RedisData::HashZipList(ref key, _) => key.clone(),
            &RedisData::SetIntSet(ref key, _) => key.clone(),
        }
    }
}

impl RedisFormat for RedisData {
    fn fmt(self, buf: &mut Vec<RedisFmt>) -> usize {
        match self {
            RedisData::String(key, rs) => {
                buf.push(RedisFmt::Cmd("set"));
                buf.push(RedisFmt::Raw(key.into_data()));
                buf.push(RedisFmt::Raw(rs.into_data()));
            }
            RedisData::List(key, rl) => {
                buf.push(RedisFmt::Cmd("lpush"));
                buf.push(RedisFmt::Raw(key.into_data()));
                let RedisList { items, .. } = rl;
                for linked_list_item in items {
                    buf.push(RedisFmt::Raw(linked_list_item.0.into_data()));
                }
            }
            RedisData::Set(key, rs) => {
                buf.push(RedisFmt::Cmd("SADD"));
                buf.push(RedisFmt::Raw(key.into_data()));
                let RedisList { items, .. } = rs;
                for set_item in items {
                    buf.push(RedisFmt::Raw(set_item.0.into_data()));
                }
            }
            RedisData::ZSet(key, RedisList { items, .. }) => {
                buf.push(RedisFmt::Cmd("ZADD"));
                buf.push(RedisFmt::Raw(key.into_data()));
                for item in items {
                    let ZSetItem { member, score } = item;
                    buf.push(RedisFmt::Raw(score.into_data()));
                    buf.push(RedisFmt::Raw(member.into_data()));
                }
            }
            RedisData::Hash(key, RedisList { items, .. }) => {

                buf.push(RedisFmt::Cmd("HSET"));
                buf.push(RedisFmt::Raw(key.into_data()));
                for item in items {
                    let HashItem { key: hkey, value } = item;
                    buf.push(RedisFmt::Raw(hkey.into_data()));
                    buf.push(RedisFmt::Raw(value.into_data()));
                }
            }

            RedisData::SetIntSet(key, rs) => {
                let intset_buf = rs.into_data();
                let IntSet { ints, .. } = IntSet::from_buf(&intset_buf)
                    .expect("faild to parse intset");
                buf.push(RedisFmt::Cmd("SADD"));
                buf.push(RedisFmt::Raw(key.into_data()));
                for intv in ints {
                    let int_str = format!("{}", intv);
                    buf.push(RedisFmt::Raw(int_str.into_bytes()));
                }
            }
            RedisData::ListZipList(key, rs) => {
                let local_buf = rs.into_data();
                let ZipList { entries, .. } = ZipList::from_buf(&local_buf)
                    .expect("faild to parse ziplist list");
                buf.push(RedisFmt::Cmd("LADD"));
                buf.push(RedisFmt::Raw(key.into_data()));
                let sp_data = entries.into_iter().map(|ZipListEntry { sp, .. }| sp);
                for data in sp_data {
                    buf.push(RedisFmt::Raw(data.into_data()));
                }

            }
            RedisData::HashZipList(key, rs) => {
                let local_buf = rs.into_data();
                let ZipList { entries, .. } = ZipList::from_buf(&local_buf)
                    .expect("faild to parse ziplist hashset");
                buf.push(RedisFmt::Cmd("LADD"));
                buf.push(RedisFmt::Raw(key.into_data()));
                let sp_data = entries.into_iter().map(|ZipListEntry { sp, .. }| sp.into_data());
                for data in sp_data {
                    buf.push(RedisFmt::Raw(data));
                }
            }
            RedisData::ZSetZipList(key, rs) => {
                let local_buf = rs.into_data();
                let ZipList { entries, .. } = ZipList::from_buf(&local_buf)
                    .expect("faild to parse ziplist sorted set");
                buf.push(RedisFmt::Cmd("LADD"));
                buf.push(RedisFmt::Raw(key.into_data()));
                let sp_data = entries.into_iter().map(|ZipListEntry { sp, .. }| sp.into_data());
                let mut is_score = false;
                for data in sp_data {
                    if is_score {
                        buf.push(RedisFmt::Raw(data));
                        continue;
                    }
                    let value = String::from_utf8(data)
                        .ok()
                        .and_then(|fv| fv.parse::<f64>().ok())
                        .expect("abort parse: wrong float format");
                    buf.push(RedisFmt::Raw(format!("{}", value).into_bytes()));
                    is_score = !is_score;
                }
            }
        };
        buf.push(RedisFmt::CRLF);
        1
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
    pub fn fmt(self, key: RedisString, buf: &mut Vec<RedisFmt>) -> usize {
        match self {
            ExpireTime::Ms(ms) => {
                let now = SystemTime::now()
                    .duration_since(time::UNIX_EPOCH)
                    .expect("system timer is too busy")
                    .as_secs();
                let sec = ms as u64 / 1000;
                if now > sec {
                    return 0;
                }
                buf.push(RedisFmt::Cmd("EXPIRE"));
                buf.push(RedisFmt::Raw(key.into_data()));
                buf.push(RedisFmt::Raw(format!("{}", sec - now).into_bytes()));
                1
            }
            ExpireTime::Sec(sec) => {
                let sec = sec as u64;
                let now = SystemTime::now()
                    .duration_since(time::UNIX_EPOCH)
                    .expect("system timer is too busy")
                    .as_secs();
                if now > sec {
                    return 0;
                }

                buf.push(RedisFmt::Cmd("EXPIRE"));
                buf.push(RedisFmt::Raw(key.into_data()));
                buf.push(RedisFmt::Raw(format!("{}", sec - now).into_bytes()));
                1
            }
            ExpireTime::None => 0,
        }
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
