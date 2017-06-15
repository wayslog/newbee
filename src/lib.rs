//! Streamed RDB Rust Parser

extern crate lzf;

#[macro_use]
pub mod com;
pub mod consts;
pub mod codec;
pub mod types;
pub mod fmt;

pub use fmt::{Group, RedisFormat, RedisFmt, RedisCmd};
pub use com::*;
pub use codec::*;
pub use types::*;
pub use consts::*;

use std::io::{self, Read};
use std::mem;

pub struct DefaultRdbParser {
    local_buf: Vec<u8>,
    cursor: usize,
    parsed: Vec<RdbEntry>,
    state: State,
}

impl DefaultRdbParser {
    pub fn read_to_cmd<R: Read>(&mut self, read: R) -> Result<Vec<RedisCmd>> {
        let _readed = self.read_to_local(read)?;
        loop {
            match self.state {
                State::Data => {
                    let data = match self.data() {
                        Err(Error::More) => break,
                        other => other?,
                    };
                    self.cursor += data.shift();
                    self.parsed.push(data);
                }
                State::Sector => {
                    let sector = self.sector()?;
                    self.cursor += sector.shift();
                    // println!("read sector: {:?}", sector);
                    self.state = State::Data;
                }
                State::Header => {
                    let header = self.header()?;
                    self.cursor += header.shift();
                    // println!("read header: {:?}", header);
                    self.state = State::Sector;
                }
            };
        }
        let entries = self.clear_buf();
        let mut fmts = vec![];
        for entry in entries {
            entry.fmt(&mut fmts);
        }
        let groups = Group::group(fmts);
        Ok(groups)
    }

    fn clear_buf(&mut self) -> Vec<RdbEntry> {
        let mut entries = vec![];
        mem::swap(&mut entries, &mut self.parsed);
        self.cursor = 0;
        entries
    }
}

impl RdbParser for DefaultRdbParser {
    fn read_to_local<R: Read>(&mut self, mut read: R) -> Result<usize> {
        let start_len = self.local_buf.len();
        let mut len = start_len;
        let mut new_write_size = 16;
        let ret;
        loop {
            if len == self.local_buf.len() {
                new_write_size *= 2;
                self.local_buf.resize(len + new_write_size, 0);
            }

            match read.read(&mut self.local_buf[len..]) {
                Ok(0) => {
                    ret = Ok(len - start_len);
                    break;
                }
                Ok(n) => len += n,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    ret = Ok(len - start_len);
                    break;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => {
                    ret = Err(Error::IoError(e));
                    break;
                }
            }
        }

        self.local_buf.truncate(len);
        ret
    }

    fn local_buf(&self) -> &[u8] {
        &self.local_buf[..]
    }
}


pub trait RdbParser {
    fn read_to_local<R: Read>(&mut self, read: R) -> Result<usize>;
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
    Data,
}

#[derive(Debug)]
pub enum RdbEntry {
    Version(u32),
    Sector(u8),
    Data { expire: ExpireTime, data: RedisData },
}
impl RdbEntry {
    pub fn is_data(&self) -> bool {
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

impl RedisFormat for RdbEntry {
    fn fmt(self, buf: &mut Vec<RedisFmt>) -> usize {
        match self {
            RdbEntry::Data { expire, data } => {
                let key = data.copy_key();
                let mut count = data.fmt(buf);
                count += expire.fmt(key, buf);
                count
            }
            _ => 0,
        }
    }
}
