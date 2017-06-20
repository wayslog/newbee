//! Streamed RDB Rust Parser

extern crate lzf;

#[macro_use]
mod com;
mod consts;
mod codec;
mod types;
mod fmt;

pub use fmt::{RedisFmt, RedisCmd};
pub use com::{Result, Error};

use fmt::{RedisFormat, Group};
use com::*;
use codec::*;
use types::*;
use consts::*;

use std::io::{self, Read};
use std::mem;

pub struct DefaultRdbParser {
    local_buf: Vec<u8>,
    cursor: usize,
    parsed: Vec<RdbEntry>,
    state: State,
    end: Vec<u8>,
}

impl Default for DefaultRdbParser {
    fn default() -> Self {
        DefaultRdbParser {
            local_buf: Vec::new(),
            cursor: 0,
            parsed: Vec::new(),
            state: State::Header,
            end: Vec::new(),
        }
    }
}

impl DefaultRdbParser {
    pub fn read_to_cmd<R: Read>(&mut self, read: &mut R) -> Result<Vec<RedisCmd>> {
        let _readed = self.read_to_local(read)?;
        loop {
            match self.state {
                State::Data => {
                    let data = match self.data() {
                        Err(Error::More) => break,
                        Err(Error::Other) => {
                            self.state = State::Crc;
                            continue;
                        }
                        other => other?,
                    };
                    self.cursor += data.shift();
                    self.parsed.push(data);
                }
                State::Sector => {
                    let sector = match self.sector() {
                        Err(Error::Other) => {
                            self.state = State::Crc;
                            continue;
                        }
                        otherwise => otherwise?,
                    };
                    self.cursor += sector.shift();
                    self.state = State::Data;
                }
                State::Header => {
                    let header = self.header()?;
                    self.cursor += header.shift();
                    self.state = State::Sector;
                }
                State::Crc => {
                    self.end = self.crc()?;
                    self.state = State::End;
                }
                State::End => {
                    break;
                }
            };
        }

        let entries = self.drain_buf();
        let mut fmts = vec![];
        for entry in entries {
            entry.fmt(&mut fmts);
        }
        let groups = Group::group(fmts);
        Ok(groups)
    }

    fn drain_buf(&mut self) -> Vec<RdbEntry> {
        let mut entries = vec![];
        mem::swap(&mut entries, &mut self.parsed);
        self.cursor = 0;
        entries
    }
}

impl RdbParser for DefaultRdbParser {
    fn read_to_local<R: Read>(&mut self, read: &mut R) -> Result<usize> {
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
        if self.cursor > self.local_buf.len() {
            &self.local_buf[self.local_buf.len()..]
        } else {
            &self.local_buf[self.cursor..]
        }
    }
}


trait RdbParser {
    fn read_to_local<R: Read>(&mut self, read: &mut R) -> Result<usize>;
    fn local_buf(&self) -> &[u8];

    fn crc(&mut self) -> Result<Vec<u8>> {
        let src = self.local_buf();
        other!(src[0] != 0xff);
        Ok(src[1..].to_vec())
    }

    fn header(&mut self) -> Result<RdbEntry> {
        let src = self.local_buf();
        more!(src.len() < REDIS_MAGIC_STRING.len() + 4);
        let version = &src[REDIS_MAGIC_STRING.len()..REDIS_MAGIC_STRING.len() + 4];
        let version_str = String::from_utf8_lossy(version);
        let version_u32 = version_str.parse::<u32>().unwrap();
        Ok(RdbEntry::Version(version_u32))
    }

    fn sector(&mut self) -> Result<RdbEntry> {
        let src = self.local_buf();
        more!(src.len() < 2);
        other!(src[0] != REDIS_RDB_OPCODE_SELECTDB);
        let length = Length::from_buf(&src[1..])?;
        Ok(RdbEntry::Sector(length))
    }

    fn data(&mut self) -> Result<RdbEntry> {
        let src = self.local_buf();
        // meet EOF
        if src[0] == 0xff {
            return Err(Error::Other);
        }
        let expire = ExpireTime::from_buf(src)?;
        let data = RedisData::from_buf(&src[expire.shift()..])?;
        Ok(RdbEntry::Data {
            expire: expire,
            data: data,
        })
    }
}


#[derive(Debug)]
enum State {
    Header,
    Sector,
    Data,
    Crc,
    End,
}

#[derive(Debug)]
enum RdbEntry {
    Version(u32),
    Sector(Length),
    Data { expire: ExpireTime, data: RedisData },
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
