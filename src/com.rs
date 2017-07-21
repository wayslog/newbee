use std::result;
use lzf;
use std::io;
use std::convert::From;
use std::string::FromUtf8Error;
use std::num::ParseFloatError;

use byteorder::{BigEndian, LittleEndian, ByteOrder};

pub trait Shift {
    #[inline]
    fn shift(&self) -> usize;
}

pub trait FromBuf
    where Self: Sized
{
    fn from_buf(src: &[u8]) -> Result<Self>;
}


macro_rules! more{
        ($e: expr) => {
            if $e {
                return Err(Error::More);
            }
        }
    }

macro_rules! other{
        ($e: expr) => {
            if $e {
                return Err(Error::Other);
            }
        }
    }

macro_rules! faild{
        ($e: expr, $situation: expr) => {
            if $e {
                return Err(Error::Faild($situation));
            }
        }
    }

macro_rules! choice {
        ($e: expr) => {
            match $e {
                Ok(lp) => return Ok(lp),
                Err(Error::Other) => {}
                Err(err) => return Err(err),
            };
        }
    }


impl Shift for u8 {
    #[inline]
    fn shift(&self) -> usize {
        1
    }
}

impl Shift for u32 {
    #[inline]
    fn shift(&self) -> usize {
        4
    }
}

impl FromBuf for u32 {
    fn from_buf(src: &[u8]) -> Result<u32> {
        more!(src.len() < 4);
        Ok(buf_to_u32(src))
    }
}

impl FromBuf for u8
    where Self: Sized
{
    fn from_buf(src: &[u8]) -> Result<Self> {
        more!(src.len() < 1);
        Ok(src[0])
    }
}

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ParserError(String),
    More,
    Faild(&'static str),
    Other,
    LzfError(lzf::LzfError),
    IoError(io::Error),
    FromUtf8Error(FromUtf8Error),
    ParseFloatError(ParseFloatError),
}

impl From<io::Error> for Error {
    fn from(oe: io::Error) -> Error {
        Error::IoError(oe)
    }
}

impl From<ParseFloatError> for Error {
    fn from(oe: ParseFloatError) -> Error {
        Error::ParseFloatError(oe)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(oe: FromUtf8Error) -> Error {
        Error::FromUtf8Error(oe)
    }
}

impl From<lzf::LzfError> for Error {
    fn from(oe: lzf::LzfError) -> Error {
        Error::LzfError(oe)
    }
}

#[inline]
pub fn buf_to_i32(src: &[u8]) -> i32 {
    LittleEndian::read_i32(src)
}

#[inline]
pub fn buf_to_i32_trim(src: &[u8]) -> i32 {
    let mut vi32 = 0i32;
    vi32 |= (src[0] as i32) << 0;
    vi32 |= (src[1] as i32) << 8;
    vi32 |= (src[2] as i32) << 16;
    vi32
}

#[inline]
pub fn buf_to_u32(src: &[u8]) -> u32 {
    LittleEndian::read_u32(src)
}

#[inline]
pub fn buf_to_u32_big(src: &[u8]) -> u32 {
    BigEndian::read_u32(src)
}


#[inline]
pub fn buf_to_u64(src: &[u8]) -> u64 {
    LittleEndian::read_u64(src)
}

#[inline]
pub fn buf_to_i64(src: &[u8]) -> i64 {
    LittleEndian::read_i64(src)
}

#[inline]
pub fn buf_to_u16(src: &[u8]) -> u16 {
    LittleEndian::read_u16(src)
}

#[inline]
pub fn buf_to_u16_big(src: &[u8]) -> u16 {
    BigEndian::read_u16(src)
}

#[inline]
pub fn buf_to_i16(src: &[u8]) -> i16 {
    LittleEndian::read_i16(src)
}

#[inline]
pub fn min<T: PartialOrd + Copy>(lhs: T, rhs: T) -> T {
    if lhs > rhs { rhs } else { lhs }
}
