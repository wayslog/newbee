use std::result;
use lzf;
use std::io;
use std::convert::From;
use std::string::FromUtf8Error;
use std::num::ParseFloatError;

pub trait Shift {
    #[inline]
    fn shift(&self) -> usize;
}

pub trait FromBuf
    where Self: Sized
{
    fn from_buf(src: &[u8]) -> Result<Self>;
}


#[macro_export]
macro_rules! more{
        ($e: expr) => {
            if $e {
                return Err(Error::More);
            }
        }
    }

#[macro_export]
macro_rules! other{
        ($e: expr) => {
            if $e {
                return Err(Error::Other);
            }
        }
    }

#[macro_export]
macro_rules! faild{
        ($e: expr, $situation: expr) => {
            if $e {
                return Err(Error::Faild($situation));
            }
        }
    }

#[macro_export]
macro_rules! choice {
        ($e: expr) => {
            match $e {
                Ok(lp) => return Ok(lp),
                Err(Error::Other) => {}
                Err(err) => return Err(err),
            };
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

pub fn data_to_float(buf: Vec<u8>) -> Result<f64> {
    let sv = String::from_utf8(buf)?;
    let val = sv.parse::<f64>()?;
    Ok(val)
}
