use std::mem;

pub enum RedisFmt {
    Cmd(&'static str),
    Raw(Vec<u8>),
    CRLF,
}

impl RedisFmt {
    fn is_crlf(&self) -> bool {
        match self {
            &RedisFmt::CRLF => true,
            _ => false,
        }
    }
    pub fn into_data(self) -> Vec<u8> {
        match self {
            RedisFmt::Cmd(cmd) => cmd.to_owned().into_bytes(),
            RedisFmt::Raw(buf) => buf,
            RedisFmt::CRLF => b"\r\n".to_vec(),
        }
    }
}

pub trait RedisFormat
    where Self: Sized
{
    fn fmt(self, buf: &mut Vec<RedisFmt>) -> usize;
}


pub type RedisFmtList = Vec<RedisFmt>;
pub struct RedisCmd(pub Vec<RedisFmt>);

pub trait Group {
    fn group(self) -> Vec<RedisCmd>;
}

impl Group for RedisFmtList {
    fn group(self) -> Vec<RedisCmd> {
        let mut group = vec![];
        let mut local = vec![];
        for fmt in self {
            if fmt.is_crlf() {
                let mut tmp = vec![];
                mem::swap(&mut tmp, &mut local);
                group.push(RedisCmd(tmp));
                continue;
            }
            local.push(fmt);
        }
        group
    }
}
