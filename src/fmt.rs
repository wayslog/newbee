use std::mem;

#[derive(Debug, Clone)]
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


#[derive(Debug, Clone)]
pub struct RedisCmd(pub Vec<RedisFmt>);

impl RedisCmd {
    pub fn into_data(self) -> Vec<Vec<u8>> {
        let RedisCmd(cmds) = self;
        cmds.into_iter().map(|x| x.into_data()).collect()
    }
}

pub trait Group {
    fn group(self) -> Vec<RedisCmd>;
}

impl Group for Vec<RedisFmt> {
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
