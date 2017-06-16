pub const REDIS_RDB_6BITLEN: u8 = 0b0;
pub const REDIS_RDB_14BITLEN: u8 = 0b01;
pub const REDIS_RDB_32BITLEN: u8 = 0b10;
pub const REDIS_RDB_ENCVAL: u8 = 0b11;

// pub const REDIS_RDB_LENERR: u32 = 111;

pub const REDIS_RDB_ENC_INT8: u8 = 0;        /* 8 bit signed integer */
pub const REDIS_RDB_ENC_INT16: u8 = 1;       /* 16 bit signed integer */
pub const REDIS_RDB_ENC_INT32: u8 = 2;       /* 32 bit signed integer */
pub const REDIS_RDB_ENC_LZF: u8 = 3;         /* string compressed with FASTLZ */

pub const REDIS_RDB_TYPE_STRING: u8 = 0;
pub const REDIS_RDB_TYPE_LIST: u8 = 1;
pub const REDIS_RDB_TYPE_SET: u8 = 2;
pub const REDIS_RDB_TYPE_ZSET: u8 = 3;
pub const REDIS_RDB_TYPE_HASH: u8 = 4;

// Object types for encoded objects.
pub const REDIS_RDB_TYPE_HASH_ZIPMAP: u8 = 9;
pub const REDIS_RDB_TYPE_LIST_ZIPLIST: u8 = 10;
pub const REDIS_RDB_TYPE_SET_INTSET: u8 = 11;
pub const REDIS_RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
pub const REDIS_RDB_TYPE_HASH_ZIPLIST: u8 = 13;

// Special RDB opcodes (saved/loaded with rdbSaveType/rdbLoadType).
pub const REDIS_RDB_OPCODE_EXPIRETIME_MS: u8 = 252;
pub const REDIS_RDB_OPCODE_EXPIRETIME_MS_LEN: usize = 8;

pub const REDIS_RDB_OPCODE_EXPIRETIME: u8 = 253;
pub const REDIS_RDB_OPCODE_EXPIRETIME_LEN: usize = 4;

pub const REDIS_RDB_OPCODE_SELECTDB: u8 = 0xFE;
pub const REDIS_RDB_FLAG_ZIPLIST_ENTRY_LEN_MAX: u8 = 253;

pub const REDIS_RDB_FLAG_ZIPLIST_ENTRY_SMALL_STR: u8 = 0b00;
pub const REDIS_RDB_FLAG_ZIPLIST_ENTRY_NORMAL_STR: u8 = 001;
pub const REDIS_RDB_FLAG_ZIPLIST_ENTRY_LARGE_STR: u8 = 0b10;

pub const REDIS_RDB_FLAG_ZIPLIST_ENTRY_NORMAL_INT: u8 = 0b00;
pub const REDIS_RDB_FLAG_ZIPLIST_ENTRY_LARGE_INT: u8 = 0b01;
pub const REDIS_RDB_FLAG_ZIPLIST_ENTRY_EXLARGE_INT: u8 = 0b10;

pub const REDIS_RDB_FLAG_ZIPLIST_ENTRY_LARGE_TRIM_INT: u8 = 0b0000;
pub const REDIS_RDB_FLAG_ZIPLIST_ENTRY_SMALL_INT: u8 = 0b1110;

pub const REDIS_MAGIC_STRING: &str = "REDIS";
