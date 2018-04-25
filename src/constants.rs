use std::u64::MAX as MAX_VALUE;

pub const BYTES_CODE: u8 = 0x01;
pub const STRING_CODE: u8 = 0x02;
pub const NESTED_CODE: u8 = 0x05;
pub const INT_ZERO_CODE: u8 = 0x14;
pub const INT_NEG_MIN_CODE: u8 = INT_ZERO_CODE - 8;
pub const INT_NEG_MAX_CODE: u8 = INT_ZERO_CODE - 1;
pub const INT_POS_MIN_CODE: u8 = INT_ZERO_CODE + 1;
pub const INT_POS_MAX_CODE: u8 = INT_ZERO_CODE + 8;
pub const FLOAT_CODE: u8 = 0x20;
pub const DOUBLE_CODE: u8 = 0x21;
pub const FALSE_CODE: u8 = 0x26;
pub const TRUE_CODE: u8 = 0x27;
pub const UUID_CODE: u8 = 0x30;

pub const NULL: u8 = 0x00;
pub const NULL_ESCAPE: u8 = 0xFF;

pub const SIZE_LIMITS: [u64; 9] = [
    0,
    (1 << (1 * 8)) - 1,
    (1 << (2 * 8)) - 1,
    (1 << (3 * 8)) - 1,
    (1 << (4 * 8)) - 1,
    (1 << (5 * 8)) - 1,
    (1 << (6 * 8)) - 1,
    (1 << (7 * 8)) - 1,
    MAX_VALUE,
];
