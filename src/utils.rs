use constants::*;

pub fn decode_byte_string(input: &[u8]) -> (usize, Vec<u8>) {
    let mut read = 0;
    let mut skip = false;
    let mut bytes = Vec::with_capacity(1024);
    for vals in input.windows(2) {
        if skip {
            skip = false;
            read += 1;
        } else {
            if vals[0] == 0 {
                read += 1;
                if vals[1] != 0xFF {
                    return (read, bytes)
                } else {
                    bytes.push(vals[0]);
                    skip = true;
                }
            } else {
                read += 1;
                bytes.push(vals[0])
            }
        }
    }

    (read + 1, bytes)
}

pub fn encode_byte_string(type_code: u8, input: &[u8], buffer: &mut Vec<u8>) {
    buffer.reserve(input.len() + 2);

    buffer.push(type_code);

    for i in 0..input.len() {
        match input[i] {
            NULL => {
                buffer.push(NULL);
                buffer.push(NULL_ESCAPE);
            }
            value => {
                buffer.push(value)
            }
        }
    }

    buffer.push(NULL);
}

pub fn encode_sortable_float(bytes: &mut [u8]) {
    if (bytes[0] & 0x80) != 0x00 {
        for i in 0..bytes.len() {
            bytes[i] ^= 0xff;
        }
    } else {
        bytes[0] ^= 0x80;
    }
}

pub fn decode_sortable_float(bytes: &mut [u8]) {
    if (bytes[0] & 0x80) != 0x80 {
        for i in 0..bytes.len() {
            bytes[i] ^= 0xff;
        }
    } else {
        bytes[0] ^= 0x80;
    }
}