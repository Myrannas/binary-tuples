use byteorder::{BigEndian, ByteOrder};
use constants::*;
use utils::*;
use errors::TupleError;
use std;
use uuid::Uuid;

#[derive(Clone, PartialEq, Debug)]
pub enum Segment {
    Bytes(Vec<u8>),
    String(String),
    Const(&'static str),
    Nested(Vec<Segment>),
    Integer(i64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    UUID(Uuid),
    Tuple(Vec<u8>),
}

pub(crate) fn encode_slice(input: &[Segment], buffer: &mut Vec<u8>) {
    for segment in input.iter() {
        segment.encode(buffer)
    }
}

impl Segment {
    pub(crate) fn encode(&self, buffer: &mut Vec<u8>) {
        match self {
            Segment::Bytes(data) => {
                encode_byte_string(BYTES_CODE, &data, buffer);
            }
            Segment::String(data) => {
                encode_byte_string(STRING_CODE, data.as_bytes(), buffer);
            }
            Segment::Const(data) => {
                encode_byte_string(STRING_CODE, data.as_bytes(), buffer);
            }
            Segment::Nested(inner) => {
                buffer.push(NESTED_CODE);
                encode_slice(&inner, buffer);
                buffer.push(NULL)
            }
            Segment::Integer(0) => {
                buffer.push(INT_ZERO_CODE);
            }
            Segment::Integer(std::i64::MIN) => {
                buffer.push(INT_ZERO_CODE - 8);
                let mut buf = [0; 8];
                BigEndian::write_u64(&mut buf, std::u64::MAX >> 1);
                buffer.extend_from_slice(&buf)
            }
            Segment::Integer(value) if *value > 0 => {
                let mut buf = [0; 8];
                BigEndian::write_u64(&mut buf, *value as u64);

                let empty_bytes = buf.iter()
                    .take_while(|v| { **v == 0 })
                    .count();

                buffer.push(INT_ZERO_CODE + 8 - empty_bytes as u8);
                buffer.extend_from_slice(&buf[empty_bytes..])
            }
            Segment::Integer(value) if *value < 0 => {
                let complement = (-*value) as u64;

                let mut buf = [0; 8];
                BigEndian::write_u64(&mut buf, complement as u64);

                let empty_bytes = buf.iter()
                    .take_while(|v| { **v == 0 })
                    .count();

                let num_bytes = 8 - empty_bytes;
                let size_limit = SIZE_LIMITS[num_bytes];

                BigEndian::write_u64(&mut buf, size_limit - complement);

                buffer.push(INT_ZERO_CODE - (num_bytes as u8));
                buffer.extend_from_slice(&buf[empty_bytes..]);
            }
            Segment::Tuple(value) => {
                buffer.extend_from_slice(&value);
            }
            Segment::Boolean(value) => {
                if *value {
                    buffer.push(TRUE_CODE)
                } else {
                    buffer.push(FALSE_CODE)
                }
            }
            Segment::UUID(value) => {
                buffer.push(UUID_CODE);
                buffer.extend_from_slice(value.as_bytes())
            }
            Segment::Float(value) => {
                buffer.reserve(5);
                buffer.push(FLOAT_CODE);

                buffer.extend_from_slice(&[0, 0, 0, 0]);

                let start = buffer.len() - 4;
                BigEndian::write_f32(&mut buffer[start..], *value);
                encode_sortable_float(&mut buffer[start..]);
            }
            Segment::Double(value) => {
                buffer.reserve(9);
                buffer.push(DOUBLE_CODE);

                buffer.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);

                let start = buffer.len() - 8;
                BigEndian::write_f64(&mut buffer[start..], *value);
                encode_sortable_float(&mut buffer[start..]);
            }
            _ => ()
        }
    }

    fn decode_segments(input: &[u8]) -> Result<(Vec<Segment>, usize), TupleError> {
        let mut segments = Vec::new();

        let mut index = 0;

        while index < input.len() {
            index += match input[index] {
                BYTES_CODE => {
                    let (read, result) = decode_byte_string(&input[index + 1..]);
                    segments.push(Segment::Bytes(result));

                    read + 1
                }
                STRING_CODE => {
                    let (read, result) = decode_byte_string(&input[index + 1..]);
                    let result = std::string::String::from_utf8(result)?;
                    segments.push(Segment::String(result));

                    read + 1
                }
                INT_NEG_MIN_CODE ... INT_NEG_MAX_CODE => {
                    let bytes = (INT_ZERO_CODE - input[index]) as usize;
                    let mut buf = [0; 8];

                    if index + bytes + 1 > input.len() {
                        return Err(TupleError::IntegerDecodeError { position: index })
                    }

                    for i in 0..bytes {
                        buf[8 - bytes + i] = input[index + i + 1];
                    }

                    let twos_complement = BigEndian::read_u64(&buf) as i64;

                    let value = if twos_complement == std::i64::MAX {
                        std::i64::MIN
                    } else {
                        twos_complement - SIZE_LIMITS[bytes] as i64
                    };

                    segments.push(Segment::Integer(value));

                    bytes + 1
                }
                INT_POS_MIN_CODE ... INT_POS_MAX_CODE => {
                    let bytes = (input[index] - INT_ZERO_CODE) as usize;
                    let mut buf = [0; 8];

                    if index + bytes + 1 > input.len() {
                        return Err(TupleError::IntegerDecodeError { position: index })
                    }

                    for i in 0..bytes {
                        buf[8 - bytes + i] = input[index + i + 1];
                    }

                    let value = BigEndian::read_u64(&buf) as i64;

                    segments.push(Segment::Integer(value));

                    bytes + 1
                }
                INT_ZERO_CODE => {
                    segments.push(Segment::Integer(0));

                    1
                }
                FLOAT_CODE => {
                    if index + 5 > input.len() {
                        return Err(TupleError::DecimalDecodeError{ position: index })
                    }

                    let mut float = [
                        input[index + 1],
                        input[index + 2],
                        input[index + 2],
                        input[index + 3]
                    ];
                    decode_sortable_float(&mut float);
                    segments.push(Segment::Float(BigEndian::read_f32(&float)));

                    5
                }
                DOUBLE_CODE => {
                    if index + 5 > input.len() {
                        return Err(TupleError::DecimalDecodeError{ position: index })
                    }

                    let mut float = [
                        input[index + 1],
                        input[index + 2],
                        input[index + 2],
                        input[index + 3],
                        input[index + 4],
                        input[index + 5],
                        input[index + 6],
                        input[index + 7],
                    ];
                    decode_sortable_float(&mut float);
                    segments.push(Segment::Double(BigEndian::read_f64(&float)));

                    9
                }
                TRUE_CODE => {
                    segments.push(Segment::Boolean(true));

                    1
                }
                FALSE_CODE => {
                    segments.push(Segment::Boolean(false));

                    1
                }
                UUID_CODE => {
                    match Uuid::from_bytes(&input[index + 1..index + 17]) {
                        Ok(uuid) => Ok(segments.push(Segment::UUID(uuid))),
                        Err(_) => Err(TupleError::UuidDecodeError {position: index})
                    }?;

                    17
                }
                NESTED_CODE => {
                    let (result, read) = Segment::decode_segments(&input[index + 1..])?;

                    segments.push(Segment::Nested(result));

                    if input[index + read + 1] != NULL {
                        return Err(TupleError::TruncatedNestedTuple);
                    }

                    read + 2
                }
                NULL => {
                    return Ok((segments, index))
                }
                value => return Err(TupleError::DecodeError { position: index, type_code: value })
            }
        }

        return Ok((segments, index))
    }

    pub(crate) fn decode(input: &[u8]) -> Result<Vec<Segment>, TupleError> {
        let ( segments, read ) = Segment::decode_segments(input)?;

        if read != input.len() {
            Err(TupleError::TruncatedTuple)
        } else {
            Ok(segments)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::UuidVersion;

    fn encode(segment: Segment) -> Vec<u8> {
        let mut buffer = Vec::new();
        segment.encode(&mut buffer);
        buffer
    }

    fn decode(buffer: &[u8]) -> Segment {
        let mut result = Segment::decode(buffer).unwrap();
        assert_eq!(result.len(), 1);
        let value = result.drain(0..1).next().unwrap();
        value
    }

    #[test]
    fn test_encode_integer() {
        let result = encode(Segment::Integer(1));

        assert_eq!(result, vec![INT_ZERO_CODE + 1, 1])
    }

    #[test]
    fn test_encode_large_integer() {
        let result = encode(Segment::Integer(257));

        assert_eq!(result, vec![INT_ZERO_CODE + 2, 1, 1])
    }

    #[test]
    fn test_encode_max_integer() {
        let result = encode(Segment::Integer(std::i64::MAX));

        assert_eq!(result, vec![INT_ZERO_CODE + 8, 127, 255, 255, 255, 255, 255, 255, 255])
    }

    #[test]
    fn test_encode_neg_255_integer() {
        let result = encode(Segment::Integer(-255));

        assert_eq!(result, vec![INT_ZERO_CODE - 1, 0])
    }

    #[test]
    fn test_encode_neg_256_integer() {
        let result = encode(Segment::Integer(-256));

        assert_eq!(result, vec![INT_ZERO_CODE - 2, 254, 255])
    }

    #[test]
    fn test_encode_min_integer() {
        let result = encode(Segment::Integer(std::i64::MIN));

        assert_eq!(result, vec![INT_ZERO_CODE - 8, 127, 255, 255, 255, 255, 255, 255, 255])
    }

    #[test]
    fn test_encode_sort_integer() {
        let max = encode(Segment::Integer(std::i64::MAX));
        let p257 = encode(Segment::Integer(256));
        let p256 = encode(Segment::Integer(256));
        let p1 = encode(Segment::Integer(1));
        let _0 = encode(Segment::Integer(0));
        let n1 = encode(Segment::Integer(-1));
        let n255 = encode(Segment::Integer(-256));
        let n256 = encode(Segment::Integer(-256));
        let min_p1 = encode(Segment::Integer(std::i64::MIN + 1));
        let min = encode(Segment::Integer(std::i64::MIN));

        let input = vec![
            min,
            min_p1,
            n256,
            n255,
            n1,
            _0,
            p1,
            p256,
            p257,
            max,
        ];

        let mut reversed = input.clone();
        reversed.reverse();
        reversed.sort();

        assert_eq!(input, reversed);
    }

    #[test]
    fn encode_const() {
        let builder = encode(Segment::Const("wow"));

        assert_eq!(builder, vec![STRING_CODE, 119, 111, 119, 0]);
    }

    #[test]
    fn encode_string() {
        let builder = encode(Segment::String(String::from("wow")));

        assert_eq!(builder, vec![STRING_CODE, 119, 111, 119, 0]);
    }

    #[test]
    fn encode_string_escaped() {
        let builder = encode(Segment::String(String::from("wow\0")));

        assert_eq!(builder, vec![STRING_CODE, 119, 111, 119, 0, 255, 0]);
    }


    #[test]
    fn encode_bytes() {
        let builder = encode(Segment::Bytes(vec![1, 2, 3, 4]));

        assert_eq!(builder, vec![BYTES_CODE, 1, 2, 3, 4, 0]);
    }

    #[test]
    fn encode_bytes_escaped() {
        let builder = encode(Segment::Bytes(vec![1, 2, 0, 3, 4]));

        assert_eq!(builder, vec![BYTES_CODE, 1, 2, 0, 255, 3, 4, 0]);
    }

    #[test]
    fn encode_int_zero() {
        let builder = encode(Segment::Integer(0));

        assert_eq!(builder, vec![INT_ZERO_CODE]);
    }

    #[test]
    fn encode_float() {
        let builder = encode(Segment::Float(1.0));

        assert_eq!(builder, vec![FLOAT_CODE, 191, 128, 0, 0]);
    }

    #[test]
    fn encode_larger_float() {
        let builder = encode(Segment::Float(2.0));

        assert_eq!(builder, vec![FLOAT_CODE, 192, 0, 0, 0]);
    }

    #[test]
    fn encode_even_larger_float() {
        let builder = encode(Segment::Float(31415.514));

        assert_eq!(builder, vec![FLOAT_CODE, 198, 245, 111, 7]);
    }

    #[test]
    fn test_encode_sort_float() {
        let p_inf = encode(Segment::Float(std::f32::INFINITY));
        let p_max = encode(Segment::Float(std::f32::MAX));
        let p1 = encode(Segment::Float(1.0));
        let p_min = encode(Segment::Float(std::f32::MIN_POSITIVE));
        let _0 = encode(Segment::Float(0.0));
        let n_min = encode(Segment::Float(-std::f32::MIN_POSITIVE));
        let n1 = encode(Segment::Float(-1.0));
        let n_max = encode(Segment::Float(-std::f32::MAX));
        let n_inf = encode(Segment::Float(std::f32::NEG_INFINITY));

        let input = vec![
            n_inf,
            n_max,
            n1,
            n_min,
            _0,
            p_min,
            p1,
            p_max,
            p_inf,
        ];

        let mut reversed = input.clone();
        reversed.reverse();
        reversed.sort();

        assert_eq!(input, reversed);
    }

    #[test]
    fn encode_double() {
        let builder = encode(Segment::Double(1.0));

        assert_eq!(builder, vec![DOUBLE_CODE, 191, 240, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn encode_larger_double() {
        let builder = encode(Segment::Double(2.0));

        assert_eq!(builder, vec![DOUBLE_CODE, 192, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn encode_max_double() {
        let builder = encode(Segment::Double(std::f64::MAX));

        assert_eq!(builder, vec![DOUBLE_CODE, 255, 239, 255, 255, 255, 255, 255, 255]);
    }

    #[test]
    fn encode_min_positive_double() {
        let builder = encode(Segment::Double(std::f64::MIN_POSITIVE));

        assert_eq!(builder, vec![DOUBLE_CODE, 128, 16, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn encode_inf_double() {
        let builder = encode(Segment::Double(std::f64::INFINITY));

        assert_eq!(builder, vec![DOUBLE_CODE, 255, 240, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn encode_neg_inf_double() {
        let builder = encode(Segment::Double(std::f64::NEG_INFINITY));

        assert_eq!(builder, vec![DOUBLE_CODE, 0, 15, 255, 255, 255, 255, 255, 255]);
    }

    #[test]
    fn test_encode_sort_double() {
        let p_inf = encode(Segment::Double(std::f64::INFINITY));
        let p_max = encode(Segment::Double(std::f64::MAX));
        let p1 = encode(Segment::Double(1.0));
        let p_min = encode(Segment::Double(std::f64::MIN_POSITIVE));
        let _0 = encode(Segment::Double(0.0));
        let n_min = encode(Segment::Double(-std::f64::MIN_POSITIVE));
        let n1 = encode(Segment::Double(-1.0));
        let n_max = encode(Segment::Double(-std::f64::MAX));
        let n_inf = encode(Segment::Double(std::f64::NEG_INFINITY));

        let input = vec![
            n_inf,
            n_max,
            n1,
            n_min,
            _0,
            p_min,
            p1,
            p_max,
            p_inf,
        ];

        let mut reversed = input.clone();
        reversed.reverse();
        reversed.sort();

        assert_eq!(input, reversed);
    }


    #[test]
    fn decode_string() {
        let result = decode(&vec![STRING_CODE, 119, 111, 119, 0]);

        assert_eq!(result, Segment::String(String::from("wow")));
    }

    #[test]
    fn decode_string_escaped() {
        let result = decode(&vec![STRING_CODE, 119, 111, 119, 0, 255, 0]);

        assert_eq!(result, Segment::String(String::from("wow\0")));
    }

    #[test]
    fn decode_string_start() {
        let result = decode(&vec![STRING_CODE, 0, 255, 119, 0, 255, 0]);

        assert_eq!(result, Segment::String(String::from("\0w\0")));
    }

    #[test]
    fn decode_bytes() {
        let result = decode(&vec![BYTES_CODE, 1, 2, 3, 4, 0]);

        assert_eq!(result, Segment::Bytes(vec![1, 2, 3, 4]));
    }

    #[test]
    fn decode_bytes_escaped() {
        let result = decode(&vec![BYTES_CODE, 1, 2, 0, 255, 3, 4, 0]);

        assert_eq!(result, Segment::Bytes(vec![1, 2, 0, 3, 4]));
    }

    #[test]
    fn decode_int_zero() {
        let result = decode(&vec![INT_ZERO_CODE]);

        assert_eq!(result, Segment::Integer(0));
    }

    #[test]
    fn decode_float() {
        let result = decode(&vec![FLOAT_CODE, 191, 128, 0, 0]);

        assert_eq!(result, Segment::Float(1.0039063));
    }

    #[test]
    fn decode_larger_float() {
        let result = decode(&vec![FLOAT_CODE, 192, 0, 0, 0]);

        assert_eq!(result, Segment::Float(2.0));
    }

    #[test]
    fn decode_even_larger_float() {
        let result = decode(&vec![FLOAT_CODE, 198, 245, 111, 7]);

        assert_eq!(result, Segment::Float(31482.717));
    }

    #[test]
    fn decode_double() {
        let result = decode(&vec![DOUBLE_CODE, 191, 240, 0, 0, 0, 0, 0, 0]);

        assert_eq!(result, Segment::Double(1.05859375));
    }

    #[test]
    fn decode_larger_double() {
        let result = decode(&vec![DOUBLE_CODE, 192, 0, 0, 0, 0, 0, 0, 0]);

        assert_eq!(result, Segment::Double(2.0));
    }

    #[test]
    fn decode_even_larger_double() {
        let result = decode(&vec![DOUBLE_CODE, 192, 222, 173, 224, 229, 96, 65, 137]);

        assert_eq!(result, Segment::Double(31610.716851562498));
    }

    #[test]
    fn test_decode_integer() {
        let result = decode(&vec![INT_ZERO_CODE + 1, 1]);

        assert_eq!(result, Segment::Integer(1));
    }

    #[test]
    fn test_decode_large_integer() {
        let result = decode(&vec![INT_ZERO_CODE + 2, 19, 136]);

        assert_eq!(result, Segment::Integer(5000));
    }

    #[test]
    fn test_decode_larger_integer() {
        let result = decode(&vec![INT_ZERO_CODE + 2, 1, 1]);

        assert_eq!(result, Segment::Integer(257));
    }

    #[test]
    fn test_decode_neg_integer() {
        let result = decode(&vec![INT_ZERO_CODE - 1, 254]);

        assert_eq!(result, Segment::Integer(-1));
    }

    #[test]
    fn test_decode_neg_one_integer() {
        let result = decode(&vec![INT_ZERO_CODE - 1, 1]);

        assert_eq!(result, Segment::Integer(-254));
    }

    #[test]
    fn test_decode_neg_malformed() {
        let result = Segment::decode(&vec![INT_ZERO_CODE - 1]).unwrap_err();

        assert_eq!(result, TupleError::IntegerDecodeError { position: 0 });
    }

    #[test]
    fn test_decode_pos_malformed() {
        let result = Segment::decode(&vec![INT_ZERO_CODE + 1]).unwrap_err();

        assert_eq!(result, TupleError::IntegerDecodeError { position: 0 });
    }

    #[test]
    fn test_decode_float_malformed() {
        let result = Segment::decode(&vec![FLOAT_CODE]).unwrap_err();

        assert_eq!(result, TupleError::DecimalDecodeError { position: 0 });
    }

    #[test]
    fn test_decode_decimal_malformed() {
        let result = Segment::decode(&vec![DOUBLE_CODE]).unwrap_err();

        assert_eq!(result, TupleError::DecimalDecodeError { position: 0 });
    }

    #[test]
    fn test_decode_max_integer() {
        let result = decode(&vec![INT_ZERO_CODE + 8, 127, 255, 255, 255, 255, 255, 255, 255]);

        assert_eq!(result, Segment::Integer(std::i64::MAX));
    }

    #[test]
    fn test_decode_max_neg_integer() {
        let result = decode(&vec![INT_ZERO_CODE - 8, 127, 255, 255, 255, 255, 255, 255, 255]);

        assert_eq!(result, Segment::Integer(std::i64::MIN));
    }

    #[test]
    fn test_decode_neg_boundary_integer() {
        let result = decode(&vec![INT_ZERO_CODE - 2, 254, 255]);

        assert_eq!(result, Segment::Integer(-256));
    }

    #[test]
    fn test_encode_double_true() {
        let builder = encode(Segment::Boolean(true));

        assert_eq!(builder, vec![TRUE_CODE]);
    }

    #[test]
    fn test_encode_double_false() {
        let builder = encode(Segment::Boolean(false));

        assert_eq!(builder, vec![FALSE_CODE]);
    }

    #[test]
    fn test_decode_boolen_true() {
        let result = decode(&vec![TRUE_CODE]);

        assert_eq!(result, Segment::Boolean(true));
    }

    #[test]
    fn test_decode_boolen_false() {
        let result = decode(&vec![FALSE_CODE]);

        assert_eq!(result, Segment::Boolean(false));
    }

    #[test]
    fn test_encode_uuid() {
        let uuid = Uuid::new(UuidVersion::Random).unwrap();
        let builder = encode(Segment::UUID(uuid));

        let mut expected_result = vec![UUID_CODE];
        expected_result.extend_from_slice(uuid.as_bytes());
        assert_eq!(builder, expected_result);
    }

    #[test]
    fn test_decode_uuid() {
        let input = vec![48, 197, 194, 162, 128, 228, 124, 65, 129, 148, 179, 194, 60, 213, 250, 237, 232];
        let result = decode(&input);
        let uuid_bytes = &input[1..];

        assert_eq!(result, Segment::UUID(Uuid::from_bytes(uuid_bytes).unwrap()));
    }

    #[test]
    fn test_encode_nested() {
        let builder = encode(Segment::Nested(vec![Segment::Const("Hello"), Segment::Boolean(true)]));

        assert_eq!(builder, vec![NESTED_CODE, STRING_CODE, 72, 101, 108, 108, 111, NULL, TRUE_CODE, NULL]);
    }

    #[test]
    fn test_encode_recursive_nested() {
        let builder = encode(Segment::Nested(vec![
            Segment::Nested(vec![
                Segment::Boolean(true),
                Segment::Const("Hello"),
            ]),
            Segment::Integer(5000)
        ]));

        assert_eq!(builder, vec![NESTED_CODE, NESTED_CODE, TRUE_CODE, STRING_CODE, 72, 101, 108, 108, 111, NULL, NULL, INT_ZERO_CODE + 2, 19, 136, NULL]);
    }

    #[test]
    fn test_decode_nested() {
        let result = decode(&vec![NESTED_CODE, STRING_CODE, 72, 101, 108, 108, 111, NULL, TRUE_CODE, NULL]);

        assert_eq!(result, Segment::Nested(vec![Segment::String(String::from("Hello")), Segment::Boolean(true)]))
    }

    #[test]
    fn test_decode_recursive_nested() {
        let builder = decode(&vec![NESTED_CODE, NESTED_CODE, TRUE_CODE, STRING_CODE, 72, 101, 108, 108, 111, NULL, NULL, INT_ZERO_CODE + 2, 19, 136, NULL]);

        assert_eq!(builder, Segment::Nested(vec![
            Segment::Nested(vec![
                Segment::Boolean(true),
                Segment::String(String::from("Hello")),
            ]),
            Segment::Integer(5000)
        ]));
    }
}