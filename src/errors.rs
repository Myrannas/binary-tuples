use std::string::FromUtf8Error;

#[derive(Debug, PartialEq)]
pub enum TupleError {
    DecodeError { position: usize, type_code: u8 },
    TruncatedNestedTuple,
    TruncatedTuple,
    StringDecodeError,
    IntegerDecodeError{ position: usize },
    DecimalDecodeError { position: usize },
    UuidDecodeError { position: usize },
}

impl From<FromUtf8Error> for TupleError {
    fn from(_err: FromUtf8Error) -> Self {
        TupleError::StringDecodeError
    }
}