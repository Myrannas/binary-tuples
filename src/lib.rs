extern crate byteorder;
extern crate uuid;

pub mod segment;
mod constants;
mod utils;
mod errors;

use uuid::Uuid;
use segment::Segment;
use errors::TupleError;

#[derive(Clone)]
/// A builder for serialized tuples
pub struct Tuple {
    buffer: Vec<u8>
}

impl Tuple {
    /// Create a new tuple builder
    pub fn new() -> Tuple {
        Tuple {
            buffer: Vec::with_capacity(128)
        }
    }

    /// Create a new tuple builder with a fixed backing capacitys
    pub fn with_capacity(capacity: usize) -> Tuple {
        Tuple {
            buffer: Vec::with_capacity(capacity)
        }
    }

    /// Create a new tuple builder from an existing byte array
    pub fn from_bytes(bytes: &[u8]) -> Tuple {
        Tuple {
            buffer: Vec::from(bytes)
        }
    }

    /// Add an individual segment to this tuple builder.
    ///
    /// ## Notes
    /// It is recommended to import AddToTuple as it greatly simplifies this API
    pub fn add_segment(&mut self, input: &Segment) {
        input.encode(&mut self.buffer);
    }

    /// Directly embed the contents of another tuple builder in this builder
    ///
    /// ## Notes
    /// This is more efficient than adding a segment slice as the backing buffer can be memcopied.
    pub fn add_builder(&mut self, input: &Tuple) {
        self.buffer.extend_from_slice(&input.buffer);
    }

    /// Return a serialized tuple
    pub fn into_bytes(self) -> Vec<u8> {
        self.buffer
    }

    /// Return a serialized tuple
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// Deserialize the segments which make up this tuple
    pub fn as_segments(&self) -> Result<Vec<Segment>, TupleError> {
        Segment::decode(&self.buffer)
    }
}

pub trait AddToTuple<T> where Self : Sized {
    fn add(&mut self, input: T);

    fn with(mut self, input: T) -> Self {
        self.add(input);

        self
    }
}

impl AddToTuple<i64> for Tuple {
    fn add(&mut self, v: i64) {
        self.add_segment(&Segment::Integer(v));
    }
}

impl AddToTuple<String> for Tuple {
    fn add(&mut self, v: String) {
        self.add_segment(&Segment::String(v));
    }
}

impl AddToTuple<f32> for Tuple {
    fn add(&mut self, v: f32) {
        self.add_segment(&Segment::Float(v));
    }
}

impl AddToTuple<f64> for Tuple {
    fn add(&mut self, v: f64) {
        self.add_segment(&Segment::Double(v));
    }
}

impl AddToTuple<Vec<u8>> for Tuple {
    fn add(&mut self, v: Vec<u8>) {
        self.add_segment(&Segment::Bytes(v));
    }
}

impl AddToTuple<&'static str> for Tuple {
    fn add(&mut self, v: &'static str) {
        self.add_segment(&Segment::Const(v));
    }
}

impl AddToTuple<Uuid> for Tuple {
    fn add(&mut self, v: Uuid) {
        self.add_segment(&Segment::UUID(v));
    }
}

impl AddToTuple<Vec<Segment>> for Tuple {
    fn add(&mut self, v: Vec<Segment>) {
        self.add_segment(&Segment::Nested(v));
    }
}

impl<'a> AddToTuple<&'a Tuple> for Tuple {
    fn add(&mut self, v: &'a Tuple) {
        self.add_builder(&v);
    }
}

impl AddToTuple<Tuple> for Tuple {
    fn add(&mut self, v: Tuple) {
        self.add_builder(&v);
    }
}

#[macro_export]
/**
 * A macro for creating a serialized tuple builder
 *
 * Supports all types of segments as plain values - these are wrapped in TupleSegments before
 * being serialized.
 *
 * Note: calling `tuple!(tuple!())` does not create a nested tuple - nested tuples are not supported in the
 *      public api yet.
 */
macro_rules! tuple {
    ($( $x:expr ),*) => {
        {
            use $crate::{Tuple, AddToTuple};

            let mut builder = Tuple::new();
            $(
                builder.add($x);
            )*
            builder
        }
    };
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_tuple_macro() {
        let result = tuple!("Test").into_bytes();

        assert_eq!(result, vec![2, 84, 101, 115, 116, 0]);
    }

    #[test]
    fn test_tuple_macro_prefix() {
        let base_tuple = tuple!("user");
        let result = tuple!(&base_tuple, "Test");

        assert_eq!(result.into_bytes(), vec![2, 117, 115, 101, 114, 0, 2, 84, 101, 115, 116, 0]);
    }
}