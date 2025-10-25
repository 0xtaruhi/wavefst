use std::borrow::Cow;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Represents a logical value associated with a signal at a given time.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SignalValue<'a> {
    /// Single bit encoded using `'0'`, `'1'`, `'x'`, `'z'`, etc.
    Bit(char),
    /// Packed vector represented as an ASCII string slice.
    Vector(Cow<'a, str>),
    /// Boolean vector packed into bits. The `width` reflects the number of logical bits.
    PackedBits {
        /// Number of logical bits encoded in the packed representation.
        width: u32,
        /// Packed bit payload (MSB-first within each byte).
        bits: Cow<'a, [u8]>,
    },
    /// Real (IEEE-754 double) value.
    Real(f64),
    /// Arbitrary bytes (used for strings, enums, packed structures).
    Bytes(Cow<'a, [u8]>),
}

impl<'a> SignalValue<'a> {
    /// Returns `true` if the value denotes an unknown (`x`) state.
    pub fn is_unknown(&self) -> bool {
        matches!(self, SignalValue::Bit(ch) if *ch == 'x' || *ch == 'X')
    }

    /// Converts the value into an owned representation.
    pub fn into_owned(self) -> SignalValue<'static> {
        match self {
            SignalValue::Bit(ch) => SignalValue::Bit(ch),
            SignalValue::Vector(v) => SignalValue::Vector(Cow::Owned(v.into_owned())),
            SignalValue::PackedBits { width, bits } => SignalValue::PackedBits {
                width,
                bits: Cow::Owned(bits.into_owned()),
            },
            SignalValue::Real(v) => SignalValue::Real(v),
            SignalValue::Bytes(bytes) => SignalValue::Bytes(Cow::Owned(bytes.into_owned())),
        }
    }
}
