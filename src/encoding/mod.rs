//! Encoding helpers (varints, zig-zag encoding, etc.).

mod varint;
mod varint_signed;

pub use varint::{VARINT_MAX_LEN, decode_varint, decode_varint_with_len, encode_varint};
#[cfg(feature = "reader")]
pub(crate) use varint_signed::decode_svarint_with_len;
pub use varint_signed::encode_svarint;
