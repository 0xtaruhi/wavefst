//! Encoding helpers (varints, zig-zag encoding, etc.).

mod varint;
mod varint_signed;

pub use varint::{VARINT_MAX_LEN, decode_varint, decode_varint_with_len, encode_varint};
pub use varint_signed::{decode_svarint, encode_svarint};
