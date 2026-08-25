use std::convert::TryFrom;
use std::io::{Read, Seek, SeekFrom};

use crate::error::{Error, Result};
use crate::types::{BlockType, FileType, FstByteOrder};
use crate::util::{read_array, read_cstring, read_u64_be};

/// Fixed sizes of textual header fields, as defined by the FST specification.
pub const VERSION_FIELD_LEN: usize = 128;
/// Length (in bytes) reserved for the ASCII date field in the header.
pub const DATE_FIELD_LEN: usize = 119;

/// In-memory representation of the FST header block.
#[derive(Debug, Clone)]
pub struct Header {
    /// Length of the header section (should remain 329 bytes for compatibility).
    pub section_length: u64,
    /// Earliest simulation timestamp recorded in the trace.
    pub start_time: u64,
    /// Latest simulation timestamp recorded in the trace.
    pub end_time: u64,
    /// Writer memory usage hint captured during dump generation.
    pub memory_used: u64,
    /// Number of scopes written by the producer.
    pub scope_count: u64,
    /// Number of variables written by the producer.
    pub var_count: u64,
    /// Highest variable handle emitted (1-based index).
    pub max_handle: u64,
    /// Number of value-change sections present in the file.
    pub vc_section_count: u64,
    /// Base-10 exponent describing the simulation time unit (10^exponent seconds).
    pub timescale_exponent: i8,
    /// Producer version string (null terminated within the 128-byte buffer).
    pub version: String,
    /// Producer supplied date string (null terminated within the 119-byte buffer).
    pub date: String,
    /// Byte order used for IEEE-754 marker, frame, and real value-change bytes.
    pub double_byte_order: FstByteOrder,
    /// File type marker (e.g. Verilog, VHDL, mixed).
    pub file_type: FileType,
    /// Simulation time zero offset stored in the header.
    pub time_zero: i64,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            section_length: 329,
            start_time: 0,
            end_time: 0,
            memory_used: 0,
            scope_count: 0,
            var_count: 0,
            max_handle: 0,
            vc_section_count: 0,
            timescale_exponent: -9,
            version: String::from("wavefst"),
            date: String::new(),
            double_byte_order: FstByteOrder::native(),
            file_type: FileType::Verilog,
            time_zero: 0,
        }
    }
}

impl Header {
    /// Reads and parses the header block from the provided reader.
    pub fn read<R: Read + Seek>(reader: &mut R) -> Result<Self> {
        let mut block_type = [0u8; 1];
        reader.read_exact(&mut block_type)?;
        let block_type = BlockType::try_from(block_type[0]).map_err(|_| {
            Error::invalid(format!("unexpected first block type {:02x}", block_type[0]))
        })?;

        if block_type != BlockType::Header {
            return Err(Error::invalid(format!(
                "expected header block (0), found {block_type:?}"
            )));
        }

        let section_length = read_u64_be(reader)?;
        if section_length < 329 {
            return Err(Error::invalid(format!(
                "header section is {section_length} bytes; at least 329 are required"
            )));
        }
        let start_time = read_u64_be(reader)?;
        let end_time = read_u64_be(reader)?;
        let endian_bytes = read_array::<8, _>(reader)?;
        let double_byte_order =
            if f64::from_le_bytes(endian_bytes).to_bits() == std::f64::consts::E.to_bits() {
                FstByteOrder::LittleEndian
            } else if f64::from_be_bytes(endian_bytes).to_bits() == std::f64::consts::E.to_bits() {
                FstByteOrder::BigEndian
            } else {
                return Err(Error::invalid("unexpected FST endian test marker"));
            };
        let memory_used = read_u64_be(reader)?;
        let scope_count = read_u64_be(reader)?;
        let var_count = read_u64_be(reader)?;
        let max_handle = read_u64_be(reader)?;
        let vc_section_count = read_u64_be(reader)?;

        let mut timescale_buf = [0u8; 1];
        reader.read_exact(&mut timescale_buf)?;
        let timescale_exponent = timescale_buf[0].cast_signed();

        let version = read_cstring(reader, VERSION_FIELD_LEN)?;
        let date = read_cstring(reader, DATE_FIELD_LEN)?;

        let mut file_type_buf = [0u8; 1];
        reader.read_exact(&mut file_type_buf)?;
        let file_type = FileType::try_from(file_type_buf[0])
            .map_err(|_| Error::invalid(format!("unknown FST file type {}", file_type_buf[0])))?;
        let time_zero = read_u64_be(reader)?.cast_signed();
        if section_length > 329 {
            let extension = i64::try_from(section_length - 329)
                .map_err(|_| Error::invalid("header extension exceeds seek range"))?;
            reader.seek(SeekFrom::Current(extension))?;
        }

        Ok(Self {
            section_length,
            start_time,
            end_time,
            memory_used,
            scope_count,
            var_count,
            max_handle,
            vc_section_count,
            timescale_exponent,
            version,
            date,
            double_byte_order,
            file_type,
            time_zero,
        })
    }

    /// Returns the timescale as 10^exponent seconds.
    #[must_use]
    pub fn timescale_factor(&self) -> f64 {
        10f64.powi(i32::from(self.timescale_exponent))
    }
}
