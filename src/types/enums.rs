#![allow(
    missing_docs,
    reason = "wire enum variants intentionally retain the self-describing upstream libfst names"
)]

use num_enum::{IntoPrimitive, TryFromPrimitive};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Enumeration of high level block identifiers present in FST streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum BlockType {
    Header = 0,
    VcData = 1,
    Blackout = 2,
    Geometry = 3,
    Hierarchy = 4,
    VcDataDynAlias = 5,
    HierarchyLz4 = 6,
    HierarchyLz4Duo = 7,
    VcDataDynAlias2 = 8,
    ZWrapper = 254,
    Skip = 255,
}

/// Scope/type markers used in hierarchy streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum ScopeType {
    VcdModule = 0,
    VcdTask = 1,
    VcdFunction = 2,
    VcdBegin = 3,
    VcdFork = 4,
    VcdGenerate = 5,
    VcdStruct = 6,
    VcdUnion = 7,
    VcdClass = 8,
    VcdInterface = 9,
    VcdPackage = 10,
    VcdProgram = 11,
    VhdlArchitecture = 12,
    VhdlProcedure = 13,
    VhdlFunction = 14,
    VhdlRecord = 15,
    VhdlProcess = 16,
    VhdlBlock = 17,
    VhdlForGenerate = 18,
    VhdlIfGenerate = 19,
    VhdlGenerate = 20,
    VhdlPackage = 21,
    SvArray = 22,
    GenAttrBegin = 252,
    GenAttrEnd = 253,
    VcdScope = 254,
    VcdUpscope = 255,
}

/// HDL family recorded in the FST header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum FileType {
    Verilog = 0,
    Vhdl = 1,
    Mixed = 2,
}

/// Byte order used for native IEEE-754 values in an FST stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum FstByteOrder {
    LittleEndian,
    BigEndian,
}

impl FstByteOrder {
    /// Returns the byte order of the current target.
    #[must_use]
    pub const fn native() -> Self {
        if cfg!(target_endian = "little") {
            Self::LittleEndian
        } else {
            Self::BigEndian
        }
    }

    /// Encodes an IEEE-754 value in this byte order.
    #[must_use]
    pub fn encode_f64(self, value: f64) -> [u8; 8] {
        match self {
            Self::LittleEndian => value.to_le_bytes(),
            Self::BigEndian => value.to_be_bytes(),
        }
    }

    /// Decodes an IEEE-754 value in this byte order.
    #[must_use]
    pub fn decode_f64(self, bytes: [u8; 8]) -> f64 {
        match self {
            Self::LittleEndian => f64::from_le_bytes(bytes),
            Self::BigEndian => f64::from_be_bytes(bytes),
        }
    }
}

/// All supported variable kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum VarType {
    VcdEvent = 0,
    VcdInteger = 1,
    VcdParameter = 2,
    VcdReal = 3,
    VcdRealParameter = 4,
    VcdReg = 5,
    VcdSupply0 = 6,
    VcdSupply1 = 7,
    VcdTime = 8,
    VcdTri = 9,
    VcdTriand = 10,
    VcdTrior = 11,
    VcdTrireg = 12,
    VcdTri0 = 13,
    VcdTri1 = 14,
    VcdWand = 15,
    VcdWire = 16,
    VcdWor = 17,
    VcdPort = 18,
    VcdSparseArray = 19,
    VcdRealtime = 20,
    GenString = 21,
    SvBit = 22,
    SvLogic = 23,
    SvInt = 24,
    SvShortInt = 25,
    SvLongInt = 26,
    SvByte = 27,
    SvEnum = 28,
    SvShortReal = 29,
}

/// Signal direction (input/output) metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum VarDir {
    Implicit = 0,
    Input = 1,
    Output = 2,
    Inout = 3,
    Buffer = 4,
    Linkage = 5,
}

/// Top-level hierarchy attribute category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum HierarchyAttributeType {
    Misc = 0,
    Array = 1,
    Enum = 2,
    Pack = 3,
}

/// Subtypes for [`HierarchyAttributeType::Misc`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum MiscAttributeType {
    Comment = 0,
    EnvironmentVariable = 1,
    SupplementalVariable = 2,
    Pathname = 3,
    SourceStem = 4,
    SourceInstantiationStem = 5,
    ValueList = 6,
    EnumTable = 7,
    Unknown = 8,
}

/// Subtypes for [`HierarchyAttributeType::Array`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum ArrayAttributeType {
    None = 0,
    Unpacked = 1,
    Packed = 2,
    Sparse = 3,
}

/// Subtypes for [`HierarchyAttributeType::Enum`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum EnumValueType {
    SvInteger = 0,
    SvBit = 1,
    SvLogic = 2,
    SvInt = 3,
    SvShortInt = 4,
    SvLongInt = 5,
    SvByte = 6,
    SvUnsignedInteger = 7,
    SvUnsignedBit = 8,
    SvUnsignedLogic = 9,
    SvUnsignedInt = 10,
    SvUnsignedShortInt = 11,
    SvUnsignedLongInt = 12,
    SvUnsignedByte = 13,
    Reg = 14,
    Time = 15,
}

/// Subtypes for [`HierarchyAttributeType::Pack`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum AggregatePackType {
    None = 0,
    Unpacked = 1,
    Packed = 2,
    TaggedPacked = 3,
}

/// Supplemental HDL object kind encoded by libfst `CreateVar2` metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum SupplementalVarType {
    None = 0,
    VhdlSignal = 1,
    VhdlVariable = 2,
    VhdlConstant = 3,
    VhdlFile = 4,
    VhdlMemory = 5,
}

/// Supplemental HDL data type encoded by libfst `CreateVar2` metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u16)]
pub enum SupplementalDataType {
    None = 0,
    VhdlBoolean = 1,
    VhdlBit = 2,
    VhdlBitVector = 3,
    VhdlStdUlogic = 4,
    VhdlStdUlogicVector = 5,
    VhdlStdLogic = 6,
    VhdlStdLogicVector = 7,
    VhdlUnsigned = 8,
    VhdlSigned = 9,
    VhdlInteger = 10,
    VhdlReal = 11,
    VhdlNatural = 12,
    VhdlPositive = 13,
    VhdlTime = 14,
    VhdlCharacter = 15,
    VhdlString = 16,
}

/// Compression marker used inside value-change blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PackType {
    None,
    Zlib,
    FastLz,
    Lz4,
}

impl PackType {
    /// Converts the on-disk marker (single byte) into a [`PackType`].
    #[must_use]
    pub fn from_marker(marker: u8) -> Option<Self> {
        match marker {
            b'Z' | b'!' | b'^' => Some(Self::Zlib),
            b'F' => Some(Self::FastLz),
            b'4' => Some(Self::Lz4),
            0 => Some(Self::None),
            _ => None,
        }
    }

    /// Returns the marker byte used in value-change blocks.
    #[must_use]
    pub fn marker(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Zlib => b'Z',
            Self::FastLz => b'F',
            Self::Lz4 => b'4',
        }
    }
}
