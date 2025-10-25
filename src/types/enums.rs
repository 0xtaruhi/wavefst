#![allow(missing_docs)]

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
    GenAttrBegin = 252,
    GenAttrEnd = 253,
    VcdScope = 254,
    VcdUpscope = 255,
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
    pub fn marker(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Zlib => b'Z',
            Self::FastLz => b'F',
            Self::Lz4 => b'4',
        }
    }
}
