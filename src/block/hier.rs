#![allow(missing_docs)]

use std::io::{Read, Write};

#[cfg(feature = "gzip")]
use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};
#[cfg(feature = "lz4")]
use lz4_flex::block::{compress as lz4_compress, decompress as lz4_decompress};

use crate::encoding::{decode_varint_with_len, encode_varint};
use crate::error::{Error, Result};
use crate::types::{BlockType, ScopeType, VarDir, VarType};
use crate::util::read_u64_be;

/// Fully decoded hierarchy block retaining the original token ordering.
#[derive(Debug, Clone, Default)]
pub struct HierarchyBlock {
    pub items: Vec<HierarchyItem>,
    pub scopes: Vec<ScopeEntry>,
    pub variables: Vec<VarEntry>,
    pub attributes: Vec<AttributeEntry>,
}

/// Ordered representation of the hierarchy token stream.
#[derive(Debug, Clone)]
pub enum HierarchyItem {
    ScopeBegin { scope_index: usize },
    ScopeEnd,
    AttributeBegin { attribute_index: usize },
    AttributeEnd,
    Var { var_index: usize },
}

/// Describes a scope (module, architecture, etc.).
#[derive(Debug, Clone)]
pub struct ScopeEntry {
    pub scope_type: ScopeType,
    pub name: String,
    pub component: Option<String>,
    pub parent: Option<usize>,
}

/// Attribute metadata emitted inside the hierarchy stream.
#[derive(Debug, Clone)]
pub struct AttributeEntry {
    pub attr_type: u8,
    pub subtype: u8,
    pub name: String,
    pub argument: u64,
    pub scope: Option<usize>,
}

/// Describes a declared variable.
#[derive(Debug, Clone)]
pub struct VarEntry {
    pub var_type: VarType,
    pub direction: VarDir,
    pub name: String,
    pub length: Option<u32>,
    pub handle: u32,
    pub alias_of: Option<u32>,
    pub scope: Option<usize>,
    pub is_alias: bool,
}

impl HierarchyBlock {
    /// Decodes a hierarchy block, decompressing the payload based on the block type and parsing the
    /// token stream into structured data.
    pub fn decode_block<R: Read>(
        reader: &mut R,
        block_type: BlockType,
        section_length: u64,
    ) -> Result<Self> {
        if section_length < 16 {
            return Err(Error::invalid(
                "hierarchy section shorter than required metadata",
            ));
        }

        let payload_len = section_length
            .checked_sub(8)
            .ok_or_else(|| Error::invalid("hierarchy section length underflow"))?;
        if payload_len < 8 {
            return Err(Error::invalid(
                "hierarchy payload shorter than metadata fields",
            ));
        }

        let uncompressed_len = read_u64_be(reader)?;
        let remaining = payload_len
            .checked_sub(8)
            .ok_or_else(|| Error::invalid("hierarchy payload length underflow"))?;
        let remaining_usize = usize::try_from(remaining)
            .map_err(|_| Error::invalid("hierarchy payload exceeds addressable memory"))?;

        let mut payload = vec![0u8; remaining_usize];
        reader.read_exact(&mut payload)?;
        let expected = usize::try_from(uncompressed_len)
            .map_err(|_| Error::invalid("hierarchy size too big"))?;

        let raw = match block_type {
            BlockType::Hierarchy => decode_zlib_maybe(&payload, expected)?,
            BlockType::HierarchyLz4 => decode_lz4(&payload, expected)?,
            BlockType::HierarchyLz4Duo => decode_lz4_duo(&payload, expected)?,
            _ => return Err(Error::invalid("unsupported hierarchy block type")),
        };

        if raw.len() != expected {
            return Err(Error::decode(
                "hierarchy payload length mismatch after decompression",
            ));
        }

        Self::parse_stream(&raw)
    }

    /// Encodes the hierarchy block using the provided compression strategy.
    pub fn encode_block(&self, compression: HierarchyCompression) -> Result<EncodedHierarchy> {
        let raw = self.emit_stream()?;
        let uncompressed_len = raw.len() as u64;

        match compression {
            HierarchyCompression::Raw => {
                let section_length = raw.len() as u64 + 16;
                Ok(EncodedHierarchy {
                    block_type: BlockType::Hierarchy,
                    section_length,
                    uncompressed_len,
                    stage_prefix: Vec::new(),
                    data: raw,
                })
            }
            HierarchyCompression::Zlib { level } => {
                #[cfg(feature = "gzip")]
                {
                    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(level));
                    encoder.write_all(&raw)?;
                    let compressed = encoder.finish()?;
                    if compressed.len() >= raw.len() {
                        let section_length = raw.len() as u64 + 16;
                        return Ok(EncodedHierarchy {
                            block_type: BlockType::Hierarchy,
                            section_length,
                            uncompressed_len,
                            stage_prefix: Vec::new(),
                            data: raw,
                        });
                    }
                    let section_length = compressed.len() as u64 + 16;
                    Ok(EncodedHierarchy {
                        block_type: BlockType::Hierarchy,
                        section_length,
                        uncompressed_len,
                        stage_prefix: Vec::new(),
                        data: compressed,
                    })
                }
                #[cfg(not(feature = "gzip"))]
                {
                    let _ = level;
                    Err(Error::unsupported(
                        "hierarchy zlib compression requires the `gzip` feature",
                    ))
                }
            }
            HierarchyCompression::Lz4 => {
                #[cfg(feature = "lz4")]
                {
                    let compressed = lz4_compress(&raw);
                    let section_length = compressed.len() as u64 + 16;
                    Ok(EncodedHierarchy {
                        block_type: BlockType::HierarchyLz4,
                        section_length,
                        uncompressed_len,
                        stage_prefix: Vec::new(),
                        data: compressed,
                    })
                }
                #[cfg(not(feature = "lz4"))]
                {
                    Err(Error::unsupported(
                        "hierarchy LZ4 compression requires the `lz4` feature",
                    ))
                }
            }
            HierarchyCompression::Lz4Duo => {
                #[cfg(feature = "lz4")]
                {
                    let stage1 = lz4_compress(&raw);
                    let stage2 = lz4_compress(&stage1);
                    let mut stage_prefix = Vec::new();
                    encode_varint(stage1.len() as u64, &mut stage_prefix);
                    let section_length = stage2.len() as u64 + 16 + stage_prefix.len() as u64;
                    Ok(EncodedHierarchy {
                        block_type: BlockType::HierarchyLz4Duo,
                        section_length,
                        uncompressed_len,
                        stage_prefix,
                        data: stage2,
                    })
                }
                #[cfg(not(feature = "lz4"))]
                {
                    Err(Error::unsupported(
                        "hierarchy LZ4 compression requires the `lz4` feature",
                    ))
                }
            }
        }
    }

    fn parse_stream(data: &[u8]) -> Result<Self> {
        let mut offset = 0usize;
        let mut scopes = Vec::new();
        let mut variables = Vec::new();
        let mut attributes = Vec::new();
        let mut items = Vec::new();
        let mut scope_stack: Vec<usize> = Vec::new();
        let mut current_handle: u32 = 0;

        while offset < data.len() {
            let tag = data[offset];
            offset += 1;

            match ScopeType::try_from(tag).ok() {
                Some(ScopeType::VcdScope) => {
                    let scope_type_byte = next_byte(data, &mut offset)?;
                    let scope_type = ScopeType::try_from(scope_type_byte)
                        .map_err(|_| Error::decode("unknown scope type in hierarchy block"))?;
                    let name = read_cstring(data, &mut offset)?;
                    let component = read_cstring(data, &mut offset)?;
                    let parent = scope_stack.last().copied();
                    scopes.push(ScopeEntry {
                        scope_type,
                        name,
                        component: if component.is_empty() {
                            None
                        } else {
                            Some(component)
                        },
                        parent,
                    });
                    let scope_index = scopes.len() - 1;
                    scope_stack.push(scope_index);
                    items.push(HierarchyItem::ScopeBegin { scope_index });
                    continue;
                }
                Some(ScopeType::VcdUpscope) => {
                    scope_stack.pop().ok_or_else(|| {
                        Error::decode(
                            "hierarchy stream attempted to upscope without matching scope",
                        )
                    })?;
                    items.push(HierarchyItem::ScopeEnd);
                    continue;
                }
                Some(ScopeType::GenAttrBegin) => {
                    let attr_type = next_byte(data, &mut offset)?;
                    let subtype = next_byte(data, &mut offset)?;
                    let name = read_cstring(data, &mut offset)?;
                    let argument = decode_varint_slice(data, &mut offset)?;
                    let scope = scope_stack.last().copied();
                    attributes.push(AttributeEntry {
                        attr_type,
                        subtype,
                        name,
                        argument,
                        scope,
                    });
                    let attribute_index = attributes.len() - 1;
                    items.push(HierarchyItem::AttributeBegin { attribute_index });
                    continue;
                }
                Some(ScopeType::GenAttrEnd) => {
                    items.push(HierarchyItem::AttributeEnd);
                    continue;
                }
                _ => {}
            }

            let var_type = VarType::try_from(tag)
                .map_err(|_| Error::decode("unexpected tag in hierarchy block"))?;
            let dir_byte = next_byte(data, &mut offset)?;
            let direction = VarDir::try_from(dir_byte)
                .map_err(|_| Error::decode("unknown variable direction in hierarchy block"))?;
            let name = read_cstring(data, &mut offset)?;
            let len = decode_varint_slice(data, &mut offset)?;
            let alias = decode_varint_slice(data, &mut offset)?;

            let length = if len == 0 { None } else { Some(len as u32) };
            let (handle, alias_of, is_alias) = if alias == 0 {
                current_handle = current_handle
                    .checked_add(1)
                    .ok_or_else(|| Error::decode("hierarchy handle overflow"))?;
                (current_handle, None, false)
            } else {
                let alias_u32 = u32::try_from(alias)
                    .map_err(|_| Error::decode("alias handle exceeds 32-bit range"))?;
                (alias_u32, Some(alias_u32), true)
            };

            let scope = scope_stack.last().copied();
            variables.push(VarEntry {
                var_type,
                direction,
                name,
                length,
                handle,
                alias_of,
                scope,
                is_alias,
            });
            let var_index = variables.len() - 1;
            items.push(HierarchyItem::Var { var_index });
        }

        if !scope_stack.is_empty() {
            return Err(Error::decode(
                "hierarchy stream ended with unterminated scopes",
            ));
        }

        Ok(HierarchyBlock {
            items,
            scopes,
            variables,
            attributes,
        })
    }

    fn emit_stream(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        for item in &self.items {
            match *item {
                HierarchyItem::ScopeBegin { scope_index } => {
                    let scope = self.scopes.get(scope_index).ok_or_else(|| {
                        Error::invalid("hierarchy item references out-of-range scope index")
                    })?;
                    out.push(ScopeType::VcdScope.into());
                    out.push(scope.scope_type.into());
                    write_cstring(&mut out, &scope.name);
                    write_cstring(&mut out, scope.component.as_deref().unwrap_or(""));
                }
                HierarchyItem::ScopeEnd => {
                    out.push(ScopeType::VcdUpscope.into());
                }
                HierarchyItem::AttributeBegin { attribute_index } => {
                    let attr = self.attributes.get(attribute_index).ok_or_else(|| {
                        Error::invalid("hierarchy item references out-of-range attribute index")
                    })?;
                    out.push(ScopeType::GenAttrBegin.into());
                    out.push(attr.attr_type);
                    out.push(attr.subtype);
                    write_cstring(&mut out, &attr.name);
                    encode_varint(attr.argument, &mut out);
                }
                HierarchyItem::AttributeEnd => {
                    out.push(ScopeType::GenAttrEnd.into());
                }
                HierarchyItem::Var { var_index } => {
                    let var = self.variables.get(var_index).ok_or_else(|| {
                        Error::invalid("hierarchy item references out-of-range variable index")
                    })?;
                    out.push(var.var_type.into());
                    out.push(var.direction.into());
                    write_cstring(&mut out, &var.name);
                    encode_varint(var.length.map(u64::from).unwrap_or(0), &mut out);
                    encode_varint(var.alias_of.map(u64::from).unwrap_or(0), &mut out);
                }
            }
        }
        Ok(out)
    }
}

/// Compression strategy for hierarchy blocks.
#[derive(Debug, Clone, Copy)]
pub enum HierarchyCompression {
    Raw,
    Zlib { level: u32 },
    Lz4,
    Lz4Duo,
}

/// Encoded hierarchy payload ready to be written to disk.
#[derive(Debug, Clone)]
pub struct EncodedHierarchy {
    pub block_type: BlockType,
    pub section_length: u64,
    pub uncompressed_len: u64,
    pub stage_prefix: Vec<u8>,
    pub data: Vec<u8>,
}

impl EncodedHierarchy {
    /// Writes the encoded payload (without the leading block tag) to the provided writer.
    pub fn write_to<W: Write + ?Sized>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.section_length.to_be_bytes())?;
        writer.write_all(&self.uncompressed_len.to_be_bytes())?;
        writer.write_all(&self.stage_prefix)?;
        writer.write_all(&self.data)?;
        Ok(())
    }
}

fn decode_zlib_maybe(payload: &[u8], expected: usize) -> Result<Vec<u8>> {
    if payload.len() == expected {
        return Ok(payload.to_vec());
    }

    #[cfg(feature = "gzip")]
    {
        let mut decoder = ZlibDecoder::new(payload);
        let mut decoded = Vec::with_capacity(expected);
        decoder.read_to_end(&mut decoded)?;
        if decoded.len() != expected {
            return Err(Error::decode(
                "hierarchy zlib decoding length mismatch with header",
            ));
        }
        Ok(decoded)
    }
    #[cfg(not(feature = "gzip"))]
    {
        Err(Error::unsupported(
            "hierarchy zlib decoding requires the `gzip` feature",
        ))
    }
}

fn decode_lz4(payload: &[u8], expected: usize) -> Result<Vec<u8>> {
    #[cfg(feature = "lz4")]
    {
        let decoded =
            lz4_decompress(payload, expected).map_err(|e| Error::decode(e.to_string()))?;
        Ok(decoded)
    }
    #[cfg(not(feature = "lz4"))]
    {
        let _ = (payload, expected);
        Err(Error::unsupported(
            "hierarchy LZ4 decoding requires the `lz4` feature",
        ))
    }
}

fn decode_lz4_duo(payload: &[u8], expected: usize) -> Result<Vec<u8>> {
    #[cfg(feature = "lz4")]
    {
        let (stage_len, consumed) = decode_varint_with_len(payload)?;
        let stage_len_usize = usize::try_from(stage_len)
            .map_err(|_| Error::decode("hierarchy duo stage length exceeds usize"))?;
        let stage2 = &payload[consumed..];
        let stage1 =
            lz4_decompress(stage2, stage_len_usize).map_err(|e| Error::decode(e.to_string()))?;
        let decoded =
            lz4_decompress(&stage1, expected).map_err(|e| Error::decode(e.to_string()))?;
        Ok(decoded)
    }
    #[cfg(not(feature = "lz4"))]
    {
        let _ = (payload, expected);
        Err(Error::unsupported(
            "hierarchy LZ4 decoding requires the `lz4` feature",
        ))
    }
}

fn read_cstring(data: &[u8], offset: &mut usize) -> Result<String> {
    let start = *offset;
    let end = data[start..]
        .iter()
        .position(|&b| b == 0)
        .map(|pos| start + pos)
        .ok_or_else(|| Error::decode("unterminated string in hierarchy block"))?;
    let text = String::from_utf8_lossy(&data[start..end]).to_string();
    *offset = end + 1;
    Ok(text)
}

fn decode_varint_slice(data: &[u8], offset: &mut usize) -> Result<u64> {
    let (value, consumed) = decode_varint_with_len(&data[*offset..])?;
    *offset += consumed;
    Ok(value)
}

fn next_byte(data: &[u8], offset: &mut usize) -> Result<u8> {
    if *offset >= data.len() {
        return Err(Error::decode("unexpected end of hierarchy payload"));
    }
    let byte = data[*offset];
    *offset += 1;
    Ok(byte)
}

fn write_cstring(buf: &mut Vec<u8>, text: &str) {
    buf.extend_from_slice(text.as_bytes());
    buf.push(0);
}
