//! Helper utilities for exporting FST metadata and value changes through `serde`.

use serde::Serialize;

use crate::block::{AttributeEntry, HierarchyBlock, ScopeEntry, VarEntry};
use crate::error::Result;
use crate::reader::{ValueChange, VcBlockChanges};
use crate::types::SignalValue;
use crate::types::{ScopeType, VarDir, VarType};

/// Hierarchy snapshot containing a tree of scopes and any variables/attributes attached to the root.
#[derive(Debug, Clone, Serialize)]
pub struct HierarchySnapshot {
    /// Roots of the scope tree reconstructed from the hierarchy payload.
    pub scopes: Vec<ScopeNode>,
    /// Variables that appear at the hierarchy root (no parent scope).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub root_variables: Vec<VariableNode>,
    /// Attributes that are not bound to any specific scope.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub root_attributes: Vec<AttributeNode>,
}

/// Recursive scope representation matching the structure described in `doc/wavefst.md Section 6`.
#[derive(Debug, Clone, Serialize)]
pub struct ScopeNode {
    /// Scope classification (module, task, package, etc.).
    pub scope_type: ScopeType,
    /// Scope identifier as written by the originating tool.
    pub name: String,
    /// Optional component/suffix string supplied by the producer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
    /// Variables declared directly inside this scope.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub variables: Vec<VariableNode>,
    /// Attributes attached to the scope header.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attributes: Vec<AttributeNode>,
    /// Child scopes nested underneath this scope.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<ScopeNode>,
}

/// Serializable view of a hierarchy attribute.
#[derive(Debug, Clone, Serialize)]
pub struct AttributeNode {
    /// Attribute type tag (matches the wire-format enumeration).
    pub attr_type: u8,
    /// Attribute subtype (interpretation depends on the type).
    pub subtype: u8,
    /// Attribute identifier.
    pub name: String,
    /// Raw argument value stored alongside the attribute.
    pub argument: u64,
}

/// Serializable representation of a variable entry.
#[derive(Debug, Clone, Serialize)]
pub struct VariableNode {
    /// Variable kind (wire, reg, real, string, etc.).
    pub var_type: VarType,
    /// Declared direction (input, output, implicit, ...).
    pub direction: VarDir,
    /// Variable name.
    pub name: String,
    /// Optional bit-width when the signal has a fixed geometry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length: Option<u32>,
    /// Canonical handle assigned by the writer.
    pub handle: u32,
    /// Canonical handle that this variable aliases, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias_of: Option<u32>,
    /// True if the entry was emitted as an alias rather than a primary declaration.
    pub is_alias: bool,
}

/// Fully owned representation of a value change event suitable for JSON export.
#[derive(Debug, Clone, Serialize)]
pub struct OwnedValueChange {
    /// Absolute simulation timestamp (already adjusted for `time_zero`).
    pub timestamp: u64,
    /// Handle of the signal that produced the change.
    pub handle: u32,
    /// Canonical handle when this change represents an alias update.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias_of: Option<u32>,
    /// Fully owned value payload.
    pub value: OwnedSignalValue,
}

/// Fully owned variant of [`SignalValue`] tailored for serialization.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", content = "data")]
pub enum OwnedSignalValue {
    /// Single-bit value (`'0'`, `'1'`, `'x'`, `'z'`, etc.).
    Bit(char),
    /// ASCII vector string (LSB-right).
    Vector(String),
    /// Packed bit representation with explicit width and raw bytes (MSB-first per byte).
    PackedBits {
        /// Number of logical bits stored in the packed buffer.
        width: u32,
        /// Raw packed bytes (MSB-first within each byte).
        bits: Vec<u8>,
    },
    /// IEEE-754 double-precision number.
    Real(f64),
    /// Raw byte payload used for variable-length signals.
    Bytes(Vec<u8>),
}

impl OwnedSignalValue {
    fn from_signal(value: &SignalValue<'_>) -> Self {
        match value {
            SignalValue::Bit(ch) => Self::Bit(*ch),
            SignalValue::Vector(text) => Self::Vector(text.to_string()),
            SignalValue::PackedBits { width, bits } => Self::PackedBits {
                width: *width,
                bits: bits.to_vec(),
            },
            SignalValue::Real(real) => Self::Real(*real),
            SignalValue::Bytes(bytes) => Self::Bytes(bytes.to_vec()),
        }
    }
}

impl OwnedValueChange {
    fn from_change(change: &ValueChange<'_>) -> Self {
        Self {
            timestamp: change.timestamp,
            handle: change.handle,
            alias_of: change.alias_of,
            value: OwnedSignalValue::from_signal(&change.value),
        }
    }
}

/// Collects *all* value changes from the iterator, returning an owned representation that can be
/// serialized with `serde_json` or similar serializers.
pub fn collect_value_changes<'a>(
    changes: &mut VcBlockChanges<'a>,
) -> Result<Vec<OwnedValueChange>> {
    let mut out = Vec::new();
    for event in changes.by_ref() {
        let event = event?;
        out.push(OwnedValueChange::from_change(&event));
    }
    Ok(out)
}

/// Builds a serializable hierarchy tree from the decoded block representation.
pub fn snapshot_hierarchy(hierarchy: &HierarchyBlock) -> HierarchySnapshot {
    let mut builders: Vec<ScopeBuilder> = hierarchy
        .scopes
        .iter()
        .map(ScopeBuilder::from_entry)
        .collect();

    let mut child_links: Vec<Vec<usize>> = vec![Vec::new(); builders.len()];
    let mut roots: Vec<usize> = Vec::new();

    for (idx, scope) in hierarchy.scopes.iter().enumerate() {
        if let Some(parent) = scope.parent {
            child_links[parent].push(idx);
        } else {
            roots.push(idx);
        }
    }

    let mut root_variables = Vec::new();
    for var in &hierarchy.variables {
        let node = VariableNode::from_entry(var);
        match var.scope {
            Some(scope_idx) => builders[scope_idx].variables.push(node),
            None => root_variables.push(node),
        }
    }

    let mut root_attributes = Vec::new();
    for attr in &hierarchy.attributes {
        let node = AttributeNode::from_entry(attr);
        match attr.scope {
            Some(scope_idx) => builders[scope_idx].attributes.push(node),
            None => root_attributes.push(node),
        }
    }

    let scopes = roots
        .into_iter()
        .map(|idx| flush_scope(idx, &builders, &child_links))
        .collect();

    HierarchySnapshot {
        scopes,
        root_variables,
        root_attributes,
    }
}

#[derive(Debug, Clone)]
struct ScopeBuilder {
    scope_type: ScopeType,
    name: String,
    component: Option<String>,
    variables: Vec<VariableNode>,
    attributes: Vec<AttributeNode>,
}

impl ScopeBuilder {
    fn from_entry(entry: &ScopeEntry) -> Self {
        Self {
            scope_type: entry.scope_type,
            name: entry.name.clone(),
            component: entry.component.clone(),
            variables: Vec::new(),
            attributes: Vec::new(),
        }
    }
}

impl VariableNode {
    fn from_entry(entry: &VarEntry) -> Self {
        Self {
            var_type: entry.var_type,
            direction: entry.direction,
            name: entry.name.clone(),
            length: entry.length,
            handle: entry.handle,
            alias_of: entry.alias_of,
            is_alias: entry.is_alias,
        }
    }
}

impl AttributeNode {
    fn from_entry(entry: &AttributeEntry) -> Self {
        Self {
            attr_type: entry.attr_type,
            subtype: entry.subtype,
            name: entry.name.clone(),
            argument: entry.argument,
        }
    }
}

fn flush_scope(index: usize, builders: &[ScopeBuilder], child_links: &[Vec<usize>]) -> ScopeNode {
    let builder = &builders[index];
    let children = child_links[index]
        .iter()
        .map(|child| flush_scope(*child, builders, child_links))
        .collect();

    ScopeNode {
        scope_type: builder.scope_type,
        name: builder.name.clone(),
        component: builder.component.clone(),
        variables: builder.variables.clone(),
        attributes: builder.attributes.clone(),
        children,
    }
}
