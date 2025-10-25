//! Structured representations of the different FST block types.

mod blackout;
mod geom;
mod header;
mod hier;
mod time;
mod vc;

pub use blackout::{BlackoutBlock, BlackoutEvent};
pub use geom::{GeomEntry, GeomInfo};
pub use header::{DATE_FIELD_LEN, Header, VERSION_FIELD_LEN};
pub use hier::{
    AttributeEntry, HierarchyBlock, HierarchyCompression, HierarchyItem, ScopeEntry, VarEntry,
};
pub use time::TimeSection;
pub use vc::{
    ChainIndexEntry, FrameEncoding, FrameSection, PackMarker, TimeEncoding, TimeTable, VcBlock,
    encode_chain_index, encode_chain_payload, encode_frame_section, encode_time_section,
};
