use std::borrow::Cow;
use std::fs::File;

use wavefst::{FstWriter, GeomEntry, Header, ScopeType, SignalValue, VarDir, VarType};

fn main() -> wavefst::Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "wavefst-example.fst".to_owned());
    let wrapped = std::env::args().any(|arg| arg == "--wrapped");
    let mut writer = FstWriter::builder(File::create(path)?)
        .wrap_with_zlib(wrapped)
        .build()?;
    writer.begin_scope(ScopeType::VcdModule, "top", None)?;
    let clock = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "clock",
        GeomEntry::Fixed(1),
    )?;
    writer.add_alias(VarType::VcdWire, VarDir::Implicit, "clock_alias", clock)?;
    let clock_copy = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "clock_copy",
        GeomEntry::Fixed(1),
    )?;
    let bus = writer.add_variable(
        VarType::VcdWire,
        VarDir::Implicit,
        "bus",
        GeomEntry::Fixed(8),
    )?;
    let real = writer.add_variable(
        VarType::VcdReal,
        VarDir::Implicit,
        "analog",
        GeomEntry::Real,
    )?;
    writer.end_scope()?;

    writer.write_header(Header {
        version: "wavefst example".to_owned(),
        ..Header::default()
    })?;
    writer.emit_change(0, clock, SignalValue::Bit('0'))?;
    writer.emit_change(0, clock_copy, SignalValue::Bit('0'))?;
    writer.emit_change(0, bus, SignalValue::Vector(Cow::Borrowed("00000000")))?;
    writer.emit_change(0, real, SignalValue::Real(0.0))?;
    writer.emit_change(5, clock, SignalValue::Bit('1'))?;
    writer.emit_change(5, clock_copy, SignalValue::Bit('1'))?;
    writer.emit_dump_active(7, false)?;
    writer.emit_dump_active(9, true)?;
    writer.emit_change(10, clock, SignalValue::Bit('0'))?;
    writer.emit_change(10, clock_copy, SignalValue::Bit('0'))?;
    writer.emit_change(10, bus, SignalValue::Vector(Cow::Borrowed("10100101")))?;
    writer.emit_change(10, real, SignalValue::Real(3.125))?;
    writer.finish()?;
    Ok(())
}
