#![cfg(feature = "serde")]

use std::fs::File;
use std::path::PathBuf;

use anyhow::Result;
use wavefst::{
    HierarchyBlock, HierarchySnapshot, ReaderBuilder, collect_value_changes, snapshot_hierarchy,
};

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data/hdl-example.fst")
}

#[test]
fn hierarchy_snapshot_contains_scopes() {
    let mut block = HierarchyBlock::default();
    block.scopes.push(wavefst::ScopeEntry {
        scope_type: wavefst::ScopeType::VcdModule,
        name: "top".into(),
        component: None,
        parent: None,
    });
    block.scopes.push(wavefst::ScopeEntry {
        scope_type: wavefst::ScopeType::VcdModule,
        name: "child".into(),
        component: Some("inst".into()),
        parent: Some(0),
    });

    block.variables.push(wavefst::VarEntry {
        var_type: wavefst::VarType::VcdWire,
        direction: wavefst::VarDir::Implicit,
        name: "sig".into(),
        length: Some(1),
        handle: 1,
        alias_of: None,
        scope: Some(1),
        is_alias: false,
    });

    let snapshot: HierarchySnapshot = snapshot_hierarchy(&block);
    assert_eq!(snapshot.scopes.len(), 1);
    let root = &snapshot.scopes[0];
    assert_eq!(root.name, "top");
    assert_eq!(root.children.len(), 1);
    assert_eq!(root.children[0].name, "child");
    assert_eq!(root.children[0].variables.len(), 1);
}

#[cfg_attr(not(feature = "gzip"), ignore = "requires gzip feature")]
#[test]
fn collect_value_changes_produces_owned_values() -> Result<()> {
    let path = fixture_path();
    let file = File::open(path)?;
    let mut reader = ReaderBuilder::new(file).build()?;

    while let Some(mut changes) = reader.next_value_changes()? {
        let owned = collect_value_changes(&mut changes)?;
        // The fixture only contains frame data, so ensure we still round-trip without panics.
        assert!(owned.iter().all(|event| event.handle > 0));
    }

    Ok(())
}
