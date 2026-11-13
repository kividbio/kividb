// A build without Lua given function libraries: it keeps them, and writes
// them back into the images it makes (#732). In a test binary of its own:
// what the server holds is process-global, and other tests write images.
//
// The `kividb-full` package (Lua) and the `kividb` package (no Lua) can be
// swapped for each other. The libraries stored on the `full` build used to be
// dropped by the default build's first snapshot, so going back to `full` lost
// them.

#![cfg(not(feature = "lua"))]

use kividb::snapshot::{
    flush_kept_functions, holds_kept_functions, load_snapshot_bytes, restore_functions,
    write_kdb_image,
};
use kividb::types::{KiviDBValue, SharedBytes};
use std::collections::HashMap;

fn image() -> Vec<u8> {
    let mut db0: HashMap<Vec<u8>, KiviDBValue> = HashMap::new();
    db0.insert(
        b"k".to_vec(),
        KiviDBValue::String(SharedBytes::from(&b"v"[..])),
    );
    let mut out = Vec::new();
    write_kdb_image(&mut out, &[db0], None).unwrap();
    out
}

/// A `FUNCTION DUMP` payload of KiviDB's own shape: `KVFN`, the version,
/// one library (name, engine, code, one function), and an 8-byte checksum,
/// which a build without Lua does not check.
fn payload() -> Vec<u8> {
    fn blob(out: &mut Vec<u8>, b: &[u8]) {
        out.extend_from_slice(&(b.len() as u32).to_le_bytes());
        out.extend_from_slice(b);
    }
    let mut out = b"KVFN".to_vec();
    out.extend_from_slice(&1u32.to_le_bytes());
    out.extend_from_slice(&1u32.to_le_bytes());
    blob(&mut out, b"swaplib");
    blob(&mut out, b"LUA");
    blob(
        &mut out,
        b"#!lua name=swaplib\nredis.register_function('f', function() return 1 end)",
    );
    out.extend_from_slice(&1u32.to_le_bytes());
    blob(&mut out, b"f");
    out.extend_from_slice(&[0u8; 8]);
    out
}

// One test: the steps share the server's process-global state.
#[test]
fn libraries_a_build_without_lua_loads_are_in_the_images_it_writes() {
    let before = load_snapshot_bytes(&image()).unwrap();
    assert_eq!(before.functions, None, "nothing loaded, nothing kept");

    restore_functions(Some(payload())).unwrap();
    assert!(holds_kept_functions());
    let kept = load_snapshot_bytes(&image()).unwrap();
    assert_eq!(kept.functions.as_deref(), Some(&payload()[..]));
    assert_eq!(kept.dbs[0].len(), 1, "the keys are still there");

    // Loading an image with none leaves none, as on a build with Lua.
    restore_functions(None).unwrap();
    assert_eq!(load_snapshot_bytes(&image()).unwrap().functions, None);

    // FUNCTION FLUSH drops them.
    restore_functions(Some(payload())).unwrap();
    flush_kept_functions();
    assert!(!holds_kept_functions());
    assert_eq!(load_snapshot_bytes(&image()).unwrap().functions, None);

    // A payload whose libraries cannot be read is not kept: a build with
    // Lua would refuse to start on it.
    restore_functions(Some(b"KVFN not a payload".to_vec())).unwrap();
    assert!(!holds_kept_functions());
    assert_eq!(load_snapshot_bytes(&image()).unwrap().functions, None);
}
