// A build without Lua given function libraries: it keeps them, and writes
// them back into the images it makes (#732). In a test binary of its own:
// what the server holds is process-global, and other tests write images.
//
// The `kividb-full` package (Lua) and the `kividb` package (no Lua) can be
// swapped for each other. The libraries stored on the `full` build used to be
// dropped by the default build's first snapshot, so going back to `full` lost
// them.

#![cfg(not(feature = "lua"))]

use kividb::snapshot::{load_snapshot_bytes, restore_functions, write_kdb_image};
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

#[test]
fn libraries_a_build_without_lua_loads_are_in_the_images_it_writes() {
    // As it came: a build without Lua does not read the payload, so any
    // bytes do; these are not a valid `FUNCTION DUMP`.
    let payload = b"KVFN not read by a build without Lua".to_vec();

    let before = load_snapshot_bytes(&image()).unwrap();
    assert_eq!(before.functions, None, "nothing loaded, nothing kept");

    restore_functions(Some(payload.clone())).unwrap();
    let kept = load_snapshot_bytes(&image()).unwrap();
    assert_eq!(kept.functions.as_deref(), Some(&payload[..]));
    assert_eq!(kept.dbs[0].len(), 1, "the keys are still there");

    // Loading an image with none leaves none, as on a build with Lua.
    restore_functions(None).unwrap();
    let after = load_snapshot_bytes(&image()).unwrap();
    assert_eq!(after.functions, None);
}
