mod acl;
mod aggregate_pipeline;
pub(crate) mod arity;
mod array;
pub(crate) mod backup;
mod bitmap;
pub mod blocking;
mod cluster;
mod command_docs;
mod command_docs_table;
mod connection;
pub mod ctx;
mod debug_cmd;
pub mod expr;
mod generics;
mod geo;
mod glob_redis;
pub mod handler;
mod hash;
pub(crate) mod hash_import;
pub mod helpers;
pub(crate) mod hotkeys;
mod hybrid;
pub(crate) mod hyperloglog;
mod json;
pub(crate) mod json_fpha;
pub(crate) mod json_path;
mod key_spec_table;
pub(crate) mod key_specs;
pub mod latency;
mod list_move;
mod lists;
#[cfg(feature = "lua")]
mod lua;
#[cfg(feature = "lua")]
pub(crate) use lua::{
    cached_script_declares_no_writes, cached_script_declares_writes, function_declares_no_writes,
    library_snapshot, lua_command_runs_long_sync_vm, restore_library_snapshot,
    script_declares_no_writes, script_declares_writes, take_last_vm_run_us,
};
#[cfg(not(feature = "lua"))]
mod lua {
    use crate::commands::ctx::Ctx;

    pub fn handle(_ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
        match name {
            "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" | "SCRIPT" => {
                Some(crate::resp::write::resp_error(
                    "ERR This KiviDB instance was not compiled with Lua scripting support. \
                     Rebuild with --features lua to enable it.",
                ))
            }
            _ => None,
        }
    }
}
// `pub(crate)` only so the replication full-resync path can reuse
// `purge_allocator` after it swaps the old dataset out; the MEMORY subcommand
// handlers themselves stay internal.
pub(crate) mod memory;
mod pubsub;
pub(crate) mod search;
pub(crate) mod search_tools;
pub mod server;
mod set_card;
mod sets;
pub mod slowlog;
// `pub(crate)` only so `replication::script_feed` can share XADD's option scan
// when rewriting an auto-generated ID; the handlers themselves stay internal.
pub(crate) mod stream_extra;
pub(crate) mod streams;
mod strftime;
mod strings;
pub mod transaction;
mod ts;
mod ts_query;
mod vector;
mod vset;
mod vset_filter;
mod zset;

pub use handler::execute_command;
