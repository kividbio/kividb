//! Compiles `csrc/lua51_str_format.c`, a patched copy of Lua 5.4.7's
//! `str_format` that is `string.format` in scripts (#771): the integer
//! conversions take any number, as Lua 5.1's did. It is compiled against
//! the include dir `mlua-sys` built the vendored Lua 5.4.7 with — the
//! same headers, the same platform defines — and linked into this crate.
//! A build without the `lua` feature compiles nothing.

fn main() {
    if std::env::var_os("CARGO_FEATURE_LUA").is_none() {
        return;
    }

    // `mlua-sys` is a direct dependency, so its `links = "lua"` metadata
    // arrives here: DEP_LUA_INCLUDE is the include dir of the vendored
    // Lua it compiled the library from.
    let include = std::env::var("DEP_LUA_INCLUDE")
        .expect("mlua-sys did not pass its vendored-Lua include dir");

    // The platform define `lua-src` compiled the library with, so
    // `luaconf.h` reads the same way here.
    let target = std::env::var("TARGET").unwrap();
    let platform = match &*target {
        t if t.contains("linux") || t.ends_with("bsd") => "LUA_USE_LINUX",
        t if t.contains("apple-darwin") => "LUA_USE_MACOSX",
        t if t.contains("apple-") || t.ends_with("emscripten") => "LUA_USE_POSIX",
        t if t.contains("windows") => "LUA_USE_WINDOWS",
        t => panic!("don't know how to build Lua for {t}"),
    };

    cc::Build::new()
        .file("csrc/lua51_str_format.c")
        .include(include)
        .define(platform, None)
        .define("LUA_COMPAT_5_3", None)
        .warnings(false)
        .compile("lua51_str_format");

    // cc's `rustc-link-lib` is recorded on the library's rlib, and this
    // crate's binary does not use the rlib — src/main.rs is a crate root
    // of its own, which compiles the same modules again — so the archive
    // must reach bins, tests and benches as a raw linker argument too.
    println!("cargo:rustc-link-arg=-llua51_str_format");

    // cc emits rerun-if-env-changed lines; once a build script emits any
    // rerun-if, the package's files are watched no more — ask for the C
    // file back.
    println!("cargo:rerun-if-changed=csrc/lua51_str_format.c");
    println!("cargo:rerun-if-changed=build.rs");
}
