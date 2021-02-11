#[cfg(feature = "docs-rs")]
fn main() {}

#[cfg(not(feature = "docs-rs"))]
fn main() {
    use std::env;

    let mut cxx = cxx_build::bridge("src/ffi/mod.rs");

    let out_dir = env::var("OUT_DIR").unwrap();
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    cxx.files(&[
        "arrow/cpp/src/arrow/util/future.cc",
        "arrow/cpp/src/arrow/util/string.cc",
        "arrow/cpp/src/arrow/util/logging.cc",
        "arrow/cpp/src/arrow/util/thread_pool.cc",
        "arrow/cpp/src/arrow/util/string_builder.cc",
        "arrow/cpp/src/arrow/device.cc",
        "arrow/cpp/src/arrow/result.cc",
        "arrow/cpp/src/arrow/buffer.cc",
        "arrow/cpp/src/arrow/status.cc",
        "arrow/cpp/src/plasma/common.cc",
        "arrow/cpp/src/plasma/fling.cc",
        "arrow/cpp/src/plasma/io.cc",
        "arrow/cpp/src/arrow/io/memory.cc",
        "arrow/cpp/src/arrow/io/interfaces.cc",
        "arrow/cpp/src/arrow/util/memory.cc",
        "arrow/cpp/src/arrow/util/io_util.cc",
        "arrow/cpp/src/arrow/memory_pool.cc",
        "arrow/cpp/src/plasma/malloc.cc",
        "arrow/cpp/src/plasma/protocol.cc",
        "arrow/cpp/src/plasma/plasma.cc",
        "arrow/cpp/src/plasma/client.cc",
    ])
    .flag_if_supported("-std=c++14")
    .include("arrow/cpp/src/")
    .include("arrow/cpp/thirdparty/flatbuffers/include")
    .opt_level(3)
    // TODO: CUDA support
    .flag_if_supported("-fwrapv")
    .flag_if_supported("-fomit-frame-pointer")
    .flag_if_supported("-funroll-loops")
    // ignore some warnings
    .flag_if_supported("-Wno-redundant-move")
    .flag_if_supported("-Wno-unused-function")
    .flag_if_supported("-Wno-unused-parameter")
    .flag_if_supported("-Wno-unused-variable")
    .file("src/ffi/ffi.cc")
    .include(manifest_dir)
    .include(out_dir)
    .compile("plasma");

    println!("cargo:rerun-if-changed=src/ffi/mod.rs");
    println!("cargo:rerun-if-changed=src/ffi/ffi.h");
    println!("cargo:rerun-if-changed=src/ffi/ffi.cc");
}
