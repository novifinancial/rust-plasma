# Rust Plasma
This repository contains Rust crates for working with Arrow Plasma, an in-memory object store which enables efficient memory sharing across processes on the same machine.

## Repository structure
This repository is organized into crates like so:

| Crate                          | Description |
| ------------------------------ | ----------- |
| [plasma-store](plasma-store)   | Rust bindings to C++ implementation of Arrow Plasma object store. |
| [plasma-stream](plasma-stream) | High-performance transport of datasets between Plasma stores on different machines. |

License
-------

This project is [MIT licensed](./LICENSE).