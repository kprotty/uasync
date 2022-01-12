uAsync
[![Crates.io](https://img.shields.io/crates/v/uasync.svg)](https://crates.io/crates/uasync)
[![Documentation](https://docs.rs/uasync/badge.svg)](https://docs.rs/uasync/)
====

A fast, `forbid(unsafe_code)`, no dependency, async executor.

This crate was made primarily to see if such a thing was pheasable. The executor performs surprisingly well against others and I went through the effort of optimizing it and providing a drop-in API with tokio. Benchmarks are provided but results on my machine are explicitly **not** provided because people always end up drawing weird conclusions from them that are annoying to try and dispute. `uasync` for now should be treated as mostly an experiment. As per the MIT license, I'm not liable for whatever you do with it. I will most likely accept PRs for documentation, cleanup, and general improvements.

```toml
[dependencies]
uasync = "0.1.1"