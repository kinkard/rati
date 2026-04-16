# About

Rati (Range-Accessed Tar Index) is a lightweight HTTP server that serves individual files from tar archives via byte-range requests. It works with both remote (S3) and local archives. Point it at a tarball, and it loads the embedded index, resolves keys to byte offsets, and streams contents to clients. Named after the auger Odin used to bore through a mountain to reach the mead of poetry locked within.

## Build & Run

```sh
cargo run --release
```

## License

All code in this project is dual-licensed under either:

- [MIT license](https://opensource.org/licenses/MIT) ([`LICENSE-MIT`](LICENSE-MIT))
- [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) ([`LICENSE-APACHE`](LICENSE-APACHE))

at your option.
