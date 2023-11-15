# courier
A cross-abstraction web request runner for developers and security
researchers. Written in Rust.

## Features & Roadmap

MVP release items are in bold.

### Query Language
- [X] **Variables and References**
- [ ] **Conditionals**
- [X] **Global Defaults**
- [ ] **Modules**
- [ ] **Parallel Execution**
- [X] **Timing Information**
- [ ] **Assertions**
    - [ ] **Fuzzing**
- [X] **HTTP/1.1**
- [ ] **gRPC**
- [X] **GraphQL**
- [X] **TLS**
- [X] **TCP**
- [ ] Websockets
- [ ] HTTP/1.0
- [ ] HTTP/2
- [ ] HTTP/3
- [ ] UDP
- [ ] quic
- [ ] h2c
- [ ] HTTP auto-serialized bodies (protobuf, zstd, gzip, etc.)
- [ ] Lower level protocols using something like [libpnet](https://github.com/libpnet/libpnet)
- [ ] Non-UTF8 payloads (UTF16/32, GB 18030, etc.)
  
### Editor Support
- [ ] LSP
- [ ] Syntax highlighting
  - [ ] vim
  - [ ] VS Code
