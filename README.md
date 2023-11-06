# courier
A cross-abstraction web request runner for developers and security
researchers. Written in Rust.

## Features & Roadmap

MVP release items are in bold.

### Query Language
- [ ] **Variables and References**
- [ ] **Conditionals**
- [ ] **Global Defaults**
- [ ] **Modules**
- [ ] **Parallel Execution**
- [ ] Assertions
- [ ] **HTTP/1.0**
- [X] **HTTP/1.1**
- [ ] Websockets
- [ ] gRPC
- [ ] GraphQL
- [ ] HTTP/2
- [ ] HTTP/3
- [ ] TCP
- [ ] UDP
- [ ] TLS
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
