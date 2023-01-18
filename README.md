# courier
An efficient web request TUI designed for developers and security researchers.
Written in rust.

## Features/Roadmap

MVP release items are in bold.

[ ] **Execute web queries**
  [ ] **HTTP/1.0**
  [ ] HTTP/1.1
  [ ] HTTP/2
  [ ] HTTP/3
  [ ] websockets
  [ ] gRPC
  [ ] GraphQL
  [ ] TCP
  [ ] UDP
  [ ] TLS
  [ ] quic
  [ ] h2c
  [ ] Lower level protocols using something like [libpnet](https://github.com/libpnet/libpnet)
[ ] **Query history**
  [ ] **Copy to plan**
  [ ] **Rerun query**
  [ ] View responses at each protocol boundary
  [ ] Search
  [ ] Persist across sessions
[ ] **API index**
  [ ] **Manual entry**
  [ ] OpenAPI
  [ ] gRPC
  [ ] GraphQL
  [ ] Persistence
[ ] **Query plans**
  [ ] Persistence
  [ ] Variables
  [ ] Functions
  [ ] Execute and view results in history
  [ ] Export to curl
  [ ] Import from curl
  [ ] Concurrent requests
  [ ] Execute individual steps
[ ] **Integrated editor**
  [ ] UTF-8 support
  [ ] View/edit 0-width, whitespace, and invalid codepoints/graphemes
  [ ] Auto-fill (variables, functions, data from index)
  [ ] Search
  [ ] Go-to definition
  [ ] Inline errors
  [ ] Syntax highlighting
  [ ] Hex mode
[ ] **Fully remappable keybinds**
  [ ] **Vi-style modes**
  [ ] Chords
  [ ] Emacs and vi default keymaps
[ ] **Plugins**
  [ ] **Add custom query plan functions (with examples for bash, python, rust, go,
  js)**
  [ ] **Endpoint scanning with rate limit detection to populate index**
  [ ] MITM proxy to populate index for set of domains/IPs
  [ ] Fuzz endpoints in index to generate plans for detected bugs
