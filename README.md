# devil
An unassuming web request runner for developers and security researchers.
Cross-protocol, written in Rust.

devil uses TOML to define "plans" which detail the various requests to run.
Here's a simple plan that gets a file over https and sends it somewhere else
over raw udp:

```toml
[pull.http]
url = "https://example.com/some/data"

[push.udp]
host = "another-domain"
port = 10000
body.cel = "steps.pull.response.body"
```

## Features

See the [examples](/examples) dir for more details.

### Common Expression Language (cel)

[Google's cel language](https://github.com/google/cel-spec) can be used to
dynamically calculate any value in a step.

```toml
Values from previous steps
Flow control
```

### Cross-protocol Configuration

Steps can include configuration for transport protocols used in a given
request.

```toml
Control packet splits
```

### Module Imports

Any plan file can be run from another plan as a module.

```toml
```

### Global or Stack-specific Defaults.

Plans can set defaults which are applied to each step where the value is
unspecified.

```toml
```

### Timing Data and Manipulation

Steps return rich timing data that can be analyzed with cel for dynamic load
testing or timing vulnerability detection.

```toml
```

## Installation

A. Download from the releases page and extract it anywhere you want.
or
B. run `cargo install devil`.

## Usage

Download and run a plan over https.
```sh
devil https://github.com/fore5fire/devil/blob/main/examples/http.dv.toml
```

Or run a plan locally.
```sh
devil example-file.dv.toml
```

## Project Priorities
In order of importance:
1. Be unsuprising. Unexpected behavior hides bugs, and a primary purpose of
   devil is to help find bugs.
2. Be flexible. Implementations don't always match the spec, so flexibility is
   more important than compliance.
3. Be inclusive. This means including lots of useful protocols and features as
   well as being easy to use with different people's backgrounds and workflows.
4. Minimize breaking changes to plans. devil makes it easy to build up a
   library of plans, and breaking changes are more painful the bigger your
   library is. Before the project gets too mature, I'll commit to providing a
   command that automatically updates plans for any breaking changes that can't
   be avoided.
5. Be fast. Time matters a lot for load testing, race conditions, and side
   channel attacks. Although devil doesn't seek to be the best performing
   network client overall, anything that doesn't need to live in the hot-path
   of running a request should be done elsewhere. devil is not a lightweight
   client and will gladly use lots of extra memory to save a few microseconds
   in the hot-path. 

## Non-goals

### Plugins
No plugin support is planned for this project. Plugins require executing local
binaries, which is a very dangerous feature if users are running plans from the
internet.

Instead, devil aims to be easily composable with other tools. If you'd like to
extend the functionality of devil, you can write a tool that imports it as a
rust library, build a server and provide importable step definitions for
various features, or simply write a wrapper that execs the cli binary.

That said, plugging devil functionality into other tools is great! Plugins for
popular development and security research tools like VS Code, Vim, Burpsuite,
and Caido are on the roadmap. If you're interested in building plugins for
other tools, open an issue to discuss any functionality devil could add to
support you.

## Roadmap

Items in each section are roughly ordered by my personal priority, if you want
it sooner then please contribute! And if you don't see something on the roadmap
that you'd find useful, open a pull request to add it here.

### Query Engine
- Improve pause resolution with async-spin-sleep crate.
- Parallel Execution
    - Control connection reuse and multiplexing
    - Coordinated pauses (see [HTTP/2 single packet attack](https://portswigger.net/research/smashing-the-state-machine#single-packet-attack))
- Assertions
    - Fuzzing
- Better error messaging
- HTTP auto-serialized bodies (protobuf, zstd, gzip, etc.)
- Non-UTF8 payloads (UTF16/32, GB 18030, etc.)

### Protocols
- IP (using [libpnet](https://github.com/libpnet/libpnet))
- HTTP multipart uploads
- Websockets
- graphql-transport-ws
- graphql-ws
- HTTP/2
- gRPC
- S3
- UDP
- QUIC
- HTTP/3
- h2c
- DNS
- DoT
- DoH
- DoQ
- SFTP
- FTP
- SSH
- SMB
  
### Integrations with other tools
- LSP (maybe based on [Taplo](https://taplo.tamasfe.dev/)?)
- VS Code plugin
- Vim/NeoVim plugin
- Burpsuite plugin
- Caido plugin (once supported)
- Python bindings for devil as a library
- c bindings for devil as a library
