# Proxyables (Rust)

A high-performance, peer-to-peer RPC library that makes remote objects feel local. Built on top of **Yamux** multiplexing, Rust's trait system, and procedural macros, it enables seamless bi-directional interaction between processes with type-safe proxy generation and distributed garbage collection.

## Features

- **Peer-to-Peer Architecture**: No strict client/server distinction — both sides can import and export objects, enabling true bi-directional communication.
- **Type-Safe Proxies**: The `#[proxy]` macro generates client-side proxy structs from trait definitions, providing compile-time type safety for remote calls.
- **`#[proxyable]` Macro**: Automatically implements the `ProxyTarget` trait for structs, converting methods into remotely callable operations.
- **Distributed Garbage Collection**: Automatically manages remote object lifecycles using reference counting and release instructions.
- **Async-First**: Built on `tokio` and `futures` for fully asynchronous operation.
- **Custom Yamux Implementation**: Includes a purpose-built Yamux multiplexing layer for efficient stream management.

## Installation

Add to your `Cargo.toml`:
```toml
[dependencies]
proxyables = { git = "https://github.com/stateforward/proxyables.rs" }
```

## Usage

### Basic Example

**Server (Exporting an object):**
```rust
use proxyables::{proxyable, Proxyable};

#[proxyable]
struct API;

impl API {
    fn echo(&self, msg: String) -> String {
        format!("echo {msg}")
    }

    fn compute(&self, a: i64, b: i64) -> i64 {
        a + b
    }
}

// stream is any AsyncRead + AsyncWrite
Proxyable::Export(stream, Arc::new(API)).await;
```

**Client (Importing the object):**
```rust
use proxyables::{proxy, Proxyable};

#[proxy]
trait Api {
    async fn echo(&self, msg: String) -> String;
    async fn compute(&self, a: i64, b: i64) -> i64;
}

let (imported, driver) = Proxyable::ImportFrom(stream);
tokio::spawn(driver);

let proxy = ApiProxy::new(imported);
let result = proxy.echo("hello".into()).await; // "echo hello"
let result = proxy.compute(10, 20).await;      // 30
```

## Architecture

1. **Proxy Layer**: `#[proxy]` generates typed proxy structs; `#[proxyable]` implements `ProxyTarget` for server-side dispatch.
2. **Instruction Protocol**: Operations (get, apply, etc.) are serialized into `ProxyInstruction` messages using MessagePack (`rmpv`).
3. **Transport**: Uses a custom Yamux implementation to multiplex concurrent operations over a single async connection.
4. **Reference Management**: A `Registry` tracks exported objects with reference counting and bidirectional ID lookup.

## License

MIT
