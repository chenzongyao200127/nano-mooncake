# nano-mooncake

Rust-native minimal Mooncake core for understanding KVCache-centric disaggregated inference.

This repository now keeps only the clean Rust implementation of the core runtime:
Transfer Engine, segment metadata, TCP data movement, Mooncake Store allocation,
two-phase put/get, and lease eviction.

## Layout

| Path | Purpose |
|------|---------|
| `Cargo.toml` | Rust crate definition |
| `src/transfer_engine.rs` | Segment registry, TCP READ/WRITE protocol, batch transfer tracking |
| `src/store.rs` | MasterService, StoreClient, bump allocation, replica state, lease eviction |
| `examples/disaggregated.rs` | Prefill to Decode KVCache transfer demo |
| `examples/store.rs` | Store `put`/`get` demo |

## Run

```bash
cargo test
cargo run --example disaggregated
cargo run --example store
```

For stricter local validation:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

## Transfer Engine

The Transfer Engine exposes named memory segments. A node registers a local
buffer, the metadata store records its segment descriptor, and other nodes read
or write it over TCP.

```rust
use std::time::Duration;

use nano_mooncake::{
    MetadataStore, OpCode, TransferEngine, TransferRequest,
};

let metadata = MetadataStore::new();
let mut engine = TransferEngine::new("node-1", metadata.clone());
let buffer = engine.register_local_memory(4 * 1024 * 1024, "127.0.0.1", 0)?;

let batch_id = engine.allocate_batch_id(1);
engine.submit_transfer(batch_id, vec![TransferRequest {
    opcode: OpCode::Write,
    source_offset: 0,
    target_id: "node-2".to_owned(),
    target_offset: 0,
    length: 1024,
}])?;

let status = engine.wait_for_completion(batch_id, Duration::from_secs(10))?;
```

TCP wire format is intentionally tiny:

```text
[8B length | 8B remote_offset | 1B opcode] + payload
```

`opcode = 0` reads from the remote segment into the local buffer.
`opcode = 1` writes from the local buffer into the remote segment.

## Store

The Store layer builds a small KVCache-oriented object store on top of the
Transfer Engine.

```rust
use std::time::Duration;

use nano_mooncake::{MasterService, MetadataStore, StoreClient};

let metadata = MetadataStore::new();
let master = MasterService::new(Duration::from_secs(300));

let client = StoreClient::new(
    master.clone(),
    metadata,
    "node-1",
    "127.0.0.1",
    0,
    1024 * 1024,
)?;

client.put("req-123:layer-0", b"kv-cache")?;
let data = client.get("req-123:layer-0")?;
```

Core Store concepts:

- Two-phase put: `put_start` allocates a replica, data moves through the
  Transfer Engine, and `put_end` commits it.
- Replica states: `Initialized`, `Processing`, `Complete`, `Removed`.
- Lease eviction: committed objects expire after the configured TTL.
- Allocation: a simple first-fit bump allocator per segment.

## Mooncake Mapping

| nano-mooncake | Mooncake concept | Simplification |
|---|---|---|
| `TransferEngine` | `mooncake-transfer-engine` | TCP only; no RDMA/NVLink/CXL |
| `MetadataStore` | `TransferMetadata` | In-process registry |
| `MasterService` | Store master | Single lock, one replica, bump allocation |
| `StoreClient` | Store client | Memory-only replicas |
| `examples/disaggregated.rs` | Prefill/Decode KV transfer | CPU memory demo |
| `examples/store.rs` | Mooncake Store put/get | Single-process demo |

## Scope

This is a teaching prototype, not a production transport. It intentionally
omits RDMA, GPU memory, multi-NIC routing, disk tiers, replication, sharded
metadata locks, high availability, and vLLM connector glue.
