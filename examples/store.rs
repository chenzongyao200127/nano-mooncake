use std::time::{Duration, Instant};

use nano_mooncake::{MasterService, MetadataStore, StoreClient};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("nano-mooncake Rust: Store put/get demo");

    let metadata = MetadataStore::new();
    let master = MasterService::new(Duration::from_secs(5));

    let mut client_a = StoreClient::new(
        master.clone(),
        metadata.clone(),
        "node-a",
        "127.0.0.1",
        0,
        1024 * 1024,
    )?;
    let mut client_b = StoreClient::new(
        master.clone(),
        metadata,
        "node-b",
        "127.0.0.1",
        0,
        1024 * 1024,
    )?;

    println!(
        "  Client A segment: {:?}",
        client_a.local_segment().map(|segment| segment.port),
    );
    println!(
        "  Client B segment: {:?}",
        client_b.local_segment().map(|segment| segment.port),
    );

    let key = "req-123:layer-0";
    let kv_data = make_bytes(8 * 128 * 64 * 2);

    let started = Instant::now();
    let put_ok = client_a.put(key, &kv_data)?;
    println!(
        "  Put {key}: {put_ok} ({:.1} ms)",
        started.elapsed().as_secs_f64() * 1000.0,
    );

    let started = Instant::now();
    let loaded = client_b.get(key)?;
    println!(
        "  Get {key}: {} ({:.1} ms)",
        loaded.as_ref() == Some(&kv_data),
        started.elapsed().as_secs_f64() * 1000.0,
    );

    for layer in 0..4 {
        let key = format!("req-456:layer-{layer}");
        let data = make_bytes(8 * 64 * 64 * 2 + layer);
        println!("  Put {key}: {}", client_a.put(&key, &data)?);
    }

    for key in ["req-123:layer-0", "req-456:layer-2", "missing"] {
        match master.query(key) {
            Some(object) => {
                let replica = &object.replicas[0];
                println!(
                    "  Query {key}: {} offset={} status={:?}",
                    replica.segment_name, replica.offset, replica.status,
                );
            }
            None => println!("  Query {key}: not found"),
        }
    }

    client_a.shutdown();
    client_b.shutdown();
    Ok(())
}

fn make_bytes(size: usize) -> Vec<u8> {
    (0..size)
        .map(|idx| ((idx.wrapping_mul(17) + 3) & 0xff) as u8)
        .collect()
}
