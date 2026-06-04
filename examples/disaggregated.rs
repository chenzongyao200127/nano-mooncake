use std::time::{Duration, Instant};

use nano_mooncake::{MetadataStore, OpCode, TransferEngine, TransferRequest, TransferStatus};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("nano-mooncake Rust: Prefill/Decode transfer demo");

    let metadata = MetadataStore::new();

    let mut prefill = TransferEngine::new("prefill-node", metadata.clone());
    let prefill_buffer = prefill.register_local_memory(4 * 1024 * 1024, "127.0.0.1", 0)?;
    let prefill_segment = prefill.local_segment().unwrap();
    println!(
        "  Prefill segment: {}:{}",
        prefill_segment.host, prefill_segment.port,
    );

    let mut decode = TransferEngine::new("decode-node", metadata);
    let decode_buffer = decode.register_local_memory(4 * 1024 * 1024, "127.0.0.1", 0)?;
    let decode_segment = decode.local_segment().unwrap();
    println!(
        "  Decode segment: {}:{}",
        decode_segment.host, decode_segment.port,
    );

    let kv_size = 4 * 2 * 8 * 128 * 64 * 2;
    let kv_cache = make_bytes(kv_size);
    {
        let mut buffer = prefill_buffer.write().unwrap();
        buffer[..kv_cache.len()].copy_from_slice(&kv_cache);
    }
    println!("  Prefill computed and staged {kv_size} bytes");

    let started = Instant::now();
    let batch_id = prefill.allocate_batch_id(1);
    prefill.submit_transfer(
        batch_id,
        vec![TransferRequest {
            opcode: OpCode::Write,
            source_offset: 0,
            target_id: "decode-node".to_owned(),
            target_offset: 0,
            length: kv_cache.len(),
        }],
    )?;

    let status = prefill.wait_for_completion(batch_id, Duration::from_secs(10))?;
    let elapsed = started.elapsed();
    println!("  Transfer status: {status:?}");
    println!("  Transfer time: {:.1} ms", elapsed.as_secs_f64() * 1000.0);
    println!(
        "  Throughput: {:.1} MiB/s",
        kv_cache.len() as f64 / elapsed.as_secs_f64() / 1024.0 / 1024.0,
    );

    assert_eq!(status, TransferStatus::Completed);
    assert_eq!(&decode_buffer.read().unwrap()[..kv_cache.len()], &kv_cache);
    println!("  Decode verified the received KV cache");

    prefill.shutdown();
    decode.shutdown();
    Ok(())
}

fn make_bytes(size: usize) -> Vec<u8> {
    (0..size)
        .map(|idx| ((idx.wrapping_mul(31) + 7) & 0xff) as u8)
        .collect()
}
