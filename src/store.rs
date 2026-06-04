use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::transfer_engine::{
    MetadataStore, SegmentDesc, SharedBuffer, TransferEngine, TransferError, checked_range,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaStatus {
    Initialized,
    Processing,
    Complete,
    Removed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replica {
    pub segment_name: String,
    pub offset: usize,
    pub size: usize,
    pub status: ReplicaStatus,
}

#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: usize,
    pub replicas: Vec<Replica>,
    pub lease_deadline: Option<Instant>,
    pub created_by: String,
}

#[derive(Debug, Clone)]
struct BumpAllocator {
    capacity: usize,
    offset: usize,
}

impl BumpAllocator {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            offset: 0,
        }
    }

    fn allocate(&mut self, size: usize) -> Option<usize> {
        let end = self.offset.checked_add(size)?;
        if end > self.capacity {
            return None;
        }
        let offset = self.offset;
        self.offset = end;
        Some(offset)
    }
}

#[derive(Debug, Default)]
struct MasterState {
    objects: HashMap<String, ObjectMetadata>,
    allocators: HashMap<String, BumpAllocator>,
    segment_order: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct MasterService {
    state: Arc<Mutex<MasterState>>,
    lease_ttl: Duration,
}

impl MasterService {
    pub fn new(default_lease: Duration) -> Self {
        Self {
            state: Arc::new(Mutex::new(MasterState::default())),
            lease_ttl: default_lease,
        }
    }

    pub fn register_segment(&self, desc: SegmentDesc) {
        let mut state = self.state.lock().expect("master lock poisoned");
        if !state.allocators.contains_key(&desc.name) {
            state.segment_order.push(desc.name.clone());
        }
        state
            .allocators
            .insert(desc.name, BumpAllocator::new(desc.size));
    }

    pub fn put_start(&self, client_name: &str, key: &str, size: usize) -> Option<Replica> {
        let mut state = self.state.lock().expect("master lock poisoned");

        for segment_name in state.segment_order.clone() {
            let Some(allocator) = state.allocators.get_mut(&segment_name) else {
                continue;
            };
            let Some(offset) = allocator.allocate(size) else {
                continue;
            };

            let replica = Replica {
                segment_name,
                offset,
                size,
                status: ReplicaStatus::Processing,
            };

            state
                .objects
                .entry(key.to_owned())
                .or_insert_with(|| ObjectMetadata {
                    key: key.to_owned(),
                    size,
                    replicas: Vec::new(),
                    lease_deadline: None,
                    created_by: client_name.to_owned(),
                })
                .replicas
                .push(replica.clone());

            return Some(replica);
        }

        None
    }

    pub fn put_end(&self, key: &str) -> bool {
        let mut state = self.state.lock().expect("master lock poisoned");
        let Some(object) = state.objects.get_mut(key) else {
            return false;
        };

        for replica in &mut object.replicas {
            if replica.status == ReplicaStatus::Processing {
                replica.status = ReplicaStatus::Complete;
            }
        }
        object.lease_deadline = Some(Instant::now() + self.lease_ttl);
        true
    }

    pub fn query(&self, key: &str) -> Option<ObjectMetadata> {
        let mut state = self.state.lock().expect("master lock poisoned");
        let expired = state
            .objects
            .get(key)
            .and_then(|object| object.lease_deadline)
            .is_some_and(|deadline| Instant::now() >= deadline);

        if expired {
            state.objects.remove(key);
            return None;
        }

        state.objects.get(key).cloned()
    }

    pub fn remove(&self, key: &str) -> bool {
        self.state
            .lock()
            .expect("master lock poisoned")
            .objects
            .remove(key)
            .is_some()
    }

    pub fn run_eviction(&self) {
        let mut state = self.state.lock().expect("master lock poisoned");
        let now = Instant::now();
        let expired: Vec<String> = state
            .objects
            .iter()
            .filter(|(_key, object)| {
                object
                    .lease_deadline
                    .is_some_and(|deadline| now >= deadline)
            })
            .map(|(key, _object)| key.clone())
            .collect();

        for key in expired {
            state.objects.remove(&key);
        }
    }
}

pub struct StoreClient {
    master: MasterService,
    local_name: String,
    engine: TransferEngine,
    buffer: SharedBuffer,
}

impl StoreClient {
    pub fn new(
        master: MasterService,
        metadata: MetadataStore,
        local_name: impl Into<String>,
        host: impl Into<String>,
        port: u16,
        segment_size: usize,
    ) -> Result<Self, TransferError> {
        let local_name = local_name.into();
        let host = host.into();

        let mut engine = TransferEngine::new(local_name.clone(), metadata);
        let buffer = engine.register_local_memory(segment_size, host, port)?;
        let desc = engine.local_segment().expect("segment was just registered");
        master.register_segment(desc);

        Ok(Self {
            master,
            local_name,
            engine,
            buffer,
        })
    }

    pub fn put(&self, key: &str, data: &[u8]) -> Result<bool, TransferError> {
        let Some(replica) = self.master.put_start(&self.local_name, key, data.len()) else {
            return Ok(false);
        };

        if replica.segment_name == self.local_name {
            let mut buffer = self.buffer.write().expect("buffer lock poisoned");
            let range = checked_range(replica.offset, data.len(), buffer.len())?;
            buffer[range].copy_from_slice(data);
        } else {
            self.engine
                .write_remote_bytes(&replica.segment_name, replica.offset, data)?;
        }

        Ok(self.master.put_end(key))
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, TransferError> {
        let Some(object) = self.master.query(key) else {
            return Ok(None);
        };
        let Some(replica) = object
            .replicas
            .iter()
            .find(|replica| replica.status == ReplicaStatus::Complete)
        else {
            return Ok(None);
        };

        if replica.segment_name == self.local_name {
            let buffer = self.buffer.read().expect("buffer lock poisoned");
            let range = checked_range(replica.offset, replica.size, buffer.len())?;
            return Ok(Some(buffer[range].to_vec()));
        }

        self.engine
            .read_remote_bytes(&replica.segment_name, replica.offset, replica.size)
            .map(Some)
    }

    pub fn local_segment(&self) -> Option<SegmentDesc> {
        self.engine.local_segment()
    }

    pub fn buffer(&self) -> SharedBuffer {
        Arc::clone(&self.buffer)
    }

    pub fn shutdown(&mut self) {
        self.engine.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_put_get_and_lease_expiry() {
        let metadata = MetadataStore::new();
        let master = MasterService::new(Duration::from_millis(30));

        let mut writer = StoreClient::new(
            master.clone(),
            metadata.clone(),
            "writer",
            "127.0.0.1",
            0,
            4096,
        )
        .unwrap();
        let mut reader =
            StoreClient::new(master.clone(), metadata, "reader", "127.0.0.1", 0, 4096).unwrap();

        assert!(writer.put("req-1:layer-0", b"kv-cache").unwrap());
        assert_eq!(
            reader.get("req-1:layer-0").unwrap(),
            Some(b"kv-cache".to_vec()),
        );

        std::thread::sleep(Duration::from_millis(50));
        master.run_eviction();
        assert_eq!(reader.get("req-1:layer-0").unwrap(), None);

        writer.shutdown();
        reader.shutdown();
    }
}
