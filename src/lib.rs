//! Rust-native nano-mooncake core.
//!
//! This crate keeps the prototype intentionally small:
//! - `transfer_engine` exposes named memory segments and TCP READ/WRITE.
//! - `store` builds a lease-backed object store on top of the transfer engine.

pub mod store;
pub mod transfer_engine;

pub use store::{MasterService, ObjectMetadata, Replica, ReplicaStatus, StoreClient};
pub use transfer_engine::{
    MetadataStore, OpCode, SegmentDesc, SharedBuffer, TransferEngine, TransferError,
    TransferRequest, TransferStatus,
};
