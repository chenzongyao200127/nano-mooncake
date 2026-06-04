use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

pub type SharedBuffer = Arc<RwLock<Vec<u8>>>;

const HEADER_LEN: usize = 17;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpCode {
    Read = 0,
    Write = 1,
}

impl TryFrom<u8> for OpCode {
    type Error = TransferError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Read),
            1 => Ok(Self::Write),
            other => Err(TransferError::InvalidOpcode(other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferStatus {
    Waiting,
    Pending,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentDesc {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferRequest {
    pub opcode: OpCode,
    pub source_offset: usize,
    pub target_id: String,
    pub target_offset: usize,
    pub length: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferTask {
    pub request: TransferRequest,
    pub status: TransferStatus,
}

#[derive(Debug)]
pub enum TransferError {
    Io(io::Error),
    MissingSegment(String),
    UnknownBatch(usize),
    NoLocalMemory,
    OutOfBounds {
        offset: usize,
        length: usize,
        capacity: usize,
    },
    InvalidOpcode(u8),
    IntegerOverflow(u64),
    AckFailed,
}

impl fmt::Display for TransferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::MissingSegment(name) => write!(f, "segment not found: {name}"),
            Self::UnknownBatch(batch_id) => write!(f, "unknown batch: {batch_id}"),
            Self::NoLocalMemory => write!(f, "local memory has not been registered"),
            Self::OutOfBounds {
                offset,
                length,
                capacity,
            } => write!(
                f,
                "range out of bounds: offset={offset}, length={length}, capacity={capacity}",
            ),
            Self::InvalidOpcode(opcode) => write!(f, "invalid opcode: {opcode}"),
            Self::IntegerOverflow(value) => {
                write!(f, "integer does not fit into usize: {value}")
            }
            Self::AckFailed => write!(f, "remote write was not acknowledged"),
        }
    }
}

impl Error for TransferError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for TransferError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug, Clone, Default)]
pub struct MetadataStore {
    segments: Arc<Mutex<HashMap<String, SegmentDesc>>>,
}

impl MetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_segment(&self, desc: SegmentDesc) {
        self.segments
            .lock()
            .expect("metadata lock poisoned")
            .insert(desc.name.clone(), desc);
    }

    pub fn get_segment(&self, name: &str) -> Option<SegmentDesc> {
        self.segments
            .lock()
            .expect("metadata lock poisoned")
            .get(name)
            .cloned()
    }

    pub fn all_segments(&self) -> HashMap<String, SegmentDesc> {
        self.segments
            .lock()
            .expect("metadata lock poisoned")
            .clone()
    }
}

#[derive(Debug)]
struct BatchState {
    tasks: Vec<TransferTask>,
    completed_count: usize,
    has_failure: bool,
}

impl BatchState {
    fn new(batch_size: usize) -> Self {
        Self {
            tasks: Vec::with_capacity(batch_size),
            completed_count: 0,
            has_failure: false,
        }
    }

    fn overall_status(&self) -> TransferStatus {
        if self.completed_count < self.tasks.len() {
            return TransferStatus::Pending;
        }
        if self.has_failure {
            TransferStatus::Failed
        } else {
            TransferStatus::Completed
        }
    }
}

#[derive(Debug)]
struct TcpServer {
    shutdown: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    port: u16,
}

impl TcpServer {
    fn start(buffer: SharedBuffer, port: u16) -> Result<Self, TransferError> {
        let listener = TcpListener::bind(("0.0.0.0", port))?;
        let port = listener.local_addr()?.port();
        listener.set_nonblocking(true)?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);

        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _addr)) => {
                        let buffer = Arc::clone(&buffer);
                        thread::spawn(move || {
                            let _ = handle_connection(stream, buffer);
                        });
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(Self {
            shutdown,
            handle: Some(handle),
            port,
        })
    }

    fn port(&self) -> u16 {
        self.port
    }

    fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for TcpServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct TcpTransport;

impl TcpTransport {
    fn start_server(self, buffer: SharedBuffer, port: u16) -> Result<TcpServer, TransferError> {
        TcpServer::start(buffer, port)
    }

    fn transfer_one(
        self,
        req: TransferRequest,
        local_buffer: SharedBuffer,
        remote: SegmentDesc,
    ) -> Result<(), TransferError> {
        match req.opcode {
            OpCode::Write => {
                let data = {
                    let buffer = local_buffer.read().expect("buffer lock poisoned");
                    let range = checked_range(req.source_offset, req.length, buffer.len())?;
                    buffer[range].to_vec()
                };
                self.write_bytes(&remote, req.target_offset, &data)
            }
            OpCode::Read => {
                let data = self.read_bytes(&remote, req.target_offset, req.length)?;
                let mut buffer = local_buffer.write().expect("buffer lock poisoned");
                let range = checked_range(req.source_offset, req.length, buffer.len())?;
                buffer[range].copy_from_slice(&data);
                Ok(())
            }
        }
    }

    fn write_bytes(
        self,
        remote: &SegmentDesc,
        target_offset: usize,
        data: &[u8],
    ) -> Result<(), TransferError> {
        let mut stream = connect(remote)?;
        let header = encode_header(data.len(), target_offset, OpCode::Write);
        stream.write_all(&header)?;
        stream.write_all(data)?;
        stream.flush()?;

        let mut ack = [0_u8; 1];
        stream.read_exact(&mut ack)?;
        if ack[0] == 1 {
            Ok(())
        } else {
            Err(TransferError::AckFailed)
        }
    }

    fn read_bytes(
        self,
        remote: &SegmentDesc,
        target_offset: usize,
        length: usize,
    ) -> Result<Vec<u8>, TransferError> {
        let mut stream = connect(remote)?;
        let header = encode_header(length, target_offset, OpCode::Read);
        stream.write_all(&header)?;
        stream.flush()?;

        let mut data = vec![0_u8; length];
        stream.read_exact(&mut data)?;
        Ok(data)
    }
}

#[derive(Debug)]
pub struct TransferEngine {
    local_name: String,
    metadata: MetadataStore,
    transport: TcpTransport,
    local_buffer: Option<SharedBuffer>,
    local_desc: Option<SegmentDesc>,
    server: Option<TcpServer>,
    batches: Mutex<HashMap<usize, Arc<Mutex<BatchState>>>>,
    next_batch_id: AtomicUsize,
}

impl TransferEngine {
    pub fn new(local_name: impl Into<String>, metadata: MetadataStore) -> Self {
        Self {
            local_name: local_name.into(),
            metadata,
            transport: TcpTransport,
            local_buffer: None,
            local_desc: None,
            server: None,
            batches: Mutex::new(HashMap::new()),
            next_batch_id: AtomicUsize::new(0),
        }
    }

    pub fn register_local_memory(
        &mut self,
        size: usize,
        host: impl Into<String>,
        port: u16,
    ) -> Result<SharedBuffer, TransferError> {
        let buffer = Arc::new(RwLock::new(vec![0_u8; size]));
        let server = self.transport.start_server(Arc::clone(&buffer), port)?;
        let desc = SegmentDesc {
            name: self.local_name.clone(),
            host: host.into(),
            port: server.port(),
            size,
        };

        self.metadata.register_segment(desc.clone());
        self.local_buffer = Some(Arc::clone(&buffer));
        self.local_desc = Some(desc);
        self.server = Some(server);

        Ok(buffer)
    }

    pub fn local_segment(&self) -> Option<SegmentDesc> {
        self.local_desc.clone()
    }

    pub fn open_segment(&self, name: &str) -> Option<SegmentDesc> {
        self.metadata.get_segment(name)
    }

    pub fn allocate_batch_id(&self, batch_size: usize) -> usize {
        let batch_id = self.next_batch_id.fetch_add(1, Ordering::Relaxed);
        self.batches
            .lock()
            .expect("batch table lock poisoned")
            .insert(batch_id, Arc::new(Mutex::new(BatchState::new(batch_size))));
        batch_id
    }

    pub fn submit_transfer(
        &self,
        batch_id: usize,
        requests: Vec<TransferRequest>,
    ) -> Result<(), TransferError> {
        let batch = self.batch(batch_id)?;
        let local_buffer = self
            .local_buffer
            .as_ref()
            .ok_or(TransferError::NoLocalMemory)?
            .clone();

        let start_idx = {
            let mut state = batch.lock().expect("batch lock poisoned");
            let start_idx = state.tasks.len();
            state.tasks.reserve(requests.len());
            for request in &requests {
                state.tasks.push(TransferTask {
                    request: request.clone(),
                    status: TransferStatus::Pending,
                });
            }
            start_idx
        };

        for (request_offset, request) in requests.into_iter().enumerate() {
            let task_idx = start_idx + request_offset;
            match self.metadata.get_segment(&request.target_id) {
                Some(remote) => {
                    let transport = self.transport;
                    let batch = Arc::clone(&batch);
                    let local_buffer = Arc::clone(&local_buffer);
                    thread::spawn(move || {
                        let success = transport
                            .transfer_one(request, local_buffer, remote)
                            .is_ok();
                        mark_task_done(&batch, task_idx, success);
                    });
                }
                None => mark_task_done(&batch, task_idx, false),
            }
        }

        Ok(())
    }

    pub fn get_transfer_status(&self, batch_id: usize) -> Result<TransferStatus, TransferError> {
        let batch = self.batch(batch_id)?;
        Ok(batch.lock().expect("batch lock poisoned").overall_status())
    }

    pub fn wait_for_completion(
        &self,
        batch_id: usize,
        timeout: Duration,
    ) -> Result<TransferStatus, TransferError> {
        let deadline = Instant::now() + timeout;
        loop {
            let status = self.get_transfer_status(batch_id)?;
            if matches!(status, TransferStatus::Completed | TransferStatus::Failed) {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Ok(TransferStatus::Failed);
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn write_remote_bytes(
        &self,
        target_id: &str,
        target_offset: usize,
        data: &[u8],
    ) -> Result<(), TransferError> {
        let remote = self
            .metadata
            .get_segment(target_id)
            .ok_or_else(|| TransferError::MissingSegment(target_id.to_owned()))?;
        self.transport.write_bytes(&remote, target_offset, data)
    }

    pub fn read_remote_bytes(
        &self,
        target_id: &str,
        target_offset: usize,
        length: usize,
    ) -> Result<Vec<u8>, TransferError> {
        let remote = self
            .metadata
            .get_segment(target_id)
            .ok_or_else(|| TransferError::MissingSegment(target_id.to_owned()))?;
        self.transport.read_bytes(&remote, target_offset, length)
    }

    pub fn shutdown(&mut self) {
        if let Some(server) = &mut self.server {
            server.shutdown();
        }
        self.server = None;
    }

    fn batch(&self, batch_id: usize) -> Result<Arc<Mutex<BatchState>>, TransferError> {
        self.batches
            .lock()
            .expect("batch table lock poisoned")
            .get(&batch_id)
            .cloned()
            .ok_or(TransferError::UnknownBatch(batch_id))
    }
}

fn mark_task_done(batch: &Arc<Mutex<BatchState>>, task_idx: usize, success: bool) {
    let mut state = batch.lock().expect("batch lock poisoned");
    if let Some(task) = state.tasks.get_mut(task_idx) {
        task.status = if success {
            TransferStatus::Completed
        } else {
            TransferStatus::Failed
        };
    }
    if !success {
        state.has_failure = true;
    }
    state.completed_count += 1;
}

fn connect(remote: &SegmentDesc) -> Result<TcpStream, TransferError> {
    let stream = TcpStream::connect((remote.host.as_str(), remote.port))?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

fn handle_connection(mut stream: TcpStream, buffer: SharedBuffer) -> Result<(), TransferError> {
    stream.set_nodelay(true)?;

    loop {
        let mut header = [0_u8; HEADER_LEN];
        match stream.read_exact(&mut header) {
            Ok(()) => {}
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                ) =>
            {
                return Ok(());
            }
            Err(err) => return Err(TransferError::Io(err)),
        }

        let (length, offset, opcode) = decode_header(&header)?;
        match opcode {
            OpCode::Write => {
                let mut data = vec![0_u8; length];
                stream.read_exact(&mut data)?;

                let mut guard = buffer.write().expect("buffer lock poisoned");
                let range = checked_range(offset, length, guard.len())?;
                guard[range].copy_from_slice(&data);

                stream.write_all(&[1])?;
                stream.flush()?;
            }
            OpCode::Read => {
                let data = {
                    let guard = buffer.read().expect("buffer lock poisoned");
                    let range = checked_range(offset, length, guard.len())?;
                    guard[range].to_vec()
                };
                stream.write_all(&data)?;
                stream.flush()?;
            }
        }
    }
}

fn encode_header(length: usize, offset: usize, opcode: OpCode) -> [u8; HEADER_LEN] {
    let mut header = [0_u8; HEADER_LEN];
    header[0..8].copy_from_slice(&(length as u64).to_le_bytes());
    header[8..16].copy_from_slice(&(offset as u64).to_le_bytes());
    header[16] = opcode as u8;
    header
}

fn decode_header(header: &[u8; HEADER_LEN]) -> Result<(usize, usize, OpCode), TransferError> {
    let length = u64::from_le_bytes(
        header[0..8]
            .try_into()
            .expect("header length slice is fixed"),
    );
    let offset = u64::from_le_bytes(
        header[8..16]
            .try_into()
            .expect("header offset slice is fixed"),
    );

    Ok((
        usize::try_from(length).map_err(|_| TransferError::IntegerOverflow(length))?,
        usize::try_from(offset).map_err(|_| TransferError::IntegerOverflow(offset))?,
        OpCode::try_from(header[16])?,
    ))
}

pub(crate) fn checked_range(
    offset: usize,
    length: usize,
    capacity: usize,
) -> Result<Range<usize>, TransferError> {
    let end = offset
        .checked_add(length)
        .ok_or(TransferError::OutOfBounds {
            offset,
            length,
            capacity,
        })?;

    if end > capacity {
        return Err(TransferError::OutOfBounds {
            offset,
            length,
            capacity,
        });
    }

    Ok(offset..end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfers_write_and_read_between_segments() {
        let metadata = MetadataStore::new();
        let mut prefill = TransferEngine::new("prefill", metadata.clone());
        let prefill_buffer = prefill.register_local_memory(4096, "127.0.0.1", 0).unwrap();

        let mut decode = TransferEngine::new("decode", metadata.clone());
        let decode_buffer = decode.register_local_memory(4096, "127.0.0.1", 0).unwrap();

        {
            let mut buffer = prefill_buffer.write().unwrap();
            buffer[0..5].copy_from_slice(b"hello");
        }

        let batch_id = prefill.allocate_batch_id(1);
        prefill
            .submit_transfer(
                batch_id,
                vec![TransferRequest {
                    opcode: OpCode::Write,
                    source_offset: 0,
                    target_id: "decode".to_owned(),
                    target_offset: 100,
                    length: 5,
                }],
            )
            .unwrap();

        assert_eq!(
            prefill
                .wait_for_completion(batch_id, Duration::from_secs(2))
                .unwrap(),
            TransferStatus::Completed,
        );
        assert_eq!(&decode_buffer.read().unwrap()[100..105], b"hello");

        {
            let mut buffer = decode_buffer.write().unwrap();
            buffer[200..205].copy_from_slice(b"world");
        }

        let batch_id = prefill.allocate_batch_id(1);
        prefill
            .submit_transfer(
                batch_id,
                vec![TransferRequest {
                    opcode: OpCode::Read,
                    source_offset: 50,
                    target_id: "decode".to_owned(),
                    target_offset: 200,
                    length: 5,
                }],
            )
            .unwrap();

        assert_eq!(
            prefill
                .wait_for_completion(batch_id, Duration::from_secs(2))
                .unwrap(),
            TransferStatus::Completed,
        );
        assert_eq!(&prefill_buffer.read().unwrap()[50..55], b"world");

        prefill.shutdown();
        decode.shutdown();
    }
}
