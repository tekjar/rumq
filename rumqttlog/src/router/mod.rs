extern crate bytes;

mod subscriptions;
mod commitlog;
mod watermarks;
mod router;

pub use router::Router;
use subscriptions::Subscription;

use self::bytes::Bytes;
use rumqttc::{Publish, Incoming, Request};
use std::fmt;
use async_channel::{Sender, Receiver, bounded};

/// Messages going into router
#[derive(Debug)]
pub enum RouterInMessage {
    /// Client id and connection handle
    Connect(Connection),
    /// Data for native commitlog
    Publish(Publish),
    /// Data for native commitlog
    Data(Vec<Incoming>),
    /// Data for commitlog of a replica
    ReplicationData(Vec<ReplicationData>),
    /// Replication acks
    ReplicationAcks(Vec<ReplicationAck>),
    /// Data request
    DataRequest(DataRequest),
    /// Topics request
    TopicsRequest(TopicsRequest),
    /// Acks request
    AcksRequest(AcksRequest),
    /// Disconnection request
    Disconnect(Disconnection)
}

/// Messages coming from router
#[derive(Debug)]
pub enum RouterOutMessage {
    /// Connection reply
    ConnectionAck(ConnectionAck),
    /// Data reply
    DataReply(DataReply),
    /// Topics reply
    TopicsReply(TopicsReply),
    /// Watermarks reply
    AcksReply(AcksReply),
}

/// Data sent router to be written to commitlog
#[derive(Debug)]
pub struct ReplicationData {
    /// Id of the packet that connection received
    pkid: u16,
    /// Topic of publish
    topic: String,
    /// Publish payload
    payload: Vec<Bytes>,
}

impl ReplicationData {
    pub fn with_capacity(pkid: u16, topic: String, cap: usize) -> ReplicationData {
        ReplicationData {
            pkid,
            topic,
            payload: Vec::with_capacity(cap)
        }
    }

    pub fn push(&mut self, payload: Bytes) {
        self.payload.push(payload)
    }
}

/// Replication connection frames this after receiving an ack from other replica
/// This is used by router to update watermarks, which is used to send acks
/// for replicated data
#[derive(Debug)]
pub struct ReplicationAck {
    topic: String,
    /// Packet id that router assigned
    offset: u64,
}

impl ReplicationAck {
    pub fn new(topic: String, offset: u64) -> ReplicationAck {
        ReplicationAck {
            topic,
            offset
        }
    }
}

/// Request that connection/linker makes to extract data from commitlog
/// NOTE Connection can make one sweep request to get data from multiple topics
/// but we'll keep it simple for now as multiple requests in one message can
/// makes constant extraction size harder
#[derive(Clone)]
pub struct DataRequest {
    /// Log to sweep
    topic: String,
    /// (segment, offset) tuples per replica (1 native and 2 replicas)
    cursors: [(u64, u64); 3],
    /// Maximum count of payload buffer per replica
    max_count: usize
}

impl DataRequest {
    /// New data request with offsets starting from 0
    pub fn new(topic: String) -> DataRequest {
        DataRequest {
            topic,
            cursors: [(0, 0); 3],
            max_count: 100
        }
    }

    pub fn with(topic: String, max_count: usize) -> DataRequest {
        DataRequest {
            topic,
            cursors: [(0, 0); 3],
            max_count
        }
    }

    /// New data request with provided offsets
    pub fn offsets(topic: String, cursors: [(u64, u64); 3]) -> DataRequest {
        DataRequest {
            topic,
            cursors,
            max_count: 100
        }
    }

    /// New data request with provided offsets
    pub fn offsets_with(
        topic: String,
        cursors: [(u64, u64); 3],
        max_count: usize
    ) -> DataRequest {
        DataRequest {
            topic,
            cursors,
            max_count
        }
    }
}

impl fmt::Debug for DataRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Topic = {}, cursors = {:?}, max_count = {}", self.topic, self.cursors, self.max_count)
    }
}

pub struct DataReply {
    /// Log to sweep
    pub topic: String,
    /// (segment, offset) tuples per replica (1 native and 2 replicas)
    pub cursors: [(u64, u64); 3],
    /// Payload size
    pub size: usize,
    /// Reply data chain
    pub payload: Vec<Bytes>,
}

impl DataReply {
    pub fn new(topic: String, cursors: [(u64, u64); 3], size:usize, payload: Vec<Bytes>) -> DataReply {
        DataReply {
            topic,
            cursors,
            size,
            payload
        }
    }
}

impl fmt::Debug for DataReply {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Topic = {:?}, Cursors = {:?}, Payload size = {}, Payload count = {}", self.topic, self.cursors, self.size, self.payload.len())
    }
}

#[derive(Debug, Clone)]
pub struct TopicsRequest {
    /// Start from this offset
    offset: usize,
    /// Maximum number of topics to read
    count: usize,
}

impl TopicsRequest {
    pub fn new() -> TopicsRequest {
        TopicsRequest {
            offset: 0,
            count: 10
        }
    }

    pub fn offset(offset: usize) -> TopicsRequest {
        TopicsRequest {
            offset,
            count: 10
        }
    }
}

#[derive(Debug)]
pub struct TopicsReply {
    /// Last topic offset
    pub offset: usize,
    /// list of new topics
    pub topics: Vec<String>,
}

impl TopicsReply {
    fn new(offset: usize, topics: Vec<String>) -> TopicsReply {
        TopicsReply {
            offset,
            topics
        }
    }
}

#[derive(Clone, Debug)]
pub struct AcksRequest;

impl AcksRequest {
    pub fn new() -> AcksRequest {
        AcksRequest
    }
}

#[derive(Debug)]
pub struct AcksReply {
    /// packet ids that can be acked
    pub acks: Vec<(u16, Request)>,
}

impl AcksReply {
    pub fn new(acks: Vec<(u16, Request)>) -> AcksReply {
        AcksReply {
            acks
        }
    }
}

#[derive(Debug, Clone)]
pub enum ConnectionType {
    Device(String),
    Replicator(usize),
}

/// Used to register a new connection with the router
/// Connection messages encompasses a handle for router to
/// communicate with this connection
#[derive(Clone)]
pub struct Connection {
    /// Kind of connection. A replicator connection or a device connection
    /// Replicator connection are only created from inside this library.
    /// All the external connections are of 'device' type
    pub(crate) conn: ConnectionType,
    /// Handle which is given to router to allow router to comminicate with
    /// this connection
    pub handle: Sender<RouterOutMessage>,
}

impl Connection {
    pub fn new(id: &str, capacity: usize) -> (Connection, Receiver<RouterOutMessage>) {
        let (this_tx, this_rx) = bounded(capacity);

        let connection = Connection {
            conn: ConnectionType::Device(id.to_owned()),
            handle: this_tx,
        };

        (connection , this_rx)
    }
}

#[derive(Debug)]
pub enum ConnectionAck {
    /// Id assigned by the router for this connectiobackn
    Success(usize),
    /// Failure and reason for failure string
    Failure(String),
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.conn)
    }
}

#[derive(Debug)]
pub struct Disconnection {
    id: String,
}

impl Disconnection {
    pub fn new(id: String) -> Disconnection {
        Disconnection {
            id
        }
    }
}