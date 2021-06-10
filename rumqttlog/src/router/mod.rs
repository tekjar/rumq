extern crate bytes;

pub(crate) mod connection;
mod metrics;
mod readyqueue;
mod router;
mod slab;
mod tracker;

use connection::Connection;
pub use router::Router;
pub use tracker::Tracker;

use self::bytes::Bytes;
pub use crate::router::metrics::{ConnectionMetrics, MetricsReply, MetricsRequest};
use mqttbytes::v4::Packet;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Messages from connection to router
#[derive(Debug)]
pub enum Event {
    /// Client id and connection handle
    Connect(Connection),
    /// Connection ready to receive more data
    Ready,
    /// Data for native commitlog
    Data(Vec<Packet>),
    /// Disconnection request
    Disconnect(Disconnection),
    /// Get metrics of a connection or all connections
    Metrics(MetricsRequest),
}

/// Requests for pull operations
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Data request
    Data(DataRequest),
    /// Topics request
    Topics(TopicsRequest),
    /// Acks request
    Acks(AcksRequest),
}

/// Notification from router to connection
#[derive(Debug)]
pub enum Notification {
    /// Connection reply
    ConnectionAck(ConnectionAck),
    /// Individual publish
    Message(Message),
    /// Data reply
    Data(Data),
    /// Watermarks reply
    Acks(Vec<Packet>),
    /// Connection paused by router
    Pause,
    /// All metrics
    Metrics(MetricsReply),
}

/// Request that connection/linker makes to extract data from commitlog
/// NOTE Connection can make one sweep request to get data from multiple topics
/// but we'll keep it simple for now as multiple requests in one message can
/// makes constant extraction size harder
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct DataRequest {
    /// Log to sweep
    pub(crate) topic: String,
    /// QoS of the request
    pub(crate) qos: u8,
    /// (segment, offset) tuples per replica (1 native and 2 replicas)
    pub(crate) cursor: (u64, u64),
    /// Last retain id
    pub(crate) last_retain: u64,
    /// Maximum count of payload buffer per replica
    max_count: usize,
}

impl DataRequest {
    /// New data request with offsets starting from 0
    pub fn new(topic: String, qos: u8) -> DataRequest {
        DataRequest {
            topic,
            qos,
            cursor: (0, 0),
            last_retain: 0,
            max_count: 100,
        }
    }

    /// New data request with provided offsets
    pub fn offsets(topic: String, qos: u8, cursor: (u64, u64), last_retain: u64) -> DataRequest {
        DataRequest {
            topic,
            qos,
            cursor,
            last_retain,
            max_count: 100,
        }
    }
}

impl fmt::Debug for DataRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Topic = {}, cursors = {:?}, max_count = {}",
            self.topic, self.cursor, self.max_count
        )
    }
}

pub struct Message {
    /// Log to sweep
    pub topic: String,
    /// Qos of the topic
    pub qos: u8,
    /// Reply data chain
    pub payload: Bytes,
}

impl Message {
    pub fn new(topic: String, qos: u8, payload: Bytes) -> Message {
        Message {
            topic,
            payload,
            qos,
        }
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Topic = {:?}, Payload size = {}",
            self.topic,
            self.payload.len()
        )
    }
}

pub struct Data {
    /// Log to sweep
    pub topic: String,
    /// Qos of the topic
    pub qos: u8,
    /// (segment, offset) tuples per replica (1 native and 2 replicas)
    pub cursor: (u64, u64),
    /// Next retain publish id
    pub last_retain: u64,
    /// Payload size
    pub size: usize,
    /// Reply data chain
    pub payload: Vec<Bytes>,
}

impl Data {
    pub fn new(
        topic: String,
        qos: u8,
        cursor: (u64, u64),
        last_retain: u64,
        size: usize,
        payload: Vec<Bytes>,
    ) -> Data {
        Data {
            topic,
            cursor,
            last_retain,
            size,
            payload,
            qos,
        }
    }
}

impl fmt::Debug for Data {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Topic = {:?}, Cursors = {:?}, Payload size = {}, Payload count = {}",
            self.topic,
            self.cursor,
            self.size,
            self.payload.len()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
            count: 10,
        }
    }

    pub fn set_count(&mut self, count: usize) {
        self.count = count;
    }

    pub fn offset(offset: usize) -> TopicsRequest {
        TopicsRequest { offset, count: 10 }
    }
}

pub struct Topics<'a> {
    offset: usize,
    topics: &'a [String],
}

impl<'a> Topics<'a> {
    pub fn new(offset: usize, topics: &'a [String]) -> Topics {
        Topics { offset, topics }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AcksRequest;

impl AcksRequest {
    pub fn new() -> AcksRequest {
        AcksRequest
    }
}

#[derive(Debug)]
pub enum ConnectionAck {
    /// Id assigned by the router for this connection and
    /// previous session status
    Success((usize, bool, Vec<Notification>)),
    /// Failure and reason for failure string
    Failure(String),
}

#[derive(Debug)]
pub struct Disconnection {
    id: String,
    execute_will: bool,
    pending: Vec<Notification>,
}

impl Disconnection {
    pub fn new(id: String, execute_will: bool, pending: Vec<Notification>) -> Disconnection {
        Disconnection {
            id,
            execute_will,
            pending,
        }
    }
}
