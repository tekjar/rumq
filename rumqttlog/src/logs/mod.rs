pub mod acks;
mod connections;
mod data;
mod topics;

use crate::{Config, Data, DataRequest};
use bytes::Bytes;
use std::sync::Arc;

pub use connections::ConnectionsLog;
pub use topics::TopicsLog;

pub struct DataLog {
    commitlog: data::DataLog,
}

impl DataLog {
    pub fn new(config: Arc<Config>) -> DataLog {
        let commitlog = data::DataLog::new(config);
        DataLog { commitlog }
    }

    /// Update matched topic offsets to current offset of this topic's commitlog
    pub fn seek_offsets_to_end(&self, topic: &mut (String, u8, (u64, u64))) {
        self.commitlog.seek_offsets_to_end(topic);
    }

    /// Connections pull logs from both replication and connections where as replicator
    /// only pull logs from connections.
    /// Data from replicator and data from connection are separated for this reason
    pub fn append(&mut self, topic: &str, bytes: Bytes) -> Option<(bool, (u64, u64))> {
        match self.commitlog.append(&topic, bytes) {
            Ok(v) => Some(v),
            Err(e) => {
                error!("Commitlog append failed. Error = {:?}", e);
                None
            }
        }
    }

    pub fn retain(&mut self, topic: &str, bytes: Bytes) -> Option<bool> {
        // id 0-10 are reserved for replications which are linked to other routers in the mesh
        match self.commitlog.retain(&topic, bytes) {
            Ok(v) => Some(v),
            Err(e) => {
                error!("Commitlog append failed. Error = {:?}", e);
                None
            }
        }
    }

    /// Extracts data from native and replicated logs. Returns None in case the
    /// log is caught up or encountered an error while reading data
    pub(crate) fn extract_data(&mut self, request: &DataRequest) -> Option<Data> {
        let topic = &request.topic;
        let mut last_retain = request.last_retain;

        // Iterate through native and replica commitlogs to collect data (of a topic)
        let (segment, offset) = request.cursor;
        match self.commitlog.readv(topic, segment, offset, last_retain) {
            Ok(Some(v)) => {
                let (jump, base_offset, record_offset, retain, data) = v;
                let cursor = match jump {
                    Some(next) => (next, next),
                    None => (base_offset, record_offset),
                };

                // Update retain id (incase readv has retained publish to consider)
                last_retain = retain;
                if data.is_empty() {
                    return None;
                }

                Some(Data::new(
                    request.topic.clone(),
                    request.qos,
                    cursor,
                    last_retain,
                    0,
                    data,
                ))
            }
            Ok(None) => None,
            Err(e) => {
                error!("Failed to extract data from commitlog. Error = {:?}", e);
                None
            }
        }
    }
}
