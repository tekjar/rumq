use std::collections::HashMap;
use std::io;

use crate::Config;
use bytes::Bytes;
use segments::MemoryLog;
use std::sync::Arc;

pub(crate) struct DataLog {
    config: Arc<Config>,
    logs: HashMap<String, Data>,
}

struct Data {
    retained: Option<(u64, Bytes)>,
    log: MemoryLog<Bytes>,
}

impl DataLog {
    pub fn new(config: Arc<Config>) -> DataLog {
        DataLog {
            config,
            logs: HashMap::new(),
        }
    }

    /// Appends the record to correct commitlog and returns a boolean to indicate
    /// if this topic is new along with the offset of append
    pub fn append(&mut self, topic: &str, record: Bytes) -> io::Result<(bool, (u64, u64))> {
        // Entry instead of if/else?
        if let Some(data) = self.logs.get_mut(topic) {
            let offsets = data.log.append(record.len(), record);
            Ok((false, offsets))
        } else {
            let max_segment_size = self.config.max_segment_size;
            let max_segment_count = self.config.max_segment_count;
            let mut data = Data {
                retained: None,
                log: MemoryLog::new(max_segment_size, max_segment_count),
            };
            let offsets = data.log.append(record.len(), record);
            self.logs.insert(topic.to_owned(), data);
            Ok((true, offsets))
        }
    }

    /// Updates retain record to and returns a boolean to indicate
    /// if this topic is new along with the offset of append
    pub fn retain(&mut self, topic: &str, record: Bytes) -> io::Result<bool> {
        if let Some(log) = self.logs.get_mut(topic) {
            if record.is_empty() {
                log.retained = None;
            } else {
                let retained = log.retained.get_or_insert((0, record.clone()));
                retained.0 += 1;
                retained.1 = record;
            }

            Ok(false)
        } else {
            let max_segment_size = self.config.max_segment_size;
            let max_segment_count = self.config.max_segment_count;
            let mut data = Data {
                retained: None,
                log: MemoryLog::new(max_segment_size, max_segment_count),
            };

            if record.is_empty() {
                data.retained = None;
            } else {
                let retained = data.retained.get_or_insert((0, record.clone()));
                retained.0 += 1;
                retained.1 = record;
            }

            self.logs.insert(topic.to_owned(), data);
            Ok(true)
        }
    }

    fn next_offset(&self, topic: &str) -> Option<(u64, u64)> {
        let data = match self.logs.get(topic) {
            Some(log) => log,
            None => return None,
        };

        Some(data.log.next_offset())
    }

    pub fn seek_offsets_to_end(&self, topic: &mut (String, u8, (u64, u64))) {
        if let Some(last_offset) = self.next_offset(&topic.0) {
            topic.2 = last_offset;
        }
    }

    pub fn readv(
        &mut self,
        topic: &str,
        in_segment: u64,
        in_offset: u64,
        last_retain: u64,
    ) -> io::Result<Option<(Option<u64>, u64, u64, u64, Vec<Bytes>)>> {
        // Router during data request and notifications will check both
        // native and replica commitlog where this topic doesn't exist
        let data = match self.logs.get_mut(topic) {
            Some(log) => log,
            None => return Ok(None),
        };

        let (jump, segment, offset, mut out) = data.log.readv(in_segment, in_offset);

        let mut last_retain = last_retain;
        if let Some((id, publish)) = &mut data.retained {
            if *id != last_retain {
                out.push(publish.clone());
                last_retain = *id;
            }
        }

        // For debugging. Will be removed later
        // println!(
        //     "In: segment {} offset {}, Out: segment {} offset {}, Count {}",
        //     in_segment,
        //     in_offset,
        //     segment,
        //     offset,
        //     data.len()
        // );
        Ok(Some((jump, segment, offset, last_retain, out)))
    }
}
