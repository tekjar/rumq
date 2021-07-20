use crate::router::TopicsRequest;
use crate::DataRequest;
use std::collections::{HashMap, VecDeque};

type Topic = String;
type ConnectionId = usize;

/// Waiter on a topic. These are used to wake connections/replicators
/// which are caught up previously and waiting on new data.
pub struct DataWaiters {
    // Map[topic]List[Connections Ids]
    waiters: HashMap<Topic, Waiters<DataRequest>>,
}

impl DataWaiters {
    pub fn new() -> DataWaiters {
        DataWaiters {
            waiters: HashMap::new(),
        }
    }

    pub fn get_mut(&mut self, topic: &str) -> Option<&mut Waiters<DataRequest>> {
        self.waiters.get_mut(topic)
    }

    /// Register data waiter
    pub fn register(&mut self, id: ConnectionId, request: DataRequest) {
        let topic = request.topic.clone();
        let waiters = self.waiters.entry(topic).or_default();
        waiters.register(id, request);
    }

    /// Remove a connection from waiters
    pub fn remove(&mut self, id: ConnectionId) -> VecDeque<DataRequest> {
        let mut pending = VecDeque::new();
        for waiters in self.waiters.values_mut() {
            if let Some(request) = waiters.remove(id) {
                pending.push_back(request)
            }
        }

        pending
    }
}

pub type TopicsWaiters = Waiters<TopicsRequest>;

/// TopicsWaiters are connections which are waiting to be notified
/// of new topics. Notifications can sometimes be false positives for topics
/// a connection (e.g replicator connection and new topic due to replicator
/// data). This connection should be polled again for next notification.
/// Having 2 waiters to prevents infinite waiter loops while trying to
/// add connection back to waiter queue
#[derive(Debug)]
pub struct Waiters<T> {
    /// Waiters on new topics
    current: VecDeque<(ConnectionId, T)>,
    /// Waiters for next iteration
    next: VecDeque<(ConnectionId, T)>,
}

impl<T> Waiters<T> {
    pub fn new() -> Waiters<T> {
        Waiters {
            current: VecDeque::with_capacity(1000),
            next: VecDeque::with_capacity(1000),
        }
    }

    /// Pushes a request to current wait queue
    pub fn register(&mut self, id: ConnectionId, request: T) {
        let request = (id, request);
        self.current.push_back(request);
    }

    /// Pops a request from current wait queue
    pub fn pop_front(&mut self) -> Option<(ConnectionId, T)> {
        self.current.pop_front()
    }

    /// Pushes a request to next wait queue
    pub fn push_back(&mut self, id: ConnectionId, request: T) {
        let request = (id, request);
        self.next.push_back(request);
    }

    /// Swaps next wait queue with current wait queue
    pub fn prepare_next(&mut self) {
        std::mem::swap(&mut self.current, &mut self.next);
    }

    /// Remove a connection from waiters
    pub fn remove(&mut self, id: ConnectionId) -> Option<T> {
        match self.current.iter().position(|x| x.0 == id) {
            Some(index) => self.current.swap_remove_back(index).map(|v| v.1),
            None => None,
        }
    }
}

impl<T> Default for Waiters<T> {
    fn default() -> Self {
        Self::new()
    }
}
