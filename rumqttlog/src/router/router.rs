use std::sync::Arc;

use jackiechan::{bounded, Receiver, RecvError, Sender, TryRecvError};
use mqttbytes::v4::{Packet, Publish, Subscribe, SubscribeReasonCode, Unsubscribe};
use thiserror::Error;

use super::connection::ConnectionType;
use super::readyqueue::ReadyQueue;
use super::slab::Slab;
use super::*;
use crate::logs::acks::Acks;

use crate::logs::{ConnectionsLog, DataLog, TopicsLog};
use crate::router::metrics::RouterMetrics;
use crate::waiters::{DataWaiters, TopicsWaiters};
use crate::{Config, ConnectionId, DataRequest, Disconnection, RouterId};

#[derive(Error, Debug)]
#[error("...")]
pub enum RouterError {
    Recv(#[from] RecvError),
    Disconnected,
}

pub struct Router {
    /// Router configuration
    config: Arc<Config>,
    /// Id of this router. Used to index native commitlog to store data from
    /// local connections
    _id: RouterId,
    /// Connections log to handle persitent session and synchronize connection
    /// information between nodes
    connectionslog: ConnectionsLog,
    /// Data logs grouped by replica
    datalog: DataLog,
    /// Topic log
    topicslog: TopicsLog,
    /// Pre-allocated list of connections
    connections: Slab<Connection>,
    /// Subscriptions and matching topics maintained per connection
    trackers: Slab<Tracker>,
    /// Watermarks of a connection
    watermarks: Slab<Acks>,
    /// Connections with more pending requests and ready to make progress
    readyqueue: ReadyQueue,
    /// Waiter on a topic. These are used to wake connections/replicators
    /// which are caught up all the data on a topic. Map[topic]List[Connections Ids]
    data_waiters: DataWaiters,
    /// Waiters on new topics
    topics_waiters: TopicsWaiters,
    /// Channel receiver to receive data from all the active connections and
    /// replicators. Each connection will have a tx handle which they use
    /// to send data and requests to router
    router_rx: Receiver<(ConnectionId, Event)>,
    /// Aggregates for all connections
    metrics: RouterMetrics,
}

impl Router {
    pub fn new(config: Arc<Config>) -> (Self, Sender<(ConnectionId, Event)>) {
        let (router_tx, router_rx) = bounded(1000);
        let id = config.id;
        let max_connections = config.max_connections;

        // Connection level information
        let connections = Slab::with_capacity(max_connections);
        let trackers = Slab::with_capacity(max_connections);
        let watermarks = Slab::with_capacity(max_connections);

        // Global data
        let connectionslog = ConnectionsLog::new();
        let datalog: DataLog = DataLog::new(config.clone());
        let topicslog = TopicsLog::new();

        // Waiters to notify new data or topics
        let data_waiters = DataWaiters::new();
        let topics_waiters = TopicsWaiters::new();
        let readyqueue = ReadyQueue::new();
        let metrics = RouterMetrics::new(id);

        let router = Router {
            config,
            _id: id,
            connectionslog,
            datalog,
            topicslog,
            connections,
            trackers,
            watermarks,
            readyqueue,
            data_waiters,
            topics_waiters,
            router_rx,
            metrics,
        };

        (router, router_tx)
    }

    /// Waits on incoming events when ready queue is empty.
    /// After pulling 1 event, tries to pull 500 more events
    /// before polling ready queue 100 times (connections)
    pub fn start(&mut self) -> Result<(), RouterError> {
        loop {
            // Block on incoming events if there are no connections
            // in ready queue.
            if self.readyqueue.is_empty() {
                let (id, data) = self.router_rx.recv()?;
                self.route(id, data);
            }

            // Try reading more from connections in a non-blocking
            // fashion to accumulate data and handle subscriptions.
            // Accumulating more data lets requests retrieve bigger
            // bulks which in turn increases efficiency
            for _ in 0..500 {
                // All these methods will handle state and errors
                match self.router_rx.try_recv() {
                    Ok((id, data)) => self.route(id, data),
                    Err(TryRecvError::Closed) => return Err(RouterError::Disconnected),
                    Err(TryRecvError::Empty) => break,
                }
            }

            // Poll 100 connections which are ready in ready queue
            for _ in 0..100 {
                match self.readyqueue.pop_front() {
                    Some(id) => self.connection_ready(id, 100),
                    None => break,
                }
            }
        }
    }

    fn route(&mut self, id: usize, data: Event) {
        match data {
            Event::Connect(connection) => self.handle_new_connection(connection),
            Event::Data(data) => self.handle_connection_data(id, data),
            Event::Disconnect(request) => self.handle_disconnection(id, request),
            Event::Ready => self.connection_ready(id, 100),
            Event::Metrics(metrics) => self.retrieve_metrics(id, metrics),
        }
    }

    fn retrieve_metrics(&mut self, id: ConnectionId, metrics: MetricsRequest) {
        info!("{:11} {:14} Id = {}", "console", "metrics", id);
        let message = match metrics {
            MetricsRequest::Config => {
                Notification::Metrics(MetricsReply::Config(self.config.clone()))
            }
            MetricsRequest::Router => {
                Notification::Metrics(MetricsReply::Router(self.metrics.clone()))
            }
            MetricsRequest::Connection(device_id) => {
                let tracker = match self.connectionslog.id(&device_id) {
                    Some(id) => self.trackers.get_mut(id).cloned(),
                    None => None,
                };

                Notification::Metrics(MetricsReply::Connection(ConnectionMetrics::new(
                    device_id, tracker,
                )))
            }
        };

        notify(&mut self.connections, id, message);
    }

    fn handle_new_connection(&mut self, connection: Connection) {
        let clean = connection.clean();

        let (id, mut tracker, mut pending) = match connection.conn.clone() {
            ConnectionType::Replicator(id) => {
                info!("{:11} {:14} Id = {}", "connection", "replicator", id,);
                self.connections.insert_at(connection, id);
                (id, None, None)
            }
            ConnectionType::Device(did) => match self.connections.insert(connection) {
                Some(id) => {
                    info!("{:11} {:14} Id = {}:{}", "connection", "remote", did, id);
                    let (tracker, pending) = self.connectionslog.add(&did, id);
                    (id, tracker, pending)
                }
                None => {
                    error!("No space for new connection!!");
                    return;
                }
            },
        };

        let previous_session = tracker.is_some();

        // Add a new tracker or resume from previous topics and offsets
        match tracker.take() {
            Some(tracker) if !clean => self.trackers.insert_at(tracker, id),
            _ => self.trackers.insert_at(Tracker::new(), id),
        }

        let ack = match pending.take() {
            Some(pending) => ConnectionAck::Success((id, previous_session, pending)),
            None => ConnectionAck::Success((id, previous_session, Vec::new())),
        };

        self.watermarks.insert_at(Acks::new(), id);
        self.readyqueue.push_back(id);

        let message = Notification::ConnectionAck(ack);
        notify(&mut self.connections, id, message);
    }

    fn handle_disconnection(&mut self, id: ConnectionId, disconnect: Disconnection) {
        let did = disconnect.id;
        let execute_will = disconnect.execute_will;
        let pending = disconnect.pending;

        info!("{:11} {:14} Id = {}:{}", "disconnect", "", did, id);

        // Forward connection will
        let mut connection = self.connections.remove(id).unwrap();
        let clean = connection.clean();

        if execute_will {
            if let Some(will) = connection.will() {
                let publish = Publish::from_bytes(will.topic, will.qos, will.message);
                self.handle_connection_publish(id, publish);
            }
        }

        let mut tracker = self.trackers.remove(id);
        let inflight_data_requests = self.data_waiters.remove(id);
        let mut inflight_topics_request = self.topics_waiters.remove(id);
        self.watermarks.remove(id);
        self.readyqueue.remove(id);

        if !clean {
            if let Some(mut tracker) = tracker.take() {
                // Add inflight data requests back to tracker
                for request in inflight_data_requests {
                    tracker.register_data_request(request);
                }

                // Add inflight topics request back to tracker
                if let Some(request) = inflight_topics_request.take() {
                    tracker.register_topics_request(request);
                }

                // Add acks request. This might be a duplicate
                // TODO Get this from 'watermarks.remove'
                tracker.register_acks_request();

                // Save tracker
                self.connectionslog.save(&did, tracker, pending);
            }
        }
    }

    fn connection_ready(&mut self, id: ConnectionId, max_iterations: usize) {
        trace!("{:11} {:14} Id = {}", "requests", "start", id,);
        let tracker = self.trackers.get_mut(id).unwrap();
        if tracker.busy_unschedule() {
            tracker.set_busy_unschedule(false);
        }

        // Iterate through a max of 'max_iterations' requests everytime a connection.
        // if polled. This prevents a connection from unfairly taking up router's time
        // preventing other connections from making progress.
        for _ in 0..max_iterations {
            match tracker.pop_request() {
                Some(request) => match request {
                    Request::Data(request) => {
                        let datalog = &mut self.datalog;
                        let waiters = &mut self.data_waiters;

                        // Get data from commitlog and register for notification if
                        // all the data is caught up.
                        if let Some(data) = handle_data_request(id, request, datalog, waiters) {
                            // If data is yielded by commitlog, register a new data request
                            // in the tracker with next offset and send data notification to
                            // the connection
                            let topic = data.topic.clone();
                            let qos = data.qos;
                            let cursors = data.cursor;
                            let last_retain = data.last_retain;

                            let request = DataRequest::offsets(topic, qos, cursors, last_retain);
                            tracker.register_data_request(request);
                            let notification = Notification::Data(data);
                            let pause = notify(&mut self.connections, id, notification);

                            // This connection might not be able to process next request. Don't schedule
                            if pause {
                                info!("Connection busy. Unschedule. Id = {}", id);
                                tracker.set_busy_unschedule(true);
                                return;
                            }
                        }
                    }
                    Request::Topics(request) => {
                        let topicslog = &mut self.topicslog;
                        let waiters = &mut self.topics_waiters;

                        // Get topics from topics log and register for notification if
                        // all the topics are caught up
                        if let Some(data) = handle_topics_request(id, request, topicslog, waiters) {
                            // Register for new topics request if previous request is handled
                            tracker.track_matched_topics(data.topics);
                            tracker.register_topics_request(TopicsRequest::offset(data.offset));
                        }
                    }
                    Request::Acks(_) => {
                        // Get acks from commitlog and register for notification if all
                        // the data is caught up.
                        let acks = self.watermarks.get_mut(id).unwrap();

                        // If acks are yielded, register a new acks request
                        // and send acks notification to the connection
                        if let Some(acks) = handle_acks_request(id, acks) {
                            tracker.register_acks_request();
                            let notification = Notification::Acks(acks);
                            let pause = notify(&mut self.connections, id, notification);

                            // This connection might not be able to process next request. Don't schedule
                            if pause {
                                info!("Connection busy/closed. Unschedule. Id = {}", id);
                                tracker.set_busy_unschedule(true);
                                return;
                            }
                        }
                    }
                },
                None => {
                    // At this point, pending requests in tracker is 0. Early return
                    // here prevents connection from being added to ready queue again.
                    // New requests are added to tracker through notifications of
                    // previous requests. When first request is added back to tracker
                    // again, it's scheduled back on ready queue again.
                    trace!("{:11} {:14} Id = {}", "requests", "done", id);
                    tracker.set_empty_unschedule(true);
                    return;
                }
            }
        }

        // If there are more requests in the tracker, add the connection back
        // to ready queue.
        trace!("{:11} {:14} Id = {}", "requests", "pause", id,);
        self.readyqueue.push_back(id);
    }

    /// Handles new incoming data on a topic
    fn handle_connection_data(&mut self, id: ConnectionId, data: Vec<Packet>) {
        trace!(
            "{:11} {:14} Id = {} Count = {}",
            "data",
            "incoming",
            id,
            data.len()
        );

        for publish in data {
            match publish {
                Packet::Publish(publish) => self.handle_connection_publish(id, publish),
                Packet::Subscribe(subscribe) => self.handle_connection_subscribe(id, subscribe),
                Packet::Unsubscribe(unsubscribe) => {
                    self.handle_connection_unsubscribe(id, unsubscribe)
                }
                incoming => {
                    warn!("Packet = {:?} not supported by router yet", incoming);
                }
            }
        }

        trace!("{:11} {:14} Id = {}", "data", "committed", id,);
    }

    fn handle_connection_subscribe(&mut self, id: ConnectionId, subscribe: Subscribe) {
        trace!(
            "{:11} {:14} Id = {} Filters = {:?}",
            "data",
            "subscribe",
            id,
            subscribe.filters
        );

        let topics = self.topicslog.readv(0, 0);
        let tracker = self.trackers.get_mut(id).unwrap();

        let mut return_codes = Vec::new();
        for filter in subscribe.filters.iter() {
            if filter.path.starts_with("test") || filter.path.starts_with('$') {
                return_codes.push(SubscribeReasonCode::Failure);
            } else {
                return_codes.push(SubscribeReasonCode::Success(filter.qos));
            }
        }

        // A new subscription should match with all the existing topics and take a snapshot of current
        // offset of all the matched topics. Subscribers will receive data from the next offset
        match topics {
            Some((_, topics)) => {
                // Add subscription and get topics matching all the existing topics
                // in the (topics) commitlog ant seek them to next offset. Add subscriptions
                // and store matched topics interna. If this is the first subscription,
                // register topics request
                if tracker.add_subscription_and_match(subscribe.filters, topics) {
                    tracker.register_topics_request(TopicsRequest::offset(topics.len()));

                    // If connection is removed from ready queue because of 0 requests,
                    // but connection itself is ready for more notifications, add
                    // connection back to ready queue
                    if tracker.empty_unschedule() {
                        self.readyqueue.push_back(id);
                        tracker.set_empty_unschedule(false);
                    }
                }

                // Take matched topics above and seek their offsets. Seeking is
                // necessary because new subscription should yield only subsequent data
                // FIXME: Verify logic of self.id. Should this be seeked for replicators as well?
                while let Some(mut topic) = tracker.next_matched() {
                    self.datalog.seek_offsets_to_end(&mut topic);
                    let (topic, qos, cursors) = (topic.0, topic.1, topic.2);
                    let request = DataRequest::offsets(topic, qos, cursors, 0);
                    tracker.register_data_request(request);

                    // If connection is removed from ready queue because of 0 requests,
                    // but connection itself is ready for more notifications, add
                    // connection back to ready queue
                    if tracker.empty_unschedule() {
                        self.readyqueue.push_back(id);
                        tracker.set_empty_unschedule(false);
                    }
                }
            }
            None => {
                // Router did not receive data from any topics yet. Add subscription and
                // register topics request from offset 0
                if tracker.add_subscription_and_match(subscribe.filters, &[]) {
                    tracker.register_topics_request(TopicsRequest::offset(0));

                    // If connection is removed from ready queue because of 0 requests,
                    // but connection itself is ready for more notifications, add
                    // connection back to ready queue
                    if tracker.empty_unschedule() {
                        self.readyqueue.push_back(id);
                        tracker.set_empty_unschedule(false);
                    }
                }
            }
        };

        // Update acks and triggers acks notification for suback
        let watermarks = self.watermarks.get_mut(id).unwrap();
        watermarks.push_subscribe_ack(subscribe.pkid, return_codes);
        self.fresh_acks_notification(id);
    }

    fn handle_connection_unsubscribe(&mut self, id: ConnectionId, unsubscribe: Unsubscribe) {
        trace!(
            "{:11} {:14} Id = {} Filters = {:?}",
            "data",
            "unsubscribe",
            id,
            unsubscribe.topics
        );

        let tracker = self.trackers.get_mut(id).unwrap();
        let inflight = tracker.remove_subscription_and_unmatch(unsubscribe.topics);

        for topic in inflight.into_iter() {
            if let Some(waiters) = self.data_waiters.get_mut(&topic) {
                waiters.remove(id);
            }
        }

        // Update acks and triggers acks notification for suback
        let watermarks = self.watermarks.get_mut(id).unwrap();
        watermarks.push_unsubscribe_ack(unsubscribe.pkid);
        self.fresh_acks_notification(id);
    }

    fn handle_connection_publish(&mut self, id: ConnectionId, publish: Publish) {
        let Publish {
            pkid,
            topic,
            payload,
            qos,
            retain,
            ..
        } = publish;

        let is_new_topic = if retain {
            let is_new_topic = match self.datalog.retain(&topic, payload) {
                Some(v) => v,
                None => return,
            };

            if qos as u8 > 0 {
                let watermarks = self.watermarks.get_mut(id).unwrap();
                watermarks.push_publish_ack(pkid, qos as u8);
            }

            is_new_topic
        } else {
            if payload.is_empty() {
                warn!("Empty publish. ID = {:?}, topic = {:?}", id, topic);
                // Some tests in paho test suite are sending empty publishes.
                // Disabling this filter for the time being
                // return;
            }

            let (is_new_topic, _) = match self.datalog.append(&topic, payload) {
                Some(v) => v,
                None => return,
            };

            if qos as u8 > 0 {
                let watermarks = self.watermarks.get_mut(id).unwrap();
                watermarks.push_publish_ack(pkid, qos as u8);
            }

            is_new_topic
        };

        // If there is a new unique append, send it to connection waiting on it
        // This is equivalent to hybrid of block and poll and we don't need timers.
        // Connections/Replicator will make a request and request might fail as
        // there are no new topic publishes. Router caches the failed request on the topic.
        // If there is new topic, router fulfills the last failed request.
        // Note that the above commitlog append distinguishes `is_new_topic` by connection's
        // commitlog (i.e native or replica commitlog). So there is a chance that topic log
        // has duplicate topics. Tracker filters these duplicates though
        if is_new_topic {
            self.topicslog.append(&topic);
            self.fresh_topics_notification();
        }

        // Notify waiters on this topic of new data
        self.fresh_data_notification(&topic);

        // Data from topics with replication factor = 0 should be acked immediately if there are
        // waiters registered. We shouldn't rely on replication acks for data acks in this case
        self.fresh_acks_notification(id);
    }

    /// Send notifications to links which registered them. Id is only used to
    /// identify if new topics are from a replicator.
    /// New topic from one connection involves notifying other connections which
    /// are possibly interested in these topics. So this involves going through
    /// all the topic waiters
    fn fresh_topics_notification(&mut self) {
        let waiters = &mut self.topics_waiters;

        while let Some((link_id, request)) = waiters.pop_front() {
            // Ignore connections with zero subscriptions.
            let tracker = self.trackers.get_mut(link_id).unwrap();
            if tracker.subscription_count() == 0 {
                // panic!("Topics request registration for 0 subscription connection");
                continue;
            }

            trace!("{:11} {:14} Id = {}", "topics", "notification", link_id);

            // Match new topics with existing subscriptions of this link.
            // Even though there are new topics, it's possible that they didn't match
            // subscriptions held by this connection.
            // Register next topics request in the router
            tracker.register_topics_request(TopicsRequest::offset(request.offset));

            // If connection is removed from ready queue because of 0 requests,
            // but connection itself is ready for more notifications, add
            // connection back to ready queue
            if tracker.empty_unschedule() {
                self.readyqueue.push_back(link_id);
                tracker.set_empty_unschedule(false);
            }
        }

        waiters.prepare_next();
    }

    /// Send data to links which registered them
    fn fresh_data_notification(&mut self, topic: &str) {
        // There might not be any waiters on this topic
        // FIXME: Every notification trigger is a hashmap lookup
        let waiters = match self.data_waiters.get_mut(topic) {
            Some(waiters) => waiters,
            None => return,
        };

        while let Some((link_id, request)) = waiters.pop_front() {
            let tracker = self.trackers.get_mut(link_id).unwrap();

            let topic = request.topic;
            let qos = request.qos;
            let cursors = request.cursor;
            let last_retain = request.last_retain;

            let request = DataRequest::offsets(topic, qos, cursors, last_retain);
            tracker.register_data_request(request);

            // If connection is removed from ready queue because of 0 requests,
            // but connection itself is ready for more notifications, add
            // connection back to ready queue
            if tracker.empty_unschedule() {
                self.readyqueue.push_back(link_id);
                tracker.set_empty_unschedule(false);
            }
        }

        waiters.prepare_next();
    }

    fn fresh_acks_notification(&mut self, id: ConnectionId) {
        let watermarks = self.watermarks.get_mut(id).unwrap();

        // Unlike data and topics where notifications are routed
        // to other connections, acks are meant for the same connection.
        // We use watermark's own flag to determine if it's waiting for
        // a notification
        if watermarks.take_pending_acks_request().is_some() {
            trace!("{:11} {:14} Id = {}", "acks", "notification", id);

            // Add next acks request to the tracker
            let tracker = self.trackers.get_mut(id).unwrap();
            tracker.register_acks_request();

            // If connection is removed from ready queue because of 0 requests,
            // but connection itself is ready for more notifications, add
            // connection back to ready queue
            if tracker.empty_unschedule() {
                self.readyqueue.push_back(id);
                tracker.set_empty_unschedule(false);
            }
        }
    }
}

fn handle_data_request(
    id: ConnectionId,
    request: DataRequest,
    datalog: &mut DataLog,
    waiters: &mut DataWaiters,
) -> Option<Data> {
    trace!(
        "{:11} {:14} Id = {} Topic = {} Offsets = {:?}",
        "data",
        "request",
        id,
        request.topic,
        request.cursor
    );

    let data = match datalog.extract_data(&request) {
        Some(data) => {
            trace!(
                "{:11} {:14} Id = {} Topic = {} Offsets = {:?} Count = {}",
                "data",
                "response",
                id,
                data.topic,
                data.cursor,
                data.payload.len()
            );

            data
        }
        None => {
            trace!(
                "{:11} {:14} Id = {} Topic = {}",
                "data",
                "register",
                id,
                request.topic
            );

            waiters.register(id, request);
            return None;
        }
    };

    Some(data)
}

fn handle_topics_request<'a>(
    id: ConnectionId,
    request: TopicsRequest,
    topicslog: &'a mut TopicsLog,
    waiters: &mut TopicsWaiters,
) -> Option<Topics<'a>> {
    trace!(
        "{:11} {:14} Id = {} Offset = {}",
        "topics",
        "request",
        id,
        request.offset
    );

    let topics = match topicslog.readv(request.offset, request.count) {
        Some(topics) => {
            trace!(
                "{:11} {:14} Id = {} Offset = {} Count = {}",
                "topics",
                "response",
                id,
                topics.0,
                topics.1.len()
            );

            Topics::new(topics.0, topics.1)
        }
        None => {
            trace!("{:11} {:14} Id = {}", "topics", "register", id);
            waiters.register(id, request);
            return None;
        }
    };

    Some(topics)
}

fn handle_acks_request(id: ConnectionId, acks: &mut Acks) -> Option<Vec<Packet>> {
    trace!("{:11} {:14} Id = {}", "acks", "request", id);
    let acks = match acks.handle_acks_request() {
        Some(acks) => {
            trace!(
                "{:11} {:14} Id = {} Count = {}",
                "acks",
                "response",
                id,
                acks.len()
            );

            acks
        }
        None => {
            trace!("{:11} {:14} Id = {}", "acks", "register", id);
            acks.register_pending_acks_request();
            return None;
        }
    };

    Some(acks)
}

/// Notifies and returns unschedule status if the connection is busy
fn notify(connections: &mut Slab<Connection>, id: ConnectionId, reply: Notification) -> bool {
    let connection = match connections.get_mut(id) {
        Some(c) => c,
        None => {
            error!("Invalid id while notifying = {:?}", id);
            return true;
        }
    };

    connection.notify(reply)
}

#[cfg(test)]
mod test {
    use super::*;
    use mqttbytes::*;

    #[test]
    fn topics_notifications_does_not_create_infinite_loops() {
        let (mut router, _tx) = Router::new(Arc::new(Config::default()));

        // Instantiate replica connections with subscriptions and topic request notifications
        for i in 10..20 {
            let client_id = &format!("{}", i);
            add_new_remote_connection(&mut router, client_id);
            add_new_subscription(&mut router, i, "hello/world");
            router.topics_waiters.register(i, TopicsRequest::new());
        }

        router.topicslog.append("hello/world");

        // Next iteration of `fresh_topics_notification` will have all missed notifications
        for i in 10..20 {
            let request = router.topics_waiters.pop_front().unwrap();
            assert_eq!(i, request.0);
        }
    }

    #[test]
    fn data_notifications_does_not_create_infinite_loops() {
        let (mut router, _tx) = Router::new(Arc::new(Config::default()));

        // Instantiate replica connections with subscriptions and topic request notifications
        for i in 10..20 {
            let client_id = &format!("{}", i);
            add_new_remote_connection(&mut router, client_id);
            add_new_subscription(&mut router, i, "hello/world");
            router
                .data_waiters
                .register(i, DataRequest::new("hello/world".to_owned(), 1));
        }

        let payload = Bytes::from(vec![1, 2, 3]);
        router.datalog.append("hello/world", payload);

        // Next iteration of `fresh_topics_notification` will have all missed notifications
        let waiters = router.data_waiters.get_mut("hello/world").unwrap();
        for i in 10..20 {
            let request = waiters.pop_front().unwrap();
            assert_eq!(i, request.0);
        }
    }

    /// Connection should not be part of next iteration of ready queue
    /// when previous notification fails
    #[test]
    fn failed_notification_connection_should_not_be_scheduled() {
        let config = Config {
            id: 0,
            ..Config::default()
        };

        let (mut router, _tx) = Router::new(Arc::new(config));
        let _rx = add_new_remote_connection(&mut router, "10");

        // Register 20 data requests in notifications
        for i in 0..20 {
            let topic = format!("hello/{}/world", i);
            let request = DataRequest::new(topic, 1);
            router.data_waiters.register(10, request);
        }

        // Along with connection ack. 10th notification will fail
        for i in 1..=10 {
            // Write a publish to commitlog
            let publish = Publish::new("hello/world", QoS::AtLeastOnce, vec![1, 2, 3]);
            router.handle_connection_data(10, vec![Packet::Publish(publish)]);

            // connack + 8 pub acks. 9 acks before this iteration
            // implies 9 notifications + 1 pause notification = 10 notifications
            // channel full before this iteration
            if i == 9 {
                assert!(router.readyqueue.pop_front().is_none());
                break;
            } else {
                // Trigger 1 request. This will notify puback for above
                // data and puts connection back in ready queue.
                let id = router.readyqueue.pop_front().unwrap();
                router.connection_ready(id, 1);
            }
        }

        assert!(router.readyqueue.is_empty());
    }

    fn add_new_remote_connection(router: &mut Router, client_id: &str) -> Receiver<Notification> {
        let (connection, rx) = Connection::new_remote(client_id, true, 10);
        router.handle_new_connection(connection);
        rx
    }

    fn add_new_subscription(router: &mut Router, id: usize, topic: &str) {
        router.handle_connection_subscribe(id, Subscribe::new(topic, QoS::AtLeastOnce));
    }
}
