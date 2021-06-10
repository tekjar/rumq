use mqttbytes::v4::*;
use mqttbytes::*;
use rumqttlog::*;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn acks_are_returned_as_expected_to_the_connection() {
    let connections = Connections::new();
    let (connection_1_id, connection_1_rx) = connections.connection("1", 10);

    // Send data one by one
    for i in 0..1000 {
        connections.data(connection_1_id, "hello/1/world", vec![1, 2, 3], i);
    }

    let mut count = 0;
    loop {
        count += wait_for_acks(&connection_1_rx).unwrap().len();
        if count == 1000 {
            break;
        }
    }

    // Send data in bulk
    connections.datav(connection_1_id, "hello/1/world", vec![1, 2, 3], 1000);
    let mut count = 0;
    loop {
        count += wait_for_acks(&connection_1_rx).unwrap().len();
        if count == 1000 {
            break;
        }
    }
}

#[test]
fn new_connection_data_notifies_interested_connections() {
    let connections = Connections::new();

    // Create connections. They will be ready in ready queue immediately and
    // acks requests will start and gets registered in waiter as there are no
    // acks to give
    let (connection_1_id, _connection_1_rx) = connections.connection("1", 5);
    let (connection_2_id, connection_2_rx) = connections.connection("2", 5);

    // Even though there are connections in ready queue, router will try to pull
    // 500 more events. This subscribe adds topics request to connection 2's tracker
    connections.subscribe(connection_2_id, "hello/+/world", 1);

    // Above subscribe will wake up previously registered acks request in
    // the waiter and adds it back to connection's tracker.
    // Now that events there are no more events ready, ready queue will be
    // scheduled and topics request will result in registration as there are
    // no new topics.
    // Next, acks request will result in response notification
    // and new acks request is added to tracker and this request
    // is registered next as there are no new acks
    let acks = wait_for_acks(&connection_2_rx).unwrap();
    assert_eq!(acks.len(), 1);

    // Router will now wait for 1 new event to unblock and tries to read 500 more
    // So all the 4 messages (of 2 topics) are written to commitlog.
    // During first data,
    // Acks request from waiter will be added to conection 10's tracker a
    // Topics requests from waiter will be added to connection 11's tracker
    connections.data(connection_1_id, "hello/1/world", vec![1, 2, 3], 1);
    connections.data(connection_1_id, "hello/1/world", vec![4, 5, 6], 2);
    connections.data(connection_1_id, "hello/2/world", vec![13, 14, 15], 4);
    connections.data(connection_1_id, "hello/2/world", vec![16, 17, 18], 5);

    // Router now schedules topics request, which results in topics response.
    // Topics response adds data requests "hello/1/world" and "hello/2/world"
    // to connection 2's tracker
    // Next data request will respond with "hello/1/world"'s data
    let data = wait_for_data(&connection_2_rx).unwrap();
    assert_eq!(data.payload.len(), 2);
    assert_eq!(data.payload[0].as_ref(), &[1, 2, 3]);
    assert_eq!(data.payload[1].as_ref(), &[4, 5, 6]);

    // Next data request will respond with "hello/2/world"'s data
    let data = wait_for_data(&connection_2_rx).unwrap();
    assert_eq!(data.payload.len(), 2);
    assert_eq!(data.payload[0].as_ref(), &[13, 14, 15]);
    assert_eq!(data.payload[1].as_ref(), &[16, 17, 18]);
}

#[test]
fn failed_notifications_are_retried_after_connection_ready() {
    let connections = Connections::new();
    let (connection_id, connection_rx) = connections.connection("1", 3);

    for i in 0..2000 {
        connections.data(connection_id, "hello/1/world", vec![1, 2, 3], i);
    }

    let mut count = 0;
    loop {
        count += match wait_for_notifications(&connection_rx) {
            Some(Notification::Acks(acks)) => acks.len(),
            Some(Notification::Pause) => {
                connections.ready(connection_id);
                continue;
            }
            v => panic!("Expecting acks or pause. Received {:?}", v),
        };

        if count == 2000 {
            break;
        }
    }
}

fn wait_for_data(rx: &Receiver<Notification>) -> Option<Data> {
    thread::sleep(Duration::from_secs(1));

    match rx.try_recv() {
        Ok(Notification::Data(reply)) => Some(reply),
        Ok(v) => {
            println!("Notification = {:?}", v);
            None
        }
        Err(e) => {
            println!("Error = {:?}", e);
            None
        }
    }
}

fn wait_for_acks(rx: &Receiver<Notification>) -> Option<Vec<Packet>> {
    thread::sleep(Duration::from_secs(1));

    match rx.try_recv() {
        Ok(Notification::Acks(reply)) => Some(reply),
        Ok(v) => {
            println!("Notification = {:?}", v);
            None
        }
        Err(e) => {
            println!("Error = {:?}", e);
            None
        }
    }
}

fn wait_for_notifications(rx: &Receiver<Notification>) -> Option<Notification> {
    thread::sleep(Duration::from_secs(1));

    match rx.try_recv() {
        Ok(notification) => Some(notification),
        Err(e) => {
            println!("Error = {:?}", e);
            None
        }
    }
}

// Broker is used to test router
pub(crate) struct Connections {
    router_tx: Sender<(ConnectionId, Event)>,
}

impl Connections {
    pub fn new() -> Connections {
        let config = Config {
            id: 0,
            ..Config::default()
        };

        let (router, router_tx) = Router::new(Arc::new(config));
        thread::spawn(move || {
            let mut router = router;
            let _ = router.start();
        });

        Connections { router_tx }
    }

    pub fn connection(&self, id: &str, cap: usize) -> (ConnectionId, Receiver<Notification>) {
        let (connection, link_rx) = rumqttlog::Connection::new_remote(id, true, cap);
        let message = Event::Connect(connection);
        self.router_tx.send((0, message)).unwrap();

        let (connection_id, ..) = match link_rx.recv().unwrap() {
            Notification::ConnectionAck(ConnectionAck::Success(id)) => id,
            o => panic!("Unexpected connection ack = {:?}", o),
        };

        (connection_id, link_rx)
    }

    pub fn ready(&self, id: usize) {
        let message = (id, Event::Ready);
        self.router_tx.try_send(message).unwrap();
    }

    pub fn data(&self, id: usize, topic: &str, payload: Vec<u8>, pkid: u16) {
        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
        publish.pkid = pkid;

        let message = Event::Data(vec![Packet::Publish(publish)]);
        let message = (id, message);
        self.router_tx.send(message).unwrap();
    }

    pub fn datav(&self, id: usize, topic: &str, payload: Vec<u8>, count: u16) {
        let mut packets = Vec::new();
        for i in 0..count {
            let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload.clone());
            publish.pkid = i;
            packets.push(Packet::Publish(publish));
        }

        let message = Event::Data(packets);
        let message = (id, message);
        self.router_tx.try_send(message).unwrap();
    }

    pub fn subscribe(&self, id: usize, filter: &str, pkid: u16) {
        let mut subscribe = Subscribe::new(filter, QoS::AtLeastOnce);
        subscribe.pkid = pkid;

        let message = Event::Data(vec![Packet::Subscribe(subscribe)]);
        let message = (id, message);
        self.router_tx.try_send(message).unwrap();
    }
}
