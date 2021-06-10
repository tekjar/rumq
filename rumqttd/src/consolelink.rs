use crate::Config;
use rumqttlog::ConnectionId;
use rumqttlog::{
    Connection, ConnectionAck, Event, MetricsReply, MetricsRequest, Notification, Receiver, Sender,
};
use std::sync::Arc;
use warp::Filter;

pub struct ConsoleLink {
    config: Arc<Config>,
    id: ConnectionId,
    router_tx: Sender<(ConnectionId, Event)>,
    link_rx: Receiver<Notification>,
}

impl ConsoleLink {
    /// Requires the corresponding Router to be running to complete
    pub fn new(config: Arc<Config>, router_tx: Sender<(ConnectionId, Event)>) -> ConsoleLink {
        let (connection, link_rx) = Connection::new_remote("console", true, 10);
        let message = (0, Event::Connect(connection));
        router_tx.send(message).unwrap();

        let (id, _, _) = match link_rx.recv().unwrap() {
            Notification::ConnectionAck(ack) => match ack {
                ConnectionAck::Success((id, session, pending)) => (id, session, pending),
                ConnectionAck::Failure(reason) => unreachable!("{}", reason),
            },
            notification => unreachable!("{:?}", notification),
        };

        ConsoleLink {
            config,
            router_tx,
            link_rx,
            id,
        }
    }
}

pub async fn start(console: Arc<ConsoleLink>) {
    let config_console = console.clone();
    let address = config_console.config.console.listen;

    let config = warp::path!("node" / "config").map(move || {
        let config = config_console.config.clone();
        warp::reply::json(&config)
    });

    let router_console = console.clone();
    let router = warp::path!("node" / "router").map(move || {
        let message = Event::Metrics(MetricsRequest::Router);
        router_console
            .router_tx
            .send((router_console.id, message))
            .unwrap();

        match router_console.link_rx.recv().unwrap() {
            Notification::Metrics(MetricsReply::Router(v)) => warp::reply::json(&v),
            v => unreachable!("{:?}", v),
        }
    });

    let connection_console = console.clone();
    let connection = warp::path!("node" / String).map(move |id| {
        let message = Event::Metrics(MetricsRequest::Connection(id));
        connection_console
            .router_tx
            .send((connection_console.id, message))
            .unwrap();

        match connection_console.link_rx.recv().unwrap() {
            Notification::Metrics(MetricsReply::Connection(v)) => warp::reply::json(&v),
            v => unreachable!("{:?}", v),
        }
    });

    let routes = warp::get().and(config.or(router).or(connection));
    warp::serve(routes).run(address).await;
}
