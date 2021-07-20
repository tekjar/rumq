use crate::network::Network;
use crate::state::{self, State};
use crate::{network, ConnectionSettings, Id};
use mqttbytes::v4::*;
use mqttbytes::*;
use rumqttlog::{
    Connection, ConnectionAck, Event, Notification, Receiver, RecvError, SendError, Sender,
};

use std::sync::Arc;
use std::{io, mem};
use tokio::time::{error::Elapsed, Duration};
use tokio::{select, time};

pub struct RemoteLink {
    config: Arc<ConnectionSettings>,
    connect: Connect,
    id: Id,
    network: Network,
    router_tx: Sender<(Id, Event)>,
    link_rx: Receiver<Notification>,
    total: usize,
    pending: Vec<Notification>,
    pub(crate) state: State,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("Network {0}")]
    Network(#[from] network::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("State error")]
    State(#[from] state::Error),
    #[error("Unexpected router message")]
    RouterMessage(Notification),
    #[error("Connack error {0}")]
    ConnAck(String),
    #[error("Keep alive time exceeded")]
    KeepAlive,
    #[error("Channel send error")]
    Send(#[from] SendError<(Id, Event)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Payload count greater than max inflight")]
    TooManyPayloads(usize),
    #[error("Persistent session requires valid client id")]
    InvalidClientId,
    #[error("Invalid username or password provided")]
    InvalidUsernameOrPassword,
    #[error("Disconnect request")]
    Disconnect,
}

impl RemoteLink {
    pub async fn new(
        config: Arc<ConnectionSettings>,
        router_tx: Sender<(Id, Event)>,
        mut network: Network,
    ) -> Result<(String, Id, RemoteLink), Error> {
        // Wait for MQTT connect packet and error out if it's not received in time to prevent
        // DOS attacks by filling total connections that the server can handle with idle open
        // connections which results in server rejecting new connections
        let timeout = Duration::from_millis(config.connection_timeout_ms.into());
        let mut connect = time::timeout(timeout, async {
            let connect = network.read_connect().await?;

            // Validate credentials if they exist
            if let Some(credentials) = &config.login_credentials {
                let validated = match &connect.login {
                    Some(l) => {
                        let mut validated = false;

                        // Iterate through all the potential credentials
                        for entry in credentials {
                            if l.validate(&entry.username, &entry.password) {
                                validated = true;
                                break;
                            }
                        }

                        validated
                    }
                    None => false,
                };

                // Return error if the username/password werenot found
                if validated == false {
                    return Err(Error::InvalidUsernameOrPassword);
                }
            }

            Ok::<_, Error>(connect)
        })
        .await??;

        // Register this connection with the router. Router replys with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)
        let client_id = connect.client_id.clone();
        let clean_session = connect.clean_session;
        if !clean_session && client_id.is_empty() {
            return Err(Error::InvalidClientId);
        }

        let (mut connection, link_rx) = Connection::new_remote(&client_id, clean_session, 10);

        // Add last will to connection
        if let Some(will) = connect.last_will.take() {
            connection.set_will(will);
        }

        let message = (0, Event::Connect(connection));
        router_tx.send(message).unwrap();

        // TODO When a new connection request is sent to the router, router should ack with error
        // TODO if it exceeds maximum allowed active connections
        // Right now link identifies failure with dropped rx in router, which is probably ok for now
        let (id, session, pending) = match link_rx.recv()? {
            Notification::ConnectionAck(ack) => match ack {
                ConnectionAck::Success((id, session, pending)) => (id, session, pending),
                ConnectionAck::Failure(reason) => return Err(Error::ConnAck(reason)),
            },
            message => return Err(Error::RouterMessage(message)),
        };

        // Send connection acknowledgement back to the client
        let connack = ConnAck::new(ConnectReturnCode::Success, session);
        network.connack(connack).await?;

        let max_inflight_count = config.max_inflight_count;
        Ok((
            client_id,
            id,
            RemoteLink {
                config,
                connect,
                id,
                network,
                router_tx,
                link_rx,
                total: 0,
                pending,
                state: State::new(max_inflight_count),
            },
        ))
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        self.network.set_keepalive(self.connect.keep_alive);
        let pending = mem::take(&mut self.pending);
        let mut pending = pending.into_iter();

        loop {
            select! {
                o = self.network.readb(&mut self.state) => {
                    let disconnect = o?;
                    self.handle_network_data().await?;

                    if disconnect {
                        return Err(Error::Disconnect)
                    }
                }
                // Receive from router when previous when state isn't in collision
                // due to previously received data request
                message = async { pending.next() } => {
                    match message {
                        Some(message) => self.handle_router_response(message).await?,
                        None => break
                    };
                }
            }
        }

        // Note:
        // Shouldn't result in bounded queue deadlocks because of blocking n/w send
        loop {
            select! {
                o = self.network.readb(&mut self.state) => {
                    let disconnect = o?;
                    self.handle_network_data().await?;

                    if disconnect {
                        return Err(Error::Disconnect)
                    }
                }
                // Receive from router when previous when state isn't in collision
                // due to previously received data request
                message = self.link_rx.async_recv(), if !self.state.pause_outgoing() => {
                    let message = message?;
                    self.handle_router_response(message).await?;
                }
            }
        }
    }

    async fn handle_network_data(&mut self) -> Result<(), Error> {
        let data = self.state.take_incoming();
        self.network.flush(self.state.write_mut()).await?;

        if !data.is_empty() {
            debug!(
                "{:11} {:14} Id = {}, Count = {}",
                "data",
                "remote",
                self.id,
                data.len()
            );

            let message = Event::Data(data);
            self.router_tx.send((self.id, message))?;
        }

        Ok(())
    }

    async fn handle_router_response(&mut self, message: Notification) -> Result<(), Error> {
        match message {
            Notification::ConnectionAck(_) => {}
            Notification::Acks(reply) => {
                trace!(
                    "{:11} {:14} Id = {}, Count = {}",
                    "acks",
                    "reply",
                    self.id,
                    reply.len()
                );

                for ack in reply.into_iter() {
                    self.state.outgoing_ack(ack)?;
                }
            }
            Notification::Message(reply) => {
                let topic = reply.topic;
                let qos = qos(reply.qos).unwrap();
                let payload = reply.payload;

                trace!(
                    "{:11} {:14} Id = {}, Topic = {}, Count = {}",
                    "data",
                    "pending",
                    self.id,
                    topic,
                    payload.len()
                );

                self.state.add_pending(topic, qos, vec![payload]);
                self.state.write_pending()?;
            }
            Notification::Data(reply) => {
                let topic = reply.topic;
                let qos = qos(reply.qos).unwrap();
                let payload = reply.payload;

                trace!(
                    "{:11} {:14} Id = {}, Topic = {}, Offsets = {:?}, Count = {}",
                    "data",
                    "reply",
                    self.id,
                    topic,
                    reply.cursor,
                    payload.len()
                );

                let payload_count = payload.len();
                if payload_count > self.config.max_inflight_count as usize {
                    return Err(Error::TooManyPayloads(payload_count));
                }

                self.total += payload_count;
                self.state.add_pending(topic, qos, payload);
                self.state.write_pending()?;
            }
            Notification::Pause => {
                let message = (self.id, Event::Ready);
                self.router_tx.send(message)?;
            }
            notification => {
                warn!("{:?} not supported in remote link", notification);
            }
        }

        self.network.flush(self.state.write_mut()).await?;
        Ok(())
    }
}
