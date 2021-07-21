//! This module offers a high level synchronous and asynchronous abstraction to
//! async eventloop.
use crate::{ConnectionError, Event, EventLoop, MqttOptions, Request};

use async_channel::{SendError, Sender, TrySendError};
use bytes::Bytes;
use mqttbytes::v4::*;
use mqttbytes::*;
use std::mem;
use tokio::runtime;
use tokio::runtime::Runtime;

/// Client Error
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Failed to send cancel request to eventloop")]
    Cancel(#[from] SendError<()>),
    #[error("Failed to send mqtt requests to eventloop")]
    Request(#[from] SendError<Request>),
    #[error("Failed to send mqtt requests to eventloop")]
    TryRequest(#[from] TrySendError<Request>),
    #[error("Serialization error")]
    Mqtt4(mqttbytes::Error),
}

/// `AsyncClient` to communicate with MQTT `Eventloop`
/// This is cloneable and can be used to asynchronously Publish, Subscribe.
#[derive(Clone, Debug)]
pub struct AsyncClient {
    request_tx: Sender<Request>,
    cancel_tx: Sender<()>,
}

impl AsyncClient {
    /// Create a new `AsyncClient`
    pub fn new(options: MqttOptions, cap: usize) -> (AsyncClient, EventLoop) {
        let mut eventloop = EventLoop::new(options, cap);
        let request_tx = eventloop.handle();
        let cancel_tx = eventloop.cancel_handle();

        let client = AsyncClient {
            request_tx,
            cancel_tx,
        };

        (client, eventloop)
    }

    /// Create a new `AsyncClient` from a pair of async channel `Sender`s. This is mostly useful for
    /// creating a test instance.
    pub fn from_senders(request_tx: Sender<Request>, cancel_tx: Sender<()>) -> AsyncClient {
        AsyncClient {
            request_tx,
            cancel_tx,
        }
    }

    /// Sends a MQTT Publish to the eventloop
    pub async fn publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), ClientError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let mut publish = Publish::new(topic, qos, payload);
        publish.retain = retain;
        let publish = Request::Publish(publish);
        self.request_tx.send(publish).await?;
        Ok(())
    }

    /// Sends a MQTT Publish to the eventloop
    pub fn try_publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), ClientError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let mut publish = Publish::new(topic, qos, payload);
        publish.retain = retain;
        let publish = Request::Publish(publish);
        self.request_tx.try_send(publish)?;
        Ok(())
    }

    /// Sends a MQTT Publish to the eventloop
    pub async fn publish_bytes<S>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: Bytes,
    ) -> Result<(), ClientError>
    where
        S: Into<String>,
    {
        let mut publish = Publish::from_bytes(topic, qos, payload);
        publish.retain = retain;
        let publish = Request::Publish(publish);
        self.request_tx.send(publish).await?;
        Ok(())
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub async fn subscribe<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<(), ClientError> {
        let subscribe = Subscribe::new(topic.into(), qos);
        let request = Request::Subscribe(subscribe);
        self.request_tx.send(request).await?;
        Ok(())
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn try_subscribe<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<(), ClientError> {
        let subscribe = Subscribe::new(topic.into(), qos);
        let request = Request::Subscribe(subscribe);
        self.request_tx.try_send(request)?;
        Ok(())
    }

    /// Sends a MQTT Subscribe for multiple topics to the eventloop
    pub async fn subscribe_many<T>(&self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        let subscribe = Subscribe::new_many(topics);
        let request = Request::Subscribe(subscribe);
        self.request_tx.send(request).await?;
        Ok(())
    }

    /// Sends a MQTT Subscribe for multiple topics to the eventloop
    pub fn try_subscribe_many<T>(&self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        let subscribe = Subscribe::new_many(topics);
        let request = Request::Subscribe(subscribe);
        self.request_tx.try_send(request)?;
        Ok(())
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub async fn unsubscribe<S: Into<String>>(&self, topic: S) -> Result<(), ClientError> {
        let unsubscribe = Unsubscribe::new(topic.into());
        let request = Request::Unsubscribe(unsubscribe);
        self.request_tx.send(request).await?;
        Ok(())
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub fn try_unsubscribe<S: Into<String>>(&self, topic: S) -> Result<(), ClientError> {
        let unsubscribe = Unsubscribe::new(topic.into());
        let request = Request::Unsubscribe(unsubscribe);
        self.request_tx.try_send(request)?;
        Ok(())
    }

    /// Sends a MQTT disconnect to the eventloop
    pub async fn disconnect(&self) -> Result<(), ClientError> {
        let request = Request::Disconnect;
        self.request_tx.send(request).await?;
        Ok(())
    }

    /// Sends a MQTT disconnect to the eventloop
    pub fn try_disconnect(&self) -> Result<(), ClientError> {
        let request = Request::Disconnect;
        self.request_tx.try_send(request)?;
        Ok(())
    }

    /// Stops the eventloop right away
    pub async fn cancel(&self) -> Result<(), ClientError> {
        self.cancel_tx.send(()).await?;
        Ok(())
    }
}

/// `Client` to communicate with MQTT eventloop `Connection`.
///
/// Client is cloneable and can be used to synchronously Publish, Subscribe.
/// Asynchronous channel handle can also be extracted if necessary
#[derive(Clone)]
pub struct Client {
    client: AsyncClient,
}

impl Client {
    /// Create a new `Client`
    pub fn new(options: MqttOptions, cap: usize) -> (Client, Connection) {
        let (client, eventloop) = AsyncClient::new(options, cap);
        let client = Client { client };
        let runtime = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let connection = Connection::new(eventloop, runtime);
        (client, connection)
    }

    /// Sends a MQTT Publish to the eventloop
    pub fn publish<S, V>(
        &mut self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), ClientError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        pollster::block_on(self.client.publish(topic, qos, retain, payload))?;
        Ok(())
    }

    pub fn try_publish<S, V>(
        &mut self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), ClientError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        self.client.try_publish(topic, qos, retain, payload)?;
        Ok(())
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn subscribe<S: Into<String>>(&mut self, topic: S, qos: QoS) -> Result<(), ClientError> {
        pollster::block_on(self.client.subscribe(topic, qos))?;
        Ok(())
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn try_subscribe<S: Into<String>>(
        &mut self,
        topic: S,
        qos: QoS,
    ) -> Result<(), ClientError> {
        self.client.try_subscribe(topic, qos)?;
        Ok(())
    }

    /// Sends a MQTT Subscribe for multiple topics to the eventloop
    pub fn subscribe_many<T>(&mut self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        pollster::block_on(self.client.subscribe_many(topics))
    }

    pub fn try_subscribe_many<T>(&mut self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        self.client.try_subscribe_many(topics)
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub fn unsubscribe<S: Into<String>>(&mut self, topic: S) -> Result<(), ClientError> {
        pollster::block_on(self.client.unsubscribe(topic))?;
        Ok(())
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub fn try_unsubscribe<S: Into<String>>(&mut self, topic: S) -> Result<(), ClientError> {
        self.client.try_unsubscribe(topic)?;
        Ok(())
    }

    /// Sends a MQTT disconnect to the eventloop
    pub fn disconnect(&mut self) -> Result<(), ClientError> {
        pollster::block_on(self.client.disconnect())?;
        Ok(())
    }

    /// Sends a MQTT disconnect to the eventloop
    pub fn try_disconnect(&mut self) -> Result<(), ClientError> {
        self.client.try_disconnect()?;
        Ok(())
    }

    /// Stops the eventloop right away
    pub fn cancel(&mut self) -> Result<(), ClientError> {
        pollster::block_on(self.client.cancel())?;
        Ok(())
    }
}

///  MQTT connection. Maintains all the necessary state
pub struct Connection {
    pub eventloop: EventLoop,
    runtime: Option<Runtime>,
}

impl Connection {
    fn new(eventloop: EventLoop, runtime: Runtime) -> Connection {
        Connection {
            eventloop,
            runtime: Some(runtime),
        }
    }

    /// Returns an iterator over this connection. Iterating over this is all that's
    /// necessary to make connection progress and maintain a robust connection.
    /// Just continuing to loop will reconnect
    /// **NOTE** Don't block this while iterating
    #[must_use = "Connection should be iterated over a loop to make progress"]
    pub fn iter(&mut self) -> Iter {
        let runtime = self.runtime.take().unwrap();
        Iter {
            connection: self,
            runtime,
        }
    }
}

/// Iterator which polls the eventloop for connection progress
pub struct Iter<'a> {
    connection: &'a mut Connection,
    runtime: runtime::Runtime,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Result<Event, ConnectionError>;

    fn next(&mut self) -> Option<Self::Item> {
        let f = self.connection.eventloop.poll();
        match self.runtime.block_on(f) {
            Ok(v) => Some(Ok(v)),
            // closing of request channel should stop the iterator
            Err(ConnectionError::RequestsDone) => {
                trace!("Done with requests");
                None
            }
            Err(ConnectionError::Cancel) => {
                trace!("Cancellation request received");
                None
            }
            Err(e) => Some(Err(e)),
        }
    }
}

impl<'a> Drop for Iter<'a> {
    fn drop(&mut self) {
        // TODO: Don't create new runtime in drop
        let runtime = runtime::Builder::new_current_thread().build().unwrap();
        self.connection.runtime = Some(mem::replace(&mut self.runtime, runtime));
    }
}
