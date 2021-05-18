use crate::{Event, Incoming, Outgoing, Request, ds::*};

use bytes::BytesMut;
use mqttbytes::v4::*;
use mqttbytes::*;
use std::collections::VecDeque;
use std::{io, time::Instant};

/// Errors during state handling
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Io Error while state is passed to network
    #[error("Io error {0:?}")]
    Io(#[from] io::Error),
    /// Broker's error reply to client's connect packet
    #[error("Connect return code `{0:?}`")]
    Connect(ConnectReturnCode),
    /// Invalid state for a given operation
    #[error("Invalid state for a given operation")]
    InvalidState,
    /// Received a packet (ack) which isn't asked for
    #[error("Received a packet (ack) which isn't asked for")]
    Unsolicited(u16),
    /// Last pingreq isn't acked
    #[error("Last pingreq isn't acked")]
    AwaitPingResp,
    /// Received a wrong packet while waiting for another packet
    #[error("Received a wrong packet while waiting for another packet")]
    WrongPacket,
    /// Collision due to broker not acking in sequence
    #[error("Broker not acking in order. Packet id collision")]
    Collision(u16),
    #[error("Received paket id is out of bounds")]
    PacketIdOutOfBounds(u16),
    #[error("Timeout while waiting to resolve collision")]
    CollisionTimeout,
    #[error("Mqtt serialization/deserialization error")]
    Deserialization(mqttbytes::Error),
}

impl From<mqttbytes::Error> for StateError {
    fn from(e: mqttbytes::Error) -> StateError {
        StateError::Deserialization(e)
    }
}

impl From<OutOfBounds> for StateError {
    fn from(OutOfBounds(pkid): OutOfBounds) -> StateError {
        StateError::PacketIdOutOfBounds(pkid)
    }
}


/// State of the mqtt connection.
// Design: Methods will just modify the state of the object without doing any network operations
// Design: All inflight queues are maintained in a pre initialized vec with index as packet id.
// This is done for 2 reasons
// Bad acks or out of order acks aren't O(n) causing cpu spikes
// Any missing acks from the broker are detected during the next recycled use of packet ids
#[derive(Debug, Clone)]
pub struct MqttState {
    /// Status of last ping
    pub await_pingresp: bool,
    /// Collision ping count. Collisions stop user requests
    /// which inturn trigger pings. Multiple pings without
    /// resolving collisions will result in error
    pub collision_ping_count: usize,
    /// Last incoming packet time
    last_incoming: Instant,
    /// Last outgoing packet time
    last_outgoing: Instant,
    /// Packet id of the last outgoing packet
    pub(crate) last_pkid: u16,
    /// Maximum number of allowed inflight
    pub(crate) max_inflight: u16,
    /// Outgoing QoS 1, 2 publishes which aren't acked yet
    pub(crate) outgoing_pub: OutgoingPublishBucketList,
    /// Packet ids of released QoS 2 publishes
    pub(crate) outgoing_rel: PkidSet,
    /// Packet ids on incoming QoS 2 publishes
    pub(crate) incoming_pub: PkidSet,
    /// Last collision due to broker not acking in order
    pub collision: Option<Publish>,
    /// Buffered incoming packets
    pub events: VecDeque<Event>,
    /// Write buffer
    pub write: BytesMut,
}

impl MqttState {
    /// Creates new mqtt state. Same state should be used during a
    /// connection for persistent sessions while new state should
    /// instantiated for clean sessions
    pub fn new(max_inflight: u16) -> Self {
        MqttState {
            await_pingresp: false,
            collision_ping_count: 0,
            last_incoming: Instant::now(),
            last_outgoing: Instant::now(),
            last_pkid: 0,
            max_inflight,
            outgoing_pub: OutgoingPublishBucketList::with_limit(max_inflight),
            outgoing_rel: PkidSet::with_limit(max_inflight),
            incoming_pub: PkidSet::full_range(),
            collision: None,
            // TODO: Optimize these sizes later
            events: VecDeque::with_capacity(100),
            write: BytesMut::with_capacity(10 * 1024),
        }
    }

    /// Number of outgoing inflight publishes
    pub fn inflight(&self) -> usize {
        self.outgoing_pub.len() + self.outgoing_rel.len()
    }

    /// Returns inflight outgoing packets and clears internal queues
    pub fn clean(&mut self) -> Vec<Request> {
        let mut pending = Vec::with_capacity(self.outgoing_pub.len() + self.outgoing_rel.len());

        // remove and collect pending publishes
        self.outgoing_pub.drain_into(&mut pending, Request::Publish);

        // remove and collect pending releases
        self.outgoing_rel.drain_into(&mut pending, |pkid| Request::PubRel(PubRel::new(pkid)));

        // remove packed ids of incoming qos2 publishes
        self.incoming_pub.clear();

        self.await_pingresp = false;
        self.collision_ping_count = 0;
        pending
    }

    /// Consolidates handling of all outgoing mqtt packet logic. Returns a packet which should
    /// be put on to the network by the eventloop
    pub fn handle_outgoing_packet(&mut self, request: Request) -> Result<(), StateError> {
        match request {
            Request::Publish(publish) => self.outgoing_publish(publish)?,
            Request::PubRel(pubrel) => self.outgoing_pubrel(pubrel)?,
            Request::Subscribe(subscribe) => self.outgoing_subscribe(subscribe)?,
            Request::Unsubscribe(unsubscribe) => self.outgoing_unsubscribe(unsubscribe)?,
            Request::PingReq => self.outgoing_ping()?,
            Request::Disconnect => self.outgoing_disconnect()?,
            _ => unimplemented!(),
        };

        self.last_outgoing = Instant::now();
        Ok(())
    }

    /// Consolidates handling of all incoming mqtt packets. Returns a `Notification` which for the
    /// user to consume and `Packet` which for the eventloop to put on the network
    /// E.g For incoming QoS1 publish packet, this method returns (Publish, Puback). Publish packet will
    /// be forwarded to user and Pubck packet will be written to network
    pub fn handle_incoming_packet(&mut self, packet: Incoming) -> Result<(), StateError> {
        let out = match &packet {
            Incoming::PingResp => self.handle_incoming_pingresp(),
            Incoming::Publish(publish) => self.handle_incoming_publish(publish),
            Incoming::SubAck(_suback) => self.handle_incoming_suback(),
            Incoming::UnsubAck(_unsuback) => self.handle_incoming_unsuback(),
            Incoming::PubAck(puback) => self.handle_incoming_puback(puback),
            Incoming::PubRec(pubrec) => self.handle_incoming_pubrec(pubrec),
            Incoming::PubRel(pubrel) => self.handle_incoming_pubrel(pubrel),
            Incoming::PubComp(pubcomp) => self.handle_incoming_pubcomp(pubcomp),
            _ => {
                error!("Invalid incoming packet = {:?}", packet);
                return Err(StateError::WrongPacket);
            }
        };

        out?;
        self.events.push_back(Event::Incoming(packet));
        self.last_incoming = Instant::now();
        Ok(())
    }

    fn handle_incoming_suback(&mut self) -> Result<(), StateError> {
        Ok(())
    }

    fn handle_incoming_unsuback(&mut self) -> Result<(), StateError> {
        Ok(())
    }

    /// Results in a publish notification in all the QoS cases. Replys with an ack
    /// in case of QoS1 and Replys rec in case of QoS while also storing the message
    fn handle_incoming_publish(&mut self, publish: &Publish) -> Result<(), StateError> {
        let qos = publish.qos;

        match qos {
            QoS::AtMostOnce => Ok(()),
            QoS::AtLeastOnce => {
                let pkid = publish.pkid;
                PubAck::new(pkid).write(&mut self.write)?;

                Ok(())
            }
            QoS::ExactlyOnce => {
                let pkid = publish.pkid;
                PubRec::new(pkid).write(&mut self.write)?;
                let _ = self.incoming_pub.insert(pkid);
                Ok(())
            }
        }
    }

    fn handle_incoming_puback(&mut self, puback: &PubAck) -> Result<(), StateError> {
        let pkid = puback.pkid;

        if let Some(publish) = self.remove_collision(pkid) {
            debug_assert_eq!(publish.pkid, pkid);
            let err = publish.write(&mut self.write);
            let _ = self.outgoing_pub.insert(publish);
            err?; // Throw error after reinserting the `publish` into the `outgoing_pub`.
            let event = Event::Outgoing(Outgoing::Publish(pkid));
            self.events.push_back(event);
            self.collision_ping_count = 0;
        }

        match self.outgoing_pub.remove(pkid)? {
            Some(_) => {
                Ok(())
            }
            None => {
                error!("Unsolicited puback packet: {:?}", pkid);
                Err(StateError::Unsolicited(pkid))
            }
        }
    }

    fn handle_incoming_pubrec(&mut self, pubrec: &PubRec) -> Result<(), StateError> {
        match self.outgoing_pub.remove(pubrec.pkid)? {
            Some(_) => {
                // NOTE: Inflight - 1 for qos2 in comp
                let _ = self.outgoing_rel.insert(pubrec.pkid);
                PubRel::new(pubrec.pkid).write(&mut self.write)?;

                let event = Event::Outgoing(Outgoing::PubRel(pubrec.pkid));
                self.events.push_back(event);
                Ok(())
            }
            None => {
                error!("Unsolicited pubrec packet: {:?}", pubrec.pkid);
                Err(StateError::Unsolicited(pubrec.pkid))
            }
        }
    }

    fn handle_incoming_pubrel(&mut self, pubrel: &PubRel) -> Result<(), StateError> {
        let pkid = pubrel.pkid;
        let had_pkid = self.incoming_pub.remove(pkid)?;
        if had_pkid {
            PubComp::new(pkid).write(&mut self.write)?;
            let event = Event::Outgoing(Outgoing::PubComp(pkid));
            self.events.push_back(event);
            Ok(())
        }
        else {
            error!("Unsolicited pubrel packet: {:?}", pkid);
            Err(StateError::Unsolicited(pkid))
        }
    }

    fn handle_incoming_pubcomp(&mut self, pubcomp: &PubComp) -> Result<(), StateError> {
        let pkid = pubcomp.pkid;

        if let Some(publish) = self.remove_collision(pkid) {
            debug_assert_eq!(publish.pkid, pkid);
            let err = publish.write(&mut self.write);
            let _ = self.outgoing_pub.insert(publish);
            err?; // Throw error after reinserting the `publish` into the `outgoing_pub`.
            let event = Event::Outgoing(Outgoing::Publish(pkid));
            self.events.push_back(event);
            self.collision_ping_count = 0;
        }

        let had_pkid = self.outgoing_rel.remove(pkid)?;
        if had_pkid {
            Ok(())
        } else {
            error!("Unsolicited pubcomp packet: {:?}", pkid);
            Err(StateError::Unsolicited(pkid))
        }
    }

    fn handle_incoming_pingresp(&mut self) -> Result<(), StateError> {
        self.await_pingresp = false;
        Ok(())
    }

    /// Adds next packet identifier to QoS 1 and 2 publish packets and returns
    /// it buy wrapping publish in packet
    fn outgoing_publish(&mut self, mut publish: Publish) -> Result<(), StateError> {
        self.assign_pkid(&mut publish);

        debug!(
            "Publish. Topic = {}, Pkid = {:?}, Payload Size = {:?}",
            publish.topic,
            publish.pkid,
            publish.payload.len()
        );

        let pkid = publish.pkid;

        match publish.qos {
            QoS::AtMostOnce => {
                publish.write(&mut self.write)?;
            }
            QoS::AtLeastOnce | QoS::ExactlyOnce => {
                // if there is an existing publish at this pkid, this implies that client
                // hasn't acked this packet yet. `next_pkid()` rolls packet id back to 1
                // after a count of 'inflight' messages. this error is possible only when
                // client isn't acking sequentially
                match self.outgoing_pub.get(pkid)? {
                    Some(_) => {
                        warn!("collision on packet id = {:?}", pkid);
                        self.collision = Some(publish);
                        return Err(StateError::Collision(pkid));
                    }
                    None => {
                        // if there is an existing publish at this pkid, this implies that broker hasn't acked this
                        // packet yet. This error is possible only when broker isn't acking sequentially
                        let err = publish.write(&mut self.write);
                        let _ = self.outgoing_pub.insert(publish);
                        err?;
                    }
                }
            }
        };

        let event = Event::Outgoing(Outgoing::Publish(pkid));
        self.events.push_back(event);
        Ok(())
    }

    fn outgoing_pubrel(&mut self, pubrel: PubRel) -> Result<(), StateError> {
        let pubrel = self.save_pubrel(pubrel)?;

        debug!("Pubrel. Pkid = {}", pubrel.pkid);
        PubRel::new(pubrel.pkid).write(&mut self.write)?;

        let event = Event::Outgoing(Outgoing::PubRel(pubrel.pkid));
        self.events.push_back(event);
        Ok(())
    }

    /// check when the last control packet/pingreq packet is received and return
    /// the status which tells if keep alive time has exceeded
    /// NOTE: status will be checked for zero keepalive times also
    fn outgoing_ping(&mut self) -> Result<(), StateError> {
        let elapsed_in = self.last_incoming.elapsed();
        let elapsed_out = self.last_outgoing.elapsed();

        if self.collision.is_some() {
            self.collision_ping_count += 1;
            if self.collision_ping_count >= 2 {
                return Err(StateError::CollisionTimeout);
            }
        }

        // raise error if last ping didn't receive ack
        if self.await_pingresp {
            return Err(StateError::AwaitPingResp);
        }

        self.await_pingresp = true;

        debug!(
            "Pingreq,
            last incoming packet before {} millisecs,
            last outgoing request before {} millisecs",
            elapsed_in.as_millis(),
            elapsed_out.as_millis()
        );

        PingReq.write(&mut self.write)?;
        let event = Event::Outgoing(Outgoing::PingReq);
        self.events.push_back(event);
        Ok(())
    }

    fn outgoing_subscribe(&mut self, mut subscription: Subscribe) -> Result<(), StateError> {
        let pkid = self.next_pkid();
        subscription.pkid = pkid;

        debug!(
            "Subscribe. Topics = {:?}, Pkid = {:?}",
            subscription.filters, subscription.pkid
        );

        subscription.write(&mut self.write)?;
        let event = Event::Outgoing(Outgoing::Subscribe(subscription.pkid));
        self.events.push_back(event);
        Ok(())
    }

    fn outgoing_unsubscribe(&mut self, mut unsub: Unsubscribe) -> Result<(), StateError> {
        let pkid = self.next_pkid();
        unsub.pkid = pkid;

        debug!(
            "Unsubscribe. Topics = {:?}, Pkid = {:?}",
            unsub.topics, unsub.pkid
        );

        unsub.write(&mut self.write)?;
        let event = Event::Outgoing(Outgoing::Unsubscribe(unsub.pkid));
        self.events.push_back(event);
        Ok(())
    }

    fn outgoing_disconnect(&mut self) -> Result<(), StateError> {
        debug!("Disconnect");

        Disconnect.write(&mut self.write)?;
        let event = Event::Outgoing(Outgoing::Disconnect);
        self.events.push_back(event);
        Ok(())
    }

    /// If there was a collision with `pkid`, remove it both from `self.collision` and from
    /// `outgoing_pub`.
    fn remove_collision(&mut self, pkid: u16) -> Option<Publish> {
        if let Some(publish) = &self.collision {
            // remove acked, previously collided packet from the state
            if publish.pkid == pkid {
                self.collision.take().unwrap();
                return self.outgoing_pub.remove(pkid).unwrap_or(None);
            }
        }

        None
    }

    fn save_pubrel(&mut self, mut pubrel: PubRel) -> Result<PubRel, StateError> {
        let pubrel = match pubrel.pkid {
            // consider PacketIdentifier(0) as uninitialized packets
            0 => {
                pubrel.pkid = self.next_pkid();
                pubrel
            }
            _ => pubrel,
        };

        let _ = self.outgoing_rel.insert(pubrel.pkid);
        Ok(pubrel)
    }

    /// Ensures the `publish` has a valid `pkid` set.
    ///
    /// NOTE: The spec requires `pkid` to be `0` in case of `QoS::AtMostOnce` and non-zero in all
    /// other cases.
    fn assign_pkid(&mut self, publish: &mut Publish) {
        match publish.qos {
            QoS::AtMostOnce => {
                publish.pkid = 0;
            }
            QoS::AtLeastOnce | QoS::ExactlyOnce => {
                // consider PacketIdentifier(0) as uninitialized packets
                if publish.pkid == 0 {
                    publish.pkid = self.next_pkid();
                }
            }
        }
    }

    /// http://stackoverflow.com/questions/11115364/mqtt-messageid-practical-implementation
    /// Packet ids are incremented till maximum set inflight messages and reset to 1 after that.
    ///
    fn next_pkid(&mut self) -> u16 {
        let next_pkid = self.last_pkid + 1;

        // When next packet id is at the edge of inflight queue,
        // set await flag. This instructs eventloop to stop
        // processing requests until all the inflight publishes
        // are acked
        if next_pkid == self.max_inflight {
            self.last_pkid = 0;
            return next_pkid;
        }

        self.last_pkid = next_pkid;
        next_pkid
    }
}

#[cfg(test)]
mod test {
    use super::{MqttState, StateError};
    use crate::{Incoming, MqttOptions, Request};
    use mqttbytes::v4::*;
    use mqttbytes::*;

    fn build_outgoing_publish(qos: QoS) -> Publish {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
        publish.qos = qos;
        publish
    }

    fn build_incoming_publish(qos: QoS, pkid: u16) -> Publish {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
        publish.pkid = pkid;
        publish.qos = qos;
        publish
    }

    fn build_mqttstate() -> MqttState {
        MqttState::new(100)
    }

    #[test]
    fn next_pkid_increments_as_expected() {
        let mut mqtt = build_mqttstate();

        for i in 1..=100 {
            let pkid = mqtt.next_pkid();

            // loops between 0-99. % 100 == 0 implies border
            let expected = i % 100;
            if expected == 0 {
                break;
            }

            assert_eq!(expected, pkid);
        }
    }

    #[test]
    fn outgoing_publish_should_set_pkid_and_add_publish_to_queue() {
        let mut mqtt = build_mqttstate();

        // QoS0 Publish
        let publish = build_outgoing_publish(QoS::AtMostOnce);

        // QoS 0 publish shouldn't be saved in queue
        mqtt.outgoing_publish(publish).unwrap();
        assert_eq!(mqtt.last_pkid, 0);
        assert_eq!(mqtt.inflight(), 0);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::AtLeastOnce);

        // Packet id should be set and publish should be saved in queue
        mqtt.outgoing_publish(publish.clone()).unwrap();
        assert_eq!(mqtt.last_pkid, 1);
        assert_eq!(mqtt.inflight(), 1);

        // Packet id should be incremented and publish should be saved in queue
        mqtt.outgoing_publish(publish).unwrap();
        assert_eq!(mqtt.last_pkid, 2);
        assert_eq!(mqtt.inflight(), 2);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        // Packet id should be set and publish should be saved in queue
        mqtt.outgoing_publish(publish.clone()).unwrap();
        assert_eq!(mqtt.last_pkid, 3);
        assert_eq!(mqtt.inflight(), 3);

        // Packet id should be incremented and publish should be saved in queue
        mqtt.outgoing_publish(publish).unwrap();
        assert_eq!(mqtt.last_pkid, 4);
        assert_eq!(mqtt.inflight(), 4);
    }

    #[test]
    fn outgoing_publish_should_zero_pkid_in_case_of_qos0() {
        let mut mqtt = build_mqttstate();

        // QoS0 Publish with pkid `42`
        let mut publish = build_outgoing_publish(QoS::AtMostOnce);
        publish.pkid = 42;

        // QoS 0 publish shouldn't be saved in queue
        mqtt.outgoing_publish(publish).unwrap();
        assert_eq!(mqtt.last_pkid, 0);

        if let Some(super::Event::Outgoing(super::Outgoing::Publish(pkid))) = mqtt.events.pop_back() {
            assert_eq!(pkid, 0);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn incoming_publish_should_be_added_to_queue_correctly() {
        let mut mqtt = build_mqttstate();

        // QoS0, 1, 2 Publishes
        let publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&publish1).unwrap();
        mqtt.handle_incoming_publish(&publish2).unwrap();
        mqtt.handle_incoming_publish(&publish3).unwrap();

        // only qos2 publish should be add to queue
        assert!(mqtt.incoming_pub.contains(3));
    }

    #[test]
    fn incoming_qos2_publish_should_send_rec_to_network_and_publish_to_user() {
        let mut mqtt = build_mqttstate();
        let publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        mqtt.handle_incoming_publish(&publish).unwrap();
        let packet = read(&mut mqtt.write, 10 * 1024).unwrap();
        match packet {
            Packet::PubRec(pubrec) => assert_eq!(pubrec.pkid, 1),
            _ => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_puback_should_remove_correct_publish_from_queue() {
        let mut mqtt = build_mqttstate();

        let publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let publish2 = build_outgoing_publish(QoS::ExactlyOnce);

        mqtt.outgoing_publish(publish1).unwrap();
        mqtt.outgoing_publish(publish2).unwrap();
        assert_eq!(mqtt.inflight(), 2);

        mqtt.handle_incoming_puback(&PubAck::new(1)).unwrap();
        assert_eq!(mqtt.inflight(), 1);

        mqtt.handle_incoming_puback(&PubAck::new(2)).unwrap();
        assert_eq!(mqtt.inflight(), 0);

        assert!(mqtt.outgoing_pub.get(1).unwrap().is_none());
        assert!(mqtt.outgoing_pub.get(2).unwrap().is_none());
    }

    #[test]
    fn incoming_pubrec_should_release_publish_from_queue_and_add_relid_to_rel_queue() {
        let mut mqtt = build_mqttstate();

        let publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let publish2 = build_outgoing_publish(QoS::ExactlyOnce);

        let _publish_out = mqtt.outgoing_publish(publish1);
        let _publish_out = mqtt.outgoing_publish(publish2);

        mqtt.handle_incoming_pubrec(&PubRec::new(2)).unwrap();
        assert_eq!(mqtt.inflight(), 2);

        // check if the remaining element's pkid is 1
        let backup = mqtt.outgoing_pub.get(1).unwrap().clone();
        assert_eq!(backup.unwrap().pkid, 1);

        // check if the qos2 element's release pkid is 2
        assert!(mqtt.outgoing_rel.contains(2));
    }

    #[test]
    fn incoming_pubrec_should_send_release_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();

        let publish = build_outgoing_publish(QoS::ExactlyOnce);
        mqtt.outgoing_publish(publish).unwrap();
        let packet = read(&mut mqtt.write, 10 * 1024).unwrap();
        match packet {
            Packet::Publish(publish) => assert_eq!(publish.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }

        mqtt.handle_incoming_pubrec(&PubRec::new(1)).unwrap();
        let packet = read(&mut mqtt.write, 10 * 1024).unwrap();
        match packet {
            Packet::PubRel(pubrel) => assert_eq!(pubrel.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_pubrel_should_send_comp_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();
        let publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        mqtt.handle_incoming_publish(&publish).unwrap();
        let packet = read(&mut mqtt.write, 10 * 1024).unwrap();
        match packet {
            Packet::PubRec(pubrec) => assert_eq!(pubrec.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }

        mqtt.handle_incoming_pubrel(&PubRel::new(1)).unwrap();
        let packet = read(&mut mqtt.write, 10 * 1024).unwrap();
        match packet {
            Packet::PubComp(pubcomp) => assert_eq!(pubcomp.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_pubcomp_should_release_correct_pkid_from_release_queue() {
        let mut mqtt = build_mqttstate();
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        mqtt.outgoing_publish(publish).unwrap();
        mqtt.handle_incoming_pubrec(&PubRec::new(1)).unwrap();

        mqtt.handle_incoming_pubcomp(&PubComp::new(1)).unwrap();
        assert_eq!(mqtt.inflight(), 0);
    }

    #[test]
    fn outgoing_ping_handle_should_throw_errors_for_no_pingresp() {
        let mut mqtt = build_mqttstate();
        let mut opts = MqttOptions::new("test", "localhost", 1883);
        opts.set_keep_alive(10);
        mqtt.outgoing_ping().unwrap();

        // network activity other than pingresp
        let publish = build_outgoing_publish(QoS::AtLeastOnce);
        mqtt.handle_outgoing_packet(Request::Publish(publish))
            .unwrap();
        mqtt.handle_incoming_packet(Incoming::PubAck(PubAck::new(1)))
            .unwrap();

        // should throw error because we didn't get pingresp for previous ping
        match mqtt.outgoing_ping() {
            Ok(_) => panic!("Should throw pingresp await error"),
            Err(StateError::AwaitPingResp) => (),
            Err(e) => panic!("Should throw pingresp await error. Error = {:?}", e),
        }
    }

    #[test]
    fn outgoing_ping_handle_should_succeed_if_pingresp_is_received() {
        let mut mqtt = build_mqttstate();

        let mut opts = MqttOptions::new("test", "localhost", 1883);
        opts.set_keep_alive(10);

        // should ping
        mqtt.outgoing_ping().unwrap();
        mqtt.handle_incoming_packet(Incoming::PingResp).unwrap();

        // should ping
        mqtt.outgoing_ping().unwrap();
    }

    #[test]
    fn incoming_puback_with_pkid_exceeding_max_inflight_should_not_panic() {
        let max_inflight = 100;
        let mut mqtt = MqttState::new(max_inflight);
        let _ = mqtt.handle_incoming_packet(Incoming::PubAck(PubAck::new(max_inflight + 1)));
    }

    #[test]
    fn incoming_pubrec_with_pkid_exceeding_max_inflight_should_not_panic() {
        let max_inflight = 100;
        let mut mqtt = MqttState::new(max_inflight);
        let _ = mqtt.handle_incoming_packet(Incoming::PubRec(PubRec::new(max_inflight + 1)));
    }

    #[test]
    fn incoming_pubrel_with_pkid_exceeding_max_inflight_should_not_panic() {
        let max_inflight = 100;
        let mut mqtt = MqttState::new(max_inflight);
        let _ = mqtt.handle_incoming_packet(Incoming::PubRel(PubRel::new(max_inflight + 1)));
    }

    #[test]
    fn incoming_pubcomp_with_pkid_exceeding_max_inflight_should_not_panic() {
        let max_inflight = 100;
        let mut mqtt = MqttState::new(max_inflight);
        let _ = mqtt.handle_incoming_packet(Incoming::PubComp(PubComp::new(max_inflight + 1)));
    }
}
