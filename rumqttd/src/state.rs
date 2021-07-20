use mqttbytes::v4::*;
use mqttbytes::*;
use rumqttlog::{Message, Notification};

use bytes::{Bytes, BytesMut};
use std::mem;
use std::vec::IntoIter;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Received unsolicited ack from the device. {0}")]
    Unsolicited(u16),
    #[error("Collision with an unacked packet")]
    Serialization(mqttbytes::Error),
    #[error("Collision with an unacked packet")]
    Collision,
    #[error("Duplicate connect")]
    DuplicateConnect,
    #[error("Client connack")]
    ClientConnAck,
    #[error("Client disconnect")]
    Disconnect,
}

impl From<mqttbytes::Error> for Error {
    fn from(e: mqttbytes::Error) -> Error {
        Error::Serialization(e)
    }
}

#[derive(Debug)]
struct Pending {
    topic: String,
    qos: QoS,
    payload: IntoIter<Bytes>,
    collision: Option<Publish>,
}

impl Pending {
    pub fn empty() -> Pending {
        Pending {
            topic: "".to_string(),
            qos: QoS::AtMostOnce,
            payload: vec![].into_iter(),
            collision: None,
        }
    }

    pub fn new(topic: String, qos: QoS, payload: IntoIter<Bytes>) -> Pending {
        Pending {
            topic,
            qos,
            payload,
            collision: None,
        }
    }

    pub fn len(&self) -> usize {
        let len = self.collision.as_ref().map_or(0, |_| 1);
        len + self.payload.len()
    }

    pub fn next(&mut self) -> Option<Bytes> {
        self.payload.next()
    }
}

/// State of the mqtt connection.
/// Design: Methods will just modify the state of the object without doing any network operations
/// Design: All inflight queues are maintained in a pre initialized vec with index as packet id.
/// This is done for 2 reasons
/// Bad acks or out of order acks aren't O(n) causing cpu spikes
/// Any missing acks from the broker are detected during the next recycled use of packet ids
#[derive(Debug)]
pub struct State {
    /// Packet id of the last outgoing packet
    last_pkid: u16,
    /// Number of outgoing inflight publishes
    inflight: u16,
    /// Maximum number of allowed inflight
    max_inflight: u16,
    /// Outgoing QoS 1, 2 publishes which aren't acked yet
    outgoing_pub: Vec<Option<Publish>>,
    /// Packet ids of released QoS 2 publishes
    outgoing_rel: Vec<Option<u16>>,
    /// Packet ids on incoming QoS 2 publishes
    incoming_pub: Vec<Option<u16>>,
    /// Pending publishes due to collision
    pending: Pending,
    /// Collected incoming packets
    pub incoming: Vec<Packet>,
    /// Write buffer
    write: BytesMut,
}

impl State {
    /// Creates new mqtt state. Same state should be used during a
    /// connection for persistent sessions while new state should
    /// instantiated for clean sessions
    pub fn new(max_inflight: u16) -> Self {
        State {
            last_pkid: 0,
            inflight: 0,
            max_inflight,
            // index 0 is wasted as 0 is not a valid packet id
            outgoing_pub: vec![None; max_inflight as usize + 1],
            outgoing_rel: vec![None; max_inflight as usize + 1],
            incoming_pub: vec![None; std::u16::MAX as usize + 1],
            pending: Pending::empty(),
            incoming: Vec::with_capacity(10),
            write: BytesMut::with_capacity(10 * 1024),
        }
    }

    pub fn pause_outgoing(&self) -> bool {
        self.inflight > self.max_inflight || self.pending.len() > 0
    }

    pub fn take_incoming(&mut self) -> Vec<Packet> {
        mem::replace(&mut self.incoming, Vec::with_capacity(10))
    }

    pub fn write_mut(&mut self) -> &mut BytesMut {
        &mut self.write
    }

    /// Returns inflight outgoing packets and clears internal queues
    pub fn clean(&mut self) -> Vec<Notification> {
        let mut pending = Vec::new();
        let mut acks = Vec::new();

        // remove and collect pending releases
        for rel in self.outgoing_rel.iter_mut() {
            if let Some(pkid) = rel.take() {
                let packet = Packet::PubRel(PubRel::new(pkid));
                acks.push(packet);
            }
        }

        pending.push(Notification::Acks(acks));

        // remove and collect pending publishes
        for publish in self.outgoing_pub.iter_mut() {
            if let Some(publish) = publish.take() {
                let message = Message::new(publish.topic, publish.qos as u8, publish.payload);
                pending.push(Notification::Message(message));
            }
        }

        pending
    }

    pub fn add_pending(&mut self, topic: String, qos: QoS, data: Vec<Bytes>) {
        self.pending = Pending::new(topic, qos, data.into_iter());
    }

    /// Adds next packet identifier to QoS 1 and 2 publish packets.
    /// Pending packets collide at the first unacked packet id.
    /// Processing pending publishes stops at this point. Eventloop
    /// waits for incoming acks to clear `pause_outgoing` flag and
    /// process more outgoing packets
    pub(crate) fn write_pending(&mut self) -> Result<(), Error> {
        while let Some(payload) = self.pending.next() {
            let mut publish = Publish::from_bytes(&self.pending.topic, self.pending.qos, payload);

            if let QoS::AtMostOnce = publish.qos {
                debug!("Publish. Qos 0. Payload size = {:?}", publish.payload.len());
                publish.write(&mut self.write)?;
                return Ok(());
            };

            // consider PacketIdentifier(0) as uninitialized packets
            if 0 == publish.pkid {
                publish.pkid = self.next_pkid();
            };

            let pkid = publish.pkid as usize;
            debug!("Publish. Pkid = {}, Size = {}", pkid, publish.payload.len());

            // If there is an existing publish at this pkid, this implies
            // that client hasn't sent ack packet yet.
            match self.outgoing_pub.get(pkid).unwrap() {
                Some(_) => {
                    info!(
                        "Collision on packet id = {:?}. Inflight = {}",
                        publish.pkid, self.inflight
                    );
                    self.pending.collision = Some(publish);
                    return Ok(());
                }
                None => {
                    publish.write(&mut self.write)?;
                    self.outgoing_pub[pkid] = Some(publish);
                    self.inflight += 1;
                }
            }
        }

        Ok(())
    }

    pub fn outgoing_ack(&mut self, ack: Packet) -> Result<(), Error> {
        match ack {
            Packet::PubAck(ack) => ack.write(&mut self.write),
            Packet::PubRec(ack) => ack.write(&mut self.write),
            // Pending outgoing release given by router. Replay
            // pubrec with these pkids. In normal flow, release
            // id a response for incoming pubrec
            Packet::PubRel(ack) => {
                self.handle_incoming_pubrec(&PubRec::new(ack.pkid))?;
                Ok(10)
            }
            Packet::PubComp(ack) => ack.write(&mut self.write),
            Packet::SubAck(ack) => ack.write(&mut self.write),
            Packet::UnsubAck(ack) => ack.write(&mut self.write),
            _ => unimplemented!(),
        }
        .map_err(Error::Serialization)?;
        Ok(())
    }

    pub fn handle_network_data(&mut self, packet: Packet) -> Result<bool, Error> {
        match packet {
            Packet::Connect(_) => return Err(Error::DuplicateConnect),
            Packet::ConnAck(_) => return Err(Error::ClientConnAck),
            Packet::Publish(publish) => {
                let forward = self.handle_incoming_publish(&publish)?;
                if forward {
                    self.incoming.push(Packet::Publish(publish));
                }
            }
            Packet::Subscribe(subscribe) => {
                self.incoming.push(Packet::Subscribe(subscribe));
            }
            Packet::Unsubscribe(unsubscribe) => {
                self.incoming.push(Packet::Unsubscribe(unsubscribe));
            }
            Packet::PubAck(ack) => {
                self.handle_incoming_puback(&ack)?;
            }
            Packet::PubRel(ack) => {
                self.handle_incoming_pubrel(&ack)?;
            }
            Packet::PubRec(ack) => {
                self.handle_incoming_pubrec(&ack)?;
            }
            Packet::PubComp(ack) => {
                self.handle_incoming_pubcomp(&ack)?;
            }
            Packet::PingReq => {
                self.handle_incoming_pingreq()?;
            }
            Packet::Disconnect => return Ok(true),
            packet => {
                error!("Packet = {:?} not supported yet", packet);
                // return Err(Error::UnsupportedPacket(packet))
            }
        }

        Ok(false)
    }

    /// Filters duplicate qos 2 publishes and returns true if publish should be forwarded
    /// to the router.
    pub fn handle_incoming_publish(&mut self, publish: &Publish) -> Result<bool, Error> {
        if publish.qos == QoS::ExactlyOnce {
            let pkid = publish.pkid;

            // If publish packet is already recorded before, this is a duplicate
            // qos 2 publish which should be filtered here.
            if mem::replace(&mut self.incoming_pub[pkid as usize], Some(pkid)).is_some() {
                PubRec::new(pkid).write(&mut self.write)?;
                return Ok(false);
            }
        }

        Ok(true)
    }

    pub fn handle_incoming_puback(&mut self, puback: &PubAck) -> Result<(), Error> {
        match mem::replace(&mut self.outgoing_pub[puback.pkid as usize], None) {
            Some(_) => self.inflight -= 1,
            None => {
                error!("Unsolicited puback packet: {:?}", puback.pkid);
                return Err(Error::Unsolicited(puback.pkid));
            }
        }

        // Check if there is a collision on this packet id before and write it
        self.check_and_resolve_collision(puback.pkid)?;

        // Try writing all the previous pending packets because of collision
        self.write_pending()?;
        Ok(())
    }

    pub fn handle_incoming_pubrec(&mut self, pubrec: &PubRec) -> Result<(), Error> {
        match mem::replace(&mut self.outgoing_pub[pubrec.pkid as usize], None) {
            // NOTE: Inflight - 1 for qos2 in comp
            Some(_) => {
                self.outgoing_rel[pubrec.pkid as usize] = Some(pubrec.pkid);
                PubRel::new(pubrec.pkid).write(&mut self.write)?;
                Ok(())
            }
            None => {
                error!("Unsolicited pubrec packet: {:?}", pubrec.pkid);
                Err(Error::Unsolicited(pubrec.pkid))
            }
        }
    }

    pub fn handle_incoming_pubrel(&mut self, pubrel: &PubRel) -> Result<(), Error> {
        match mem::replace(&mut self.incoming_pub[pubrel.pkid as usize], None) {
            Some(_) => {
                let response = PubComp::new(pubrel.pkid);
                response.write(&mut self.write)?;
                Ok(())
            }
            None => {
                error!("Unsolicited pubrel packet: {:?}", pubrel.pkid);
                Err(Error::Unsolicited(pubrel.pkid))
            }
        }
    }

    pub fn handle_incoming_pubcomp(&mut self, pubcomp: &PubComp) -> Result<(), Error> {
        match mem::replace(&mut self.outgoing_rel[pubcomp.pkid as usize], None) {
            Some(_) => {
                self.inflight -= 1;
            }
            None => {
                error!("Unsolicited pubcomp packet: {:?}", pubcomp.pkid);
                return Err(Error::Unsolicited(pubcomp.pkid));
            }
        }

        // Check if there is a collision on this packet id before and write it
        self.check_and_resolve_collision(pubcomp.pkid)?;

        // Try writing all the previous pending packets because of collision
        self.write_pending()?;
        Ok(())
    }

    pub fn handle_incoming_pingreq(&mut self) -> Result<(), Error> {
        PingResp.write(&mut self.write)?;
        Ok(())
    }

    fn check_and_resolve_collision(&mut self, pkid: u16) -> Result<(), Error> {
        if let Some(publish) = &self.pending.collision {
            if publish.pkid == pkid {
                let publish = self.pending.collision.take().unwrap();
                publish.write(&mut self.write)?;
                self.outgoing_pub[pkid as usize] = Some(publish);
                self.inflight += 1;
                info!(
                    "Resolving collision on packet id = {:?}. Inflight = {}",
                    pkid, self.inflight
                );
            }
        }

        Ok(())
    }

    fn next_pkid(&mut self) -> u16 {
        let next_pkid = self.last_pkid + 1;
        if next_pkid == self.max_inflight {
            self.last_pkid = 0;
            return next_pkid;
        }

        self.last_pkid = next_pkid;
        next_pkid
    }
}
