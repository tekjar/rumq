use mqttbytes::v4::*;
use std::mem;

/// Watermarks for a given topic
#[derive(Debug)]
pub struct Acks {
    pending_acks_request: Option<()>,
    /// Committed packet ids for acks
    acks: Vec<Packet>,
}

impl Acks {
    pub fn new() -> Acks {
        Acks {
            pending_acks_request: None,
            acks: Vec::new(),
        }
    }

    pub fn handle_acks_request(&mut self) -> Option<Vec<Packet>> {
        let acks = self.acks();
        if acks.is_empty() {
            return None;
        }

        Some(acks)
    }

    pub fn register_pending_acks_request(&mut self) {
        self.pending_acks_request = Some(())
    }

    pub fn take_pending_acks_request(&mut self) -> Option<()> {
        self.pending_acks_request.take()
    }

    pub fn push_publish_ack(&mut self, pkid: u16, qos: u8) {
        match qos {
            1 => self.acks.push(Packet::PubAck(PubAck::new(pkid))),
            2 => self.acks.push(Packet::PubRec(PubRec::new(pkid))),
            _ => {}
        }
    }

    pub fn push_subscribe_ack(&mut self, pkid: u16, return_codes: Vec<SubscribeReasonCode>) {
        let suback = SubAck::new(pkid, return_codes);
        let suback = Packet::SubAck(suback);
        self.acks.push(suback)
    }

    pub fn push_unsubscribe_ack(&mut self, pkid: u16) {
        let unsuback = UnsubAck::new(pkid);
        let unsuback = Packet::UnsubAck(unsuback);
        self.acks.push(unsuback)
    }

    /// Returns committed acks by take
    pub fn acks(&mut self) -> Vec<Packet> {
        mem::take(&mut self.acks)
    }
}
