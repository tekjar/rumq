use super::*;
use alloc::string::String;
use alloc::vec::Vec;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Return code in connack
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum ConnectReturnCode {
    Success = 0,
    UnspecifiedError = 128,
    MalformedPacket = 129,
    ProtocolError = 130,
    ImplementationSpecificError = 131,
    UnsupportedProtocolVersion = 132,
    ClientIdentifierNotValid = 133,
    BadUserNamePassword = 134,
    NotAuthorized = 135,
    ServerUnavailable = 136,
    ServerBusy = 137,
    Banned = 138,
    BadAuthenticationMethod = 140,
    TopicNameInvalid = 144,
    PacketTooLarge = 149,
    QuotaExceeded = 151,
    PayloadFormatInvalid = 153,
    RetainNotSupported = 154,
    QoSNotSupported = 155,
    UseAnotherServer = 156,
    ServerMoved = 157,
    ConnectionRateExceeded = 159,
}

/// Acknowledgement to connect packet
#[derive(Debug, Clone, PartialEq)]
pub struct ConnAck {
    pub session_present: bool,
    pub code: ConnectReturnCode,
    pub properties: ConnAckProperties,
}

impl ConnAck {
    pub fn new(code: ConnectReturnCode, session_present: bool) -> ConnAck {
        ConnAck {
            code,
            session_present,
            properties: Default::default(),
        }
    }

    fn len(&self) -> usize {
        let mut len = 1  // session present
                        + 1; // code

        let properties_len = self.properties.len();
        let properties_len_len = len_len(properties_len);
        len += properties_len_len + properties_len;

        len
    }

    pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);

        let flags = read_u8(&mut bytes)?;
        let return_code = read_u8(&mut bytes)?;

        let session_present = (flags & 0x01) == 1;
        let code = connect_return(return_code)?;
        let connack = ConnAck {
            session_present,
            code,
            properties: ConnAckProperties::extract(&mut bytes)?,
        };

        Ok(connack)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        let len = self.len();
        buffer.put_u8(0x20);

        let count = write_remaining_length(buffer, len)?;
        buffer.put_u8(self.session_present as u8);
        buffer.put_u8(self.code as u8);

        self.properties.write(buffer)?;

        Ok(1 + count + len)
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct ConnAckProperties {
    pub session_expiry_interval: Option<u32>,
    pub receive_max: Option<u16>,
    pub max_qos: Option<u8>,
    pub retain_available: Option<u8>,
    pub max_packet_size: Option<u32>,
    pub assigned_client_identifier: Option<String>,
    pub topic_alias_max: Option<u16>,
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
    pub wildcard_subscription_available: Option<u8>,
    pub subscription_identifiers_available: Option<u8>,
    pub shared_subscription_available: Option<u8>,
    pub server_keep_alive: Option<u16>,
    pub response_information: Option<String>,
    pub server_reference: Option<String>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Bytes>,
}

trait PropLen {
    fn l(&self) -> usize;
}
impl PropLen for u8 {
    fn l(&self) -> usize {
        1
    }
}
impl PropLen for u16 {
    fn l(&self) -> usize {
        2
    }
}
impl PropLen for u32 {
    fn l(&self) -> usize {
        4
    }
}
impl PropLen for String {
    fn l(&self) -> usize {
        2 + self.len()
    }
}
impl PropLen for Bytes {
    fn l(&self) -> usize {
        2 + self.len()
    }
}
impl<T1: PropLen, T2: PropLen> PropLen for (T1, T2) {
    fn l(&self) -> usize {
        self.0.l() + self.1.l()
    }
}
impl<T: PropLen> PropLen for Option<T> {
    fn l(&self) -> usize {
        match self {
            Some(v) => 1 + v.l(),
            None => 0,
        }
    }
}
impl<T: PropLen> PropLen for Vec<T> {
    fn l(&self) -> usize {
        self.iter().map(|v| 1 + v.l()).sum()
    }
}

trait PropPut {
    fn put(&self, buffer: &mut BytesMut);
}
impl PropPut for u8 {
    fn put(&self, buffer: &mut BytesMut) {
        buffer.put_u8(*self)
    }
}
impl PropPut for u16 {
    fn put(&self, buffer: &mut BytesMut) {
        buffer.put_u16(*self)
    }
}
impl PropPut for u32 {
    fn put(&self, buffer: &mut BytesMut) {
        buffer.put_u32(*self)
    }
}
impl PropPut for String {
    fn put(&self, buffer: &mut BytesMut) {
        write_mqtt_string(buffer, self)
    }
}
impl PropPut for Bytes {
    fn put(&self, buffer: &mut BytesMut) {
        write_mqtt_bytes(buffer, self)
    }
}
impl<T1: PropPut, T2: PropPut> PropPut for (T1, T2) {
    fn put(&self, buffer: &mut BytesMut) {
        self.0.put(buffer);
        self.1.put(buffer);
    }
}
trait PropPutWrite {
    fn write(self, buffer: &mut BytesMut, property: PropertyType);
}
impl<T> PropPutWrite for &Option<T>
where
    T: PropPut,
{
    fn write(self, buffer: &mut BytesMut, property: PropertyType) {
        if let Some(v) = self {
            property.write(buffer);
            v.put(buffer);
        }
    }
}
impl<T: PropPut> PropPutWrite for &Vec<T> {
    fn write(self, buffer: &mut BytesMut, property: PropertyType) {
        for v in self {
            property.write(buffer);
            v.put(buffer);
        }
    }
}

impl ConnAckProperties {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.session_expiry_interval.l()
            + self.receive_max.l()
            + self.max_qos.l()
            + self.retain_available.l()
            + self.max_packet_size.l()
            + self.assigned_client_identifier.l()
            + self.topic_alias_max.l()
            + self.reason_string.l()
            + self.user_properties.l()
            + self.wildcard_subscription_available.l()
            + self.subscription_identifiers_available.l()
            + self.shared_subscription_available.l()
            + self.server_keep_alive.l()
            + self.response_information.l()
            + self.server_reference.l()
            + self.authentication_method.l()
            + self.authentication_data.l()
    }

    pub fn extract(bytes: &mut Bytes) -> Result<ConnAckProperties, Error> {
        let mut properties = Self::default();

        let (properties_len_len, properties_len) = length(bytes.iter())?;
        bytes.advance(properties_len_len);
        let bytes = &mut bytes.split_to(properties_len);

        while bytes.has_remaining() {
            match property(read_u8(bytes)?)? {
                PropertyType::SessionExpiryInterval => {
                    properties.session_expiry_interval = Some(read_u32(bytes)?)
                }
                PropertyType::ReceiveMaximum => properties.receive_max = Some(read_u16(bytes)?),
                PropertyType::MaximumQos => properties.max_qos = Some(read_u8(bytes)?),
                PropertyType::RetainAvailable => {
                    properties.retain_available = Some(read_u8(bytes)?)
                }
                PropertyType::AssignedClientIdentifier => {
                    properties.assigned_client_identifier = Some(read_mqtt_string(bytes)?)
                }
                PropertyType::MaximumPacketSize => {
                    properties.max_packet_size = Some(read_u32(bytes)?)
                }
                PropertyType::TopicAliasMaximum => {
                    properties.topic_alias_max = Some(read_u16(bytes)?)
                }
                PropertyType::ReasonString => {
                    properties.reason_string = Some(read_mqtt_string(bytes)?)
                }
                PropertyType::UserProperty => properties
                    .user_properties
                    .push((read_mqtt_string(bytes)?, read_mqtt_string(bytes)?)),
                PropertyType::WildcardSubscriptionAvailable => {
                    properties.wildcard_subscription_available = Some(read_u8(bytes)?)
                }
                PropertyType::SubscriptionIdentifierAvailable => {
                    properties.subscription_identifiers_available = Some(read_u8(bytes)?)
                }
                PropertyType::SharedSubscriptionAvailable => {
                    properties.shared_subscription_available = Some(read_u8(bytes)?)
                }
                PropertyType::ServerKeepAlive => {
                    properties.server_keep_alive = Some(read_u16(bytes)?)
                }
                PropertyType::ResponseInformation => {
                    properties.response_information = Some(read_mqtt_string(bytes)?)
                }
                PropertyType::ServerReference => {
                    properties.server_reference = Some(read_mqtt_string(bytes)?)
                }
                PropertyType::AuthenticationMethod => {
                    properties.authentication_method = Some(read_mqtt_string(bytes)?)
                }
                PropertyType::AuthenticationData => {
                    properties.authentication_data = Some(read_mqtt_bytes(bytes)?)
                }
                _ => return Err(Error::InvalidPropertyType(read_u8(bytes)?)),
            }
        }

        Ok(properties)
    }

    fn write(&self, buffer: &mut BytesMut) -> Result<(), Error> {
        let len = self.len();
        write_remaining_length(buffer, len)?;

        self.session_expiry_interval
            .write(buffer, PropertyType::SessionExpiryInterval);
        self.receive_max.write(buffer, PropertyType::ReceiveMaximum);
        self.max_qos.write(buffer, PropertyType::MaximumQos);
        self.retain_available
            .write(buffer, PropertyType::RetainAvailable);
        self.max_packet_size
            .write(buffer, PropertyType::MaximumPacketSize);
        self.assigned_client_identifier
            .write(buffer, PropertyType::AssignedClientIdentifier);
        self.topic_alias_max
            .write(buffer, PropertyType::TopicAliasMaximum);
        self.reason_string.write(buffer, PropertyType::ReasonString);
        self.user_properties
            .write(buffer, PropertyType::UserProperty);
        self.wildcard_subscription_available
            .write(buffer, PropertyType::WildcardSubscriptionAvailable);
        self.subscription_identifiers_available
            .write(buffer, PropertyType::SubscriptionIdentifierAvailable);
        self.shared_subscription_available
            .write(buffer, PropertyType::SharedSubscriptionAvailable);
        self.server_keep_alive
            .write(buffer, PropertyType::ServerKeepAlive);
        self.response_information
            .write(buffer, PropertyType::ResponseInformation);
        self.server_reference
            .write(buffer, PropertyType::ServerReference);
        self.authentication_method
            .write(buffer, PropertyType::AuthenticationMethod);
        self.authentication_data
            .write(buffer, PropertyType::AuthenticationData);
        Ok(())
    }
}

/// Connection return code type
fn connect_return(num: u8) -> Result<ConnectReturnCode, Error> {
    let code = match num {
        0 => ConnectReturnCode::Success,
        128 => ConnectReturnCode::UnspecifiedError,
        129 => ConnectReturnCode::MalformedPacket,
        130 => ConnectReturnCode::ProtocolError,
        131 => ConnectReturnCode::ImplementationSpecificError,
        132 => ConnectReturnCode::UnsupportedProtocolVersion,
        133 => ConnectReturnCode::ClientIdentifierNotValid,
        134 => ConnectReturnCode::BadUserNamePassword,
        135 => ConnectReturnCode::NotAuthorized,
        136 => ConnectReturnCode::ServerUnavailable,
        137 => ConnectReturnCode::ServerBusy,
        138 => ConnectReturnCode::Banned,
        140 => ConnectReturnCode::BadAuthenticationMethod,
        144 => ConnectReturnCode::TopicNameInvalid,
        149 => ConnectReturnCode::PacketTooLarge,
        151 => ConnectReturnCode::QuotaExceeded,
        153 => ConnectReturnCode::PayloadFormatInvalid,
        154 => ConnectReturnCode::RetainNotSupported,
        155 => ConnectReturnCode::QoSNotSupported,
        156 => ConnectReturnCode::UseAnotherServer,
        157 => ConnectReturnCode::ServerMoved,
        159 => ConnectReturnCode::ConnectionRateExceeded,
        num => return Err(Error::InvalidConnectReturnCode(num)),
    };

    Ok(code)
}

#[cfg(test)]
mod test {
    use super::*;
    use alloc::vec;
    use bytes::{Bytes, BytesMut};
    use pretty_assertions::assert_eq;

    fn sample() -> ConnAck {
        let properties = ConnAckProperties {
            session_expiry_interval: Some(1234),
            receive_max: Some(432),
            max_qos: Some(2),
            retain_available: Some(1),
            max_packet_size: Some(100),
            assigned_client_identifier: Some("test".to_owned()),
            topic_alias_max: Some(456),
            reason_string: Some("test".to_owned()),
            user_properties: vec![("test".to_owned(), "test".to_owned())],
            wildcard_subscription_available: Some(1),
            subscription_identifiers_available: Some(1),
            shared_subscription_available: Some(0),
            server_keep_alive: Some(1234),
            response_information: Some("test".to_owned()),
            server_reference: Some("test".to_owned()),
            authentication_method: Some("test".to_owned()),
            authentication_data: Some(Bytes::from(vec![1, 2, 3, 4])),
        };

        ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success,
            properties,
        }
    }

    fn sample_bytes() -> Vec<u8> {
        vec![
            0x20, // Packet type
            0x57, // Remaining length
            0x00, 0x00, // Session, code
            0x54, // Properties length
            0x11, 0x00, 0x00, 0x04, 0xd2, // Session expiry interval
            0x21, 0x01, 0xb0, // Receive maximum
            0x24, 0x02, // Maximum qos
            0x25, 0x01, // Retain available
            0x27, 0x00, 0x00, 0x00, 0x64, // Maximum packet size
            0x12, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // Assigned client identifier
            0x22, 0x01, 0xc8, // Topic alias max
            0x1f, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // Reason string
            0x26, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x04, 0x74, 0x65, 0x73,
            0x74, // user properties
            0x28, 0x01, // wildcard_subscription_available
            0x29, 0x01, // subscription_identifiers_available
            0x2a, 0x00, // shared_subscription_available
            0x13, 0x04, 0xd2, // server keep_alive
            0x1a, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // response_information
            0x1c, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // server reference
            0x15, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // authentication method
            0x16, 0x00, 0x04, 0x01, 0x02, 0x03, 0x04, // authentication data
        ]
    }

    #[test]
    fn connack_parsing_works() {
        let mut stream = bytes::BytesMut::new();
        let packetstream = &sample_bytes();
        stream.extend_from_slice(&packetstream[..]);

        let fixed_header = parse_fixed_header(stream.iter()).unwrap();
        let connack_bytes = stream.split_to(fixed_header.frame_length()).freeze();
        let connack = ConnAck::read(fixed_header, connack_bytes).unwrap();

        assert_eq!(connack, sample());
    }

    #[test]
    fn connack_encoding_works() {
        let connack = sample();
        let mut buf = BytesMut::new();
        connack.write(&mut buf).unwrap();
        assert_eq!(&buf[..], sample_bytes());
    }
}
