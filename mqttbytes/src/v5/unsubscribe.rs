use super::*;
use alloc::string::String;
use alloc::vec::Vec;
use bytes::{Buf, Bytes};

/// Unsubscribe packet
#[derive(Debug, Clone, PartialEq)]
pub struct Unsubscribe {
    pub pkid: u16,
    pub filters: Vec<String>,
    pub properties: Option<UnsubscribeProperties>,
}

impl Unsubscribe {
    pub fn new<S: Into<String>>(topic: S) -> Unsubscribe {
        Unsubscribe {
            pkid: 0,
            filters: vec![topic.into()],
            properties: None,
        }
    }

    pub fn len(&self) -> usize {
        // Packet id + length of filters (unlike subscribe, this just a string.
        // Hence 2 is prefixed for len per filter)
        let mut len = 2 + self.filters.iter().fold(0, |s, t| 2 + s + t.len());

        if let Some(properties) = &self.properties {
            let properties_len = properties.len();
            let properties_len_len = len_len(properties_len);
            len += properties_len_len + properties_len;
        } else {
            // just 1 byte representing 0 len
            len += 1;
        }

        len
    }

    pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);

        let pkid = read_u16(&mut bytes)?;
        dbg!(pkid);
        let properties = UnsubscribeProperties::extract(&mut bytes)?;

        let mut filters = Vec::with_capacity(1);
        while bytes.has_remaining() {
            let filter = read_mqtt_string(&mut bytes)?;
            filters.push(filter);
        }

        let unsubscribe = Unsubscribe {
            pkid,
            filters,
            properties,
        };
        Ok(unsubscribe)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        buffer.put_u8(0xA2);

        // write remaining length
        let remaining_len = self.len();
        let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;

        // write packet id
        buffer.put_u16(self.pkid);

        match &self.properties {
            Some(properties) => properties.write(buffer)?,
            None => {
                write_remaining_length(buffer, 0)?;
            }
        };

        // write filters
        for filter in self.filters.iter() {
            write_mqtt_string(buffer, filter);
        }

        Ok(1 + remaining_len_bytes + remaining_len)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnsubscribeProperties {
    pub user_properties: Vec<(String, String)>,
}

impl UnsubscribeProperties {
    fn len(&self) -> usize {
        let mut len = 0;

        for (key, value) in self.user_properties.iter() {
            len += 1 + 2 + key.len() + 2 + value.len();
        }

        len
    }

    fn extract(mut bytes: &mut Bytes) -> Result<Option<UnsubscribeProperties>, Error> {
        let mut user_properties = Vec::new();

        let (properties_len_len, properties_len) = length(bytes.iter())?;
        bytes.advance(properties_len_len);

        if properties_len == 0 {
            return Ok(None);
        }

        let mut cursor = 0;
        // read until cursor reaches property length. properties_len = 0 will skip this loop
        while cursor < properties_len {
            let prop = read_u8(&mut bytes)?;
            cursor += 1;

            match property(prop)? {
                PropertyType::UserProperty => {
                    let key = read_mqtt_string(&mut bytes)?;
                    let value = read_mqtt_string(&mut bytes)?;
                    cursor += 2 + key.len() + 2 + value.len();
                    user_properties.push((key, value));
                }
                _ => return Err(Error::InvalidPropertyType(prop)),
            }
        }

        Ok(Some(UnsubscribeProperties { user_properties }))
    }

    fn write(&self, buffer: &mut BytesMut) -> Result<(), Error> {
        let len = self.len();
        write_remaining_length(buffer, len)?;

        for (key, value) in self.user_properties.iter() {
            buffer.put_u8(PropertyType::UserProperty as u8);
            write_mqtt_string(buffer, key);
            write_mqtt_string(buffer, value);
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use alloc::vec;
    use bytes::BytesMut;
    use pretty_assertions::assert_eq;

    fn sample() -> Unsubscribe {
        let properties = UnsubscribeProperties {
            user_properties: vec![("test".to_owned(), "test".to_owned())],
        };

        Unsubscribe {
            pkid: 10,
            filters: vec!["hello".to_owned(), "world".to_owned()],
            properties: Some(properties),
        }
    }

    fn sample_bytes() -> Vec<u8> {
        vec![
            0xa2, // packet type
            0x1e, // remaining len
            0x00, 0x0a, // pkid
            0x0d, // properties len
            0x26, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x04, 0x74, 0x65, 0x73,
            0x74, // user properties
            0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, // filter 1
            0x00, 0x05, 0x77, 0x6f, 0x72, 0x6c, 0x64, // filter 2
        ]
    }

    #[test]
    fn unsubscribe_parsing_works() {
        let mut stream = BytesMut::new();
        let packetstream = &sample_bytes();

        stream.extend_from_slice(&packetstream[..]);

        let fixed_header = parse_fixed_header(stream.iter()).unwrap();
        let unsubscribe_bytes = stream.split_to(fixed_header.frame_length()).freeze();
        let unsubscribe = Unsubscribe::read(fixed_header, unsubscribe_bytes).unwrap();
        assert_eq!(unsubscribe, sample());
    }

    #[test]
    fn subscribe_encoding_works() {
        let publish = sample();
        let mut buf = BytesMut::new();
        publish.write(&mut buf).unwrap();

        println!("{:X?}", buf);
        println!("{:#04X?}", &buf[..]);
        assert_eq!(&buf[..], sample_bytes());
    }

    fn sample2() -> Unsubscribe {
        Unsubscribe {
            pkid: 10,
            filters: vec!["hello".to_owned()],
            properties: None,
        }
    }

    fn sample2_bytes() -> Vec<u8> {
        vec![
            0xa2, 0x0a, 0x00, 0x0a, 0x00, 0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
        ]
    }

    #[test]
    fn subscribe2_parsing_works() {
        let mut stream = BytesMut::new();
        let packetstream = &sample2_bytes();

        stream.extend_from_slice(&packetstream[..]);

        let fixed_header = parse_fixed_header(stream.iter()).unwrap();
        let subscribe_bytes = stream.split_to(fixed_header.frame_length()).freeze();
        let subscribe = Unsubscribe::read(fixed_header, subscribe_bytes).unwrap();
        assert_eq!(subscribe, sample2());
    }

    #[test]
    fn subscribe2_encoding_works() {
        let publish = sample2();
        let mut buf = BytesMut::new();
        publish.write(&mut buf).unwrap();

        // println!("{:X?}", buf);
        // println!("{:#04X?}", &buf[..]);
        assert_eq!(&buf[..], sample2_bytes());
    }
}
