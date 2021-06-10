use std::io::Cursor;
use std::io::Write;
use std::time::Instant;

mod common;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {
    let count = 1024 * 1024;
    let payload_size = 1024;
    let payload = vec![1; payload_size];

    let data = generate_data(count, &payload[..]);

    let guard = pprof::ProfilerGuard::new(100).unwrap();

    // let mut output = BytesMut::with_capacity(10 * 1024);
    let mut output = Buffer::new(2 * 1024 * 1024 * 1024);

    let start = Instant::now();
    for publish in data.into_iter() {
        encode(&mut output, publish).unwrap();
    }

    let elapsed_micros = start.elapsed().as_micros();
    let total_size = output.written;
    let throughput = (total_size * 1_000_000) / elapsed_micros as usize;
    let write_throughput = throughput as f32 / 1024.0 / 1024.0 / 1024.0;
    let total_size_gb = total_size as f32 / 1024.0 / 1024.0 / 1024.0;

    let start = Instant::now();
    let mut packets: Vec<ServerOp> = Vec::with_capacity(count);

    let len = output.written;
    let mut buffer = Cursor::new(output.bytes);
    while buffer.position() < len as u64 - 1 {
        // dbg!(buffer.position());
        let packet = decode(&mut buffer).unwrap().unwrap();
        packets.push(packet);
    }

    let elapsed_micros = start.elapsed().as_micros();
    let throughput = (total_size * 1_000_000) / elapsed_micros as usize;
    let read_throughput = throughput as f32 / 1024.0 / 1024.0 / 1024.0;

    // --------------------------- results ---------------------------------------

    let print = common::Print {
        id: "natsparser".to_owned(),
        messages: count,
        payload_size,
        total_size_gb,
        write_throughput_gpbs: write_throughput,
        read_throughput_gpbs: read_throughput,
    };

    println!("{}", serde_json::to_string_pretty(&print).unwrap());
    common::profile("bench.pb", guard);
}

fn generate_data(count: usize, payload: &[u8]) -> Vec<ClientOp> {
    let mut data = Vec::with_capacity(count);

    for _ in 0..count {
        let publish = ClientOp::Pub {
            subject: "hello/world",
            reply_to: None,
            payload,
        };

        data.push(publish);
    }

    data
}

/// Reconnect buffer.
///
/// If the connection was broken and the client is currently reconnecting, PUB messages get stored
/// in this buffer of limited size. As soon as the connection is then re-established, buffered
/// messages will be sent to the server.
struct Buffer {
    /// Bytes in the buffer.
    ///
    /// There are three interesting ranges in this slice:
    ///
    /// - `..flushed` contains buffered PUB messages.
    /// - `flushed..written` contains a partial PUB message at the end.
    /// - `written..` is empty space in the buffer.
    pub bytes: Box<[u8]>,

    /// Number of written bytes.
    pub written: usize,

    /// Number of bytes marked as "flushed".
    flushed: usize,
}

use std::io::{self, Error, ErrorKind};

impl Buffer {
    /// Creates a new buffer with the given size.
    fn new(size: usize) -> Buffer {
        Buffer {
            bytes: vec![0_u8; size].into_boxed_slice(),
            written: 0,
            flushed: 0,
        }
    }

    /// Clears the buffer and returns buffered bytes.
    fn _clear(&mut self) -> &[u8] {
        let buffered = &self.bytes[..self.flushed];
        self.written = 0;
        self.flushed = 0;
        buffered
    }
}

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = buf.len();

        // Check if `buf` will fit into this `Buffer`.
        if self.bytes.len() - self.written < n {
            // Fill the buffer to prevent subsequent smaller writes.
            self.written = self.bytes.len();

            Err(Error::new(
                ErrorKind::Other,
                "the disconnect buffer is full",
            ))
        } else {
            // Append `buf` into the buffer.
            let range = self.written..self.written + n;
            self.bytes[range].copy_from_slice(&buf[..n]);
            self.written += n;
            Ok(n)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flushed = self.written;
        Ok(())
    }
}

use std::io::prelude::*;
use std::str::FromStr;

use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
    iter::{FromIterator, IntoIterator},
    ops::Deref,
};

/// A protocol operation sent by the server.
#[derive(Debug)]
pub(crate) enum ServerOp {
    /// `MSG <subject> <sid> [reply-to] <#bytes>\r\n[payload]\r\n`
    Msg {
        subject: String,
        sid: u64,
        reply_to: Option<String>,
        payload: Vec<u8>,
    },

    /// `HMSG <subject> <sid> [reply-to] <# header bytes> <# total bytes>\r\n<version
    /// line>\r\n[headers]\r\n\r\n[payload]\r\n`
    Hmsg {
        subject: String,
        headers: Headers,
        sid: u64,
        reply_to: Option<String>,
        payload: Vec<u8>,
    },

    /// `PING`
    Ping,

    /// `PONG`
    Pong,

    /// `-ERR <error message>`
    Err(String),

    /// Unknown protocol message.
    Unknown(String),
}

/// Decodes a single operation from the server.
///
/// If the connection is closed, `None` will be returned.
pub(crate) fn decode(mut stream: impl BufRead) -> io::Result<Option<ServerOp>> {
    // Read a line, which should be human readable.
    let mut line = Vec::new();
    if stream.read_until(b'\n', &mut line)? == 0 {
        // If zero bytes were read, the connection is closed.
        return Ok(None);
    }

    // Convert into a UTF8 string for simpler parsing.
    String::from_utf8_lossy(&line);
    let line = String::from_utf8(line).map_err(|err| Error::new(ErrorKind::InvalidInput, err))?;
    let line_uppercase = line.trim();
    // .to_uppercase();

    if line_uppercase.starts_with("PING") {
        return Ok(Some(ServerOp::Ping));
    }

    if line_uppercase.starts_with("PONG") {
        return Ok(Some(ServerOp::Pong));
    }

    if line_uppercase.starts_with("MSG") {
        // Extract whitespace-delimited arguments that come after "MSG".
        let args = line["MSG".len()..]
            .split_whitespace()
            .filter(|s| !s.is_empty());
        let args = args.collect::<Vec<_>>();

        // Parse the operation syntax: MSG <subject> <sid> [reply-to] <#bytes>
        let (subject, sid, reply_to, num_bytes) = match args[..] {
            [subject, sid, num_bytes] => (subject, sid, None, num_bytes),
            [subject, sid, reply_to, num_bytes] => (subject, sid, Some(reply_to), num_bytes),
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid number of arguments after MSG",
                ))
            }
        };

        // Convert the slice into an owned string.
        let subject = subject.to_string();

        // Parse the subject ID.
        let sid = u64::from_str(sid).map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                "cannot parse sid argument after MSG",
            )
        })?;

        // Convert the slice into an owned string.
        let reply_to = reply_to.map(ToString::to_string);

        // Parse the number of payload bytes.
        let num_bytes = u32::from_str(num_bytes).map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                "cannot parse the number of bytes argument after MSG",
            )
        })?;

        // Read the payload.
        let mut payload = Vec::new();
        payload.resize(num_bytes as usize, 0_u8);
        stream.read_exact(&mut payload[..])?;
        // Read "\r\n".
        stream.read_exact(&mut [0_u8; 2])?;

        return Ok(Some(ServerOp::Msg {
            subject,
            sid,
            reply_to,
            payload,
        }));
    }

    if line_uppercase.starts_with("HMSG") {
        // Extract whitespace-delimited arguments that come after "HMSG".
        let args = line["HMSG".len()..]
            .split_whitespace()
            .filter(|s| !s.is_empty());
        let args = args.collect::<Vec<_>>();

        // Parse the operation syntax:
        // `HMSG <subject> <sid> [reply-to] <# header bytes>
        // <# total bytes>\r\n<version line>\r\n[headers]\r\n\r\n[payload]\r\n`
        let (subject, sid, reply_to, num_header_bytes, num_bytes) = match args[..] {
            [subject, sid, num_header_bytes, num_bytes] => {
                (subject, sid, None, num_header_bytes, num_bytes)
            }
            [subject, sid, reply_to, num_header_bytes, num_bytes] => {
                (subject, sid, Some(reply_to), num_header_bytes, num_bytes)
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid number of arguments after HMSG",
                ))
            }
        };

        // Convert the slice into an owned string.
        let subject = subject.to_string();

        // Parse the subject ID.
        let sid = u64::from_str(sid).map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                "cannot parse sid argument after HMSG",
            )
        })?;

        // Convert the slice into an owned string.
        let reply_to = reply_to.map(ToString::to_string);

        // Parse the number of payload bytes.
        let num_header_bytes = u32::from_str(num_header_bytes).map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                "cannot parse the number of header bytes argument after HMSG",
            )
        })?;

        // Parse the number of payload bytes.
        let num_bytes = u32::from_str(num_bytes).map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                "cannot parse the number of bytes argument after HMSG",
            )
        })?;

        if num_bytes <= num_header_bytes {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "number of header bytes was greater than or \
                equal to the total number of bytes after HMSG",
            ));
        }

        let num_payload_bytes = num_bytes - num_header_bytes;

        // `HMSG <subject> <sid> [reply-to]
        // <# header bytes> <# total bytes>\r\n
        // <version line>\r\n[headers]\r\n\r\n[payload]\r\n`

        // Read the header payload.
        let mut header_payload = Vec::new();
        header_payload.resize(num_header_bytes as usize, 0_u8);
        stream.read_exact(&mut header_payload[..])?;

        let headers = Headers::try_from(&*header_payload)?;

        // Read the payload.
        let mut payload = Vec::new();
        payload.resize(num_payload_bytes as usize, 0_u8);
        stream.read_exact(&mut payload[..])?;
        // Read "\r\n".
        stream.read_exact(&mut [0_u8; 2])?;

        return Ok(Some(ServerOp::Hmsg {
            subject,
            headers,
            sid,
            reply_to,
            payload,
        }));
    }

    if line_uppercase.starts_with("-ERR") {
        // Extract the message argument.
        let msg = line["-ERR".len()..].trim().trim_matches('\'').to_string();

        return Ok(Some(ServerOp::Err(msg)));
    }

    Ok(Some(ServerOp::Unknown(line)))
}

/// A protocol operation sent by the client.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ClientOp<'a> {
    /// `PUB <subject> [reply-to] <#bytes>\r\n[payload]\r\n`
    Pub {
        subject: &'a str,
        reply_to: Option<&'a str>,
        payload: &'a [u8],
    },
}

/// Encodes a single operation from the client.
pub(crate) fn encode(mut stream: impl Write, op: ClientOp<'_>) -> io::Result<()> {
    match &op {
        ClientOp::Pub {
            subject,
            reply_to,
            payload,
        } => {
            stream.write_all(b"PUB ")?;
            stream.write_all(subject.as_bytes())?;
            stream.write_all(b" ")?;

            if let Some(reply_to) = reply_to {
                stream.write_all(reply_to.as_bytes())?;
                stream.write_all(b" ")?;
            }

            let mut buf = itoa::Buffer::new();
            stream.write_all(buf.format(payload.len()).as_bytes())?;
            stream.write_all(b"\r\n")?;

            stream.write_all(payload)?;
            stream.write_all(b"\r\n")?;
        }
    }

    Ok(())
}

/// A multi-map from header name to a set of values for that header
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Headers {
    /// A multi-map from header name to a set of values for that header
    pub inner: HashMap<String, HashSet<String>>,
}

impl FromIterator<(String, String)> for Headers {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (String, String)>,
    {
        let mut inner = HashMap::default();
        for (k, v) in iter {
            let entry = inner.entry(k).or_insert_with(HashSet::default);
            entry.insert(v);
        }
        Headers { inner }
    }
}

impl<'a> FromIterator<(&'a String, &'a String)> for Headers {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (&'a String, &'a String)>,
    {
        let mut inner = HashMap::default();
        for (k, v) in iter {
            let k = k.to_string();
            let v = v.to_string();
            let entry = inner.entry(k).or_insert_with(HashSet::default);
            entry.insert(v);
        }
        Headers { inner }
    }
}

impl<'a> FromIterator<&'a (&'a String, &'a String)> for Headers {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = &'a (&'a String, &'a String)>,
    {
        let mut inner = HashMap::default();
        for (k, v) in iter {
            let k = k.to_string();
            let v = v.to_string();
            let entry = inner.entry(k).or_insert_with(HashSet::default);
            entry.insert(v);
        }
        Headers { inner }
    }
}

impl<'a> FromIterator<(&'a str, &'a str)> for Headers {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (&'a str, &'a str)>,
    {
        let mut inner = HashMap::default();
        for (k, v) in iter {
            let k = k.to_string();
            let v = v.to_string();
            let entry = inner.entry(k).or_insert_with(HashSet::default);
            entry.insert(v);
        }
        Headers { inner }
    }
}

impl<'a> FromIterator<&'a (&'a str, &'a str)> for Headers {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = &'a (&'a str, &'a str)>,
    {
        let mut inner = HashMap::default();
        for (k, v) in iter {
            let k = k.to_string();
            let v = v.to_string();
            let entry = inner.entry(k).or_insert_with(HashSet::default);
            entry.insert(v);
        }
        Headers { inner }
    }
}

fn parse_error<T, E: AsRef<str>>(e: E) -> std::io::Result<T> {
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        e.as_ref(),
    ))
}

impl TryFrom<&[u8]> for Headers {
    type Error = std::io::Error;

    fn try_from(buf: &[u8]) -> std::io::Result<Self> {
        let mut inner = HashMap::default();
        let mut lines = if let Ok(line) = std::str::from_utf8(buf) {
            line.lines()
        } else {
            return parse_error("invalid utf8 received");
        };

        if let Some(line) = lines.next() {
            if !line.starts_with("NATS/") {
                return parse_error("version line does not begin with NATS/");
            }
        } else {
            return parse_error("expected header information not present");
        };

        for line in lines {
            let splits = line.split(':').map(str::trim).collect::<Vec<_>>();
            match splits[..] {
                [k, v] => {
                    let entry = inner.entry(k.to_string()).or_insert_with(HashSet::default);
                    entry.insert(v.to_string());
                }
                [""] => continue,
                _ => {
                    return parse_error("malformed header input");
                }
            }
        }

        Ok(Headers { inner })
    }
}

impl Deref for Headers {
    type Target = HashMap<String, HashSet<String>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
