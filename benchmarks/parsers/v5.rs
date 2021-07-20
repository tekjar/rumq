use bytes::{Buf, BytesMut};
use mqttbytes::v5;
use mqttbytes::QoS;
use std::time::Instant;

mod common;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {
    pretty_env_logger::init();
    let count = 1024 * 1024;
    let payload_size = 1024;
    let guard = pprof::ProfilerGuard::new(100).unwrap();

    // ------------------------- write throughput -------------------------------

    let data = generate_data(count, payload_size);
    let mut output = BytesMut::with_capacity(10 * 1024);

    let start = Instant::now();
    for publish in data.into_iter() {
        publish.write(&mut output).unwrap();
    }

    let elapsed_micros = start.elapsed().as_micros();
    let total_size = output.len();
    let throughput = (total_size * 1_000_000) / elapsed_micros as usize;
    let write_throughput = throughput as f32 / 1024.0 / 1024.0 / 1024.0;
    let total_size_gb = total_size as f32 / 1024.0 / 1024.0 / 1024.0;

    // --------------------------- read throughput -------------------------------

    let start = Instant::now();
    let mut packets = Vec::with_capacity(count);
    while output.has_remaining() {
        let packet = v5::read(&mut output, 10 * 1024).unwrap();
        packets.push(packet);
    }

    let elapsed_micros = start.elapsed().as_micros();
    let throughput = (total_size * 1_000_000) / elapsed_micros as usize;
    let read_throughput = throughput as f32 / 1024.0 / 1024.0 / 1024.0;

    // --------------------------- results ---------------------------------------

    let print = common::Print {
        id: "mqttv5parser".to_owned(),
        messages: count,
        payload_size,
        total_size_gb,
        write_throughput_gpbs: write_throughput,
        read_throughput_gpbs: read_throughput,
    };

    println!("{}", serde_json::to_string_pretty(&print).unwrap());
    common::profile("bench.pb", guard);
}

fn generate_data(count: usize, payload_size: usize) -> Vec<v5::Publish> {
    let mut data = Vec::with_capacity(count);
    for i in 0..count {
        let mut publish = v5::Publish::new("hello/world", QoS::AtLeastOnce, vec![1; payload_size]);
        publish.pkid = (i % 100 + 1) as u16;
        data.push(publish);
    }

    data
}
