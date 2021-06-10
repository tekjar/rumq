use rumqttc::{self, Client, Event, Incoming, MqttOptions, QoS};
use std::error::Error;
use std::thread;
use std::time::{Duration, Instant};

mod common;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {
    pretty_env_logger::init();
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    start("rumqtt-sync", 100, 1_000_000).unwrap();
    common::profile("bench.pb", guard);
}

pub fn start(id: &str, payload_size: usize, count: usize) -> Result<(), Box<dyn Error>> {
    let mut mqttoptions = MqttOptions::new(id, "localhost", 1883);
    mqttoptions.set_keep_alive(20);
    mqttoptions.set_inflight(100);

    let (mut client, mut connection) = Client::new(mqttoptions, 10);
    thread::spawn(move || {
        for _i in 0..count {
            let payload = vec![0; payload_size];
            let qos = QoS::AtLeastOnce;
            let topic = "hello/benchmarks/world";
            client.publish(topic, qos, false, payload).unwrap();
        }

        thread::sleep(Duration::from_secs(1));
    });

    let mut acks_count = 0;
    let start = Instant::now();
    for event in connection.iter() {
        if let Ok(Event::Incoming(Incoming::PubAck(_))) = event {
            acks_count += 1;
            if acks_count == count {
                break;
            }
        }
    }

    let elapsed_micros = start.elapsed().as_micros();
    let throughput = (acks_count * 1_000_000) / elapsed_micros as usize;

    // --------------------------- results ---------------------------------------

    let print = common::Print {
        id: id.to_owned(),
        messages: count,
        payload_size,
        throughput,
    };

    println!("{}", serde_json::to_string_pretty(&print).unwrap());
    println!("@");
    Ok(())
}
