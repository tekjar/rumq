use futures_util::stream::StreamExt;
use std::thread;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task;
use tokio::time;

use rumq_client::{self, MqttOptions, Request, eventloop};
use std::time::Duration;

#[tokio::main(basic_scheduler)]
async fn main() {
    pretty_env_logger::init();
    color_backtrace::install();

    let (requests_tx, requests_rx) = channel(10);
    let mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    let mqttoptions = mqttoptions.set_keep_alive(10).set_throttle(Duration::from_secs(1));
    let mut eventloop = eventloop(mqttoptions, requests_rx).await.unwrap();

    thread::spawn(move || {
        #[tokio::main(basic_scheduler)]
        async fn requests(mut requests_tx: Sender<Request>) {
            task::spawn(async move {
                for i in 0..10 {
                    requests_tx.send(publish_request(i)).await.unwrap();
                    time::delay_for(Duration::from_secs(1)).await; 
                }
            }).await.unwrap();
        }

        requests(requests_tx);
        thread::sleep(Duration::from_secs(3));
    });

    while let Some(item) = eventloop.next().await {
        println!("{:?}", item);
    }

    println!("State = {:?}", eventloop.state);
}

fn publish_request(i: u8) -> Request {
    let topic = "hello/world".to_owned();
    let payload = vec![1, 2, 3, i];

    let publish = rumq_client::publish(topic, payload);
    Request::Publish(publish)
}
