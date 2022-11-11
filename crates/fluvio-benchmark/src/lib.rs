use std::time::Duration;

use async_std::{task::block_on, future::timeout, stream::StreamExt};
use bench_env::{FLUVIO_BENCH_RECORDS_PER_BATCH, EnvOrDefault, FLUVIO_BENCH_RECORD_NUM_BYTES};
use fluvio::{
    TopicProducer, PartitionConsumer, metadata::topic::TopicSpec, FluvioAdmin, RecordKey, Offset,
};
use rand::{distributions::Alphanumeric, Rng};

pub mod bench_env;
pub mod throughput;
pub mod latency;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const TOPIC_NAME: &str = "benchmarking-topic";

pub type Setup = (
    (TopicProducer, std::vec::Vec<std::string::String>),
    (PartitionConsumer, std::vec::Vec<std::string::String>),
);

pub fn setup() -> Setup {
    block_on(timeout(DEFAULT_TIMEOUT, do_setup())).unwrap()
}

pub async fn do_setup() -> Setup {
    // println!("Setting up for new iteration");
    let new_topic = TopicSpec::new_computed(1, 1, None);

    // println!("Connecting to fluvio");

    let admin = FluvioAdmin::connect().await.unwrap();
    // println!("Deleting old topic if present");
    let _ = admin
        .delete::<TopicSpec, String>(TOPIC_NAME.to_string())
        .await;
    // println!("Creating new topic {TOPIC_NAME}");
    admin
        .create(TOPIC_NAME.to_string(), false, new_topic)
        .await
        .unwrap();

    // println!("Creating producer and consumers");
    let producer = fluvio::producer(TOPIC_NAME).await.unwrap();
    let consumer = fluvio::consumer(TOPIC_NAME, 0).await.unwrap();
    let data: Vec<String> = (0..FLUVIO_BENCH_RECORDS_PER_BATCH.env_or_default())
        .map(|_| generate_random_string(FLUVIO_BENCH_RECORD_NUM_BYTES.env_or_default()))
        .collect();

    // Make sure producer and consumer are ready to go
    producer.send(RecordKey::NULL, "setup").await.unwrap();
    producer.flush().await.unwrap();
    consumer
        .stream(Offset::absolute(0).unwrap())
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();

    ((producer, data.clone()), (consumer, data))
}

fn generate_random_string(size: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(size)
        .map(char::from)
        .collect()
}