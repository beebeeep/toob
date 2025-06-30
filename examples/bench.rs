use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use clap::{Arg, Command, value_parser};
use futures_lite::StreamExt;
use futures_util::pin_mut;
use glommio::sync::RwLock;
use glommio::{LocalExecutor, LocalExecutorBuilder, Placement, spawn_local, timer};
use prost::Message;
use rand::RngCore;
use toob::client;

fn main() {
    let matches = Command::new("toob client")
        .args([
            Arg::new("rps")
                .short('r')
                .help("total rps")
                .required(true)
                .value_parser(value_parser!(u32)),
            Arg::new("producers")
                .short('p')
                .help("number of producers")
                .required(true)
                .value_parser(value_parser!(usize)),
            Arg::new("consumers")
                .short('c')
                .help("number of consumers")
                .required(true)
                .value_parser(value_parser!(usize)),
            Arg::new("payload-size")
                .short('S')
                .help("size of message")
                .required(true)
                .value_parser(value_parser!(usize)),
            Arg::new("topic")
                .short('t')
                .help("topic")
                .default_value("test_topic")
                .value_parser(value_parser!(String)),
        ])
        .get_matches();
    let rps = matches.get_one::<u32>("rps").unwrap();
    let producers = matches.get_one::<usize>("producers").unwrap();
    let consumers = matches.get_one::<usize>("consumers").unwrap();
    let payload_size = matches.get_one::<usize>("payload-size").unwrap();
    let topic = matches.get_one::<String>("topic").unwrap();
    let (producer_tx, producer_rx) = async_channel::unbounded();
    let (consumer_tx, consumer_rx) = async_channel::unbounded();

    let ex = LocalExecutor::default();
    let partitions = ex.run(async {
        let mut client = client::Client::connect("localhost:8137").await.unwrap();
        client.metadata(topic).await.unwrap()
    });
    let partitions_per_consumer = partitions / (*consumers as u32);
    eprintln!("topic has {partitions} partitions, {partitions_per_consumer} per consumer thread");

    // spawn dispatcher
    let builder = LocalExecutorBuilder::new(Placement::Fixed(0));
    let disp = {
        let payload_size = *payload_size;
        let rps = *rps;
        builder
            .spawn(async move || {
                dispatcher(payload_size, rps, consumer_rx, producer_tx).await;
            })
            .unwrap()
    };

    // spawn producers
    let mut producer_threads = Vec::new();
    for cpu in 1..producers + 1 {
        eprintln!("starting producer at cpu {cpu}");
        let producer_rx = producer_rx.clone();
        let builder = LocalExecutorBuilder::new(Placement::Fixed(cpu));
        producer_threads.push(
            builder
                .spawn(async move || producer(producer_rx).await)
                .unwrap(),
        );
    }

    // spawn consumers
    let mut consumer_threads = Vec::new();
    let mut next_partition = 0u32;
    for cpu in producers + 1..producers + consumers + 1 {
        let consumer_tx = consumer_tx.clone();
        let builder = LocalExecutorBuilder::new(Placement::Fixed(cpu));
        let partitions = next_partition..next_partition + partitions_per_consumer;
        eprintln!(
            "starting consumer on cpu {cpu} for partitions {:?}",
            partitions.clone().collect::<Vec<u32>>()
        );
        consumer_threads.push(
            builder
                .spawn(async move || {
                    let mut partition_consumers = Vec::new();
                    for partition in partitions {
                        let ctx = consumer_tx.clone();
                        partition_consumers.push(
                            spawn_local(async move { consumer(ctx, partition).await }).detach(),
                        );
                    }

                    for c in partition_consumers {
                        c.await;
                    }
                })
                .unwrap(),
        );
        next_partition += partitions_per_consumer;
    }

    disp.join().unwrap();
    for t in producer_threads {
        t.join().unwrap();
    }
    for t in consumer_threads {
        t.join().unwrap();
    }
}

#[derive(Clone, PartialEq, Message)]
struct Probe {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(bytes, tag = "2")]
    payload: Vec<u8>,
}

async fn dispatcher(
    payload_size: usize,
    rps: u32,
    from_consumer: async_channel::Receiver<Probe>,
    to_producer: async_channel::Sender<Probe>,
) {
    let messages = Rc::new(RwLock::new(HashMap::<u64, Instant>::new()));

    let msgs = messages.clone();
    let observer = spawn_local(async move {
        loop {
            let msg = from_consumer.recv().await.unwrap();
            match msgs.write().await.unwrap().remove(&msg.id) {
                None => {
                    eprintln!("got unknown message id {}", msg.id)
                }
                Some(produced_at) => {
                    println!(
                        "got message id {}, RTT is {:#?}",
                        msg.id,
                        Instant::now().duration_since(produced_at)
                    );
                    // todo!();
                }
            }
        }
    })
    .detach();

    let msgs = messages.clone();
    let mut rng = rand::rng();
    let period = Duration::from_micros(1000_000 / rps as u64);
    let emitter = spawn_local(async move {
        for id in 0..u64::MAX {
            let mut payload = vec![0; payload_size];
            rng.fill_bytes(&mut payload);
            let probe = Probe { id, payload };
            let generated_at = Instant::now();
            msgs.write().await.unwrap().insert(probe.id, generated_at);
            to_producer.send(probe).await.unwrap();
            match Instant::now().duration_since(generated_at) {
                d if d < period => {
                    timer::sleep(period - d).await;
                }
                _ => {
                    println!("skip!")
                }
            }
        }
    })
    .detach();

    observer.await;
    emitter.await;
}

async fn producer(in_msg: async_channel::Receiver<Probe>) {
    let mut client = client::Client::connect("localhost:8137").await.unwrap();
    loop {
        let msg = in_msg.recv().await.unwrap();
        let _ = client
            .produce("test_topic", vec![msg.encode_to_vec()])
            .await
            .unwrap();
    }
}

async fn consumer(out_msg: async_channel::Sender<Probe>, partition: u32) {
    let mut client = client::Client::connect("localhost:8137").await.unwrap();
    let messages = client
        .consume("test_topic", partition, 0, None)
        .await
        .unwrap();
    pin_mut!(messages);
    while let Some(msg) = messages.next().await {
        let (_, msg) = msg.unwrap();
        let msg = Probe::decode(msg.value.as_ref()).unwrap();
        out_msg.send(msg).await.unwrap();
    }
}
