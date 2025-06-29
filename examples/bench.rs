use std::sync::Arc;

use bytes::BytesMut;
use clap::{Arg, Command, Id, value_parser};
use futures_lite::StreamExt;
use futures_util::pin_mut;
use glommio::{CpuSet, LocalExecutor, LocalExecutorPoolBuilder};
use prost;
use prost::Message;
use toob::client;

fn main() {
    let matches = Command::new("toob client")
        .args([
            Arg::new("rps")
                .short('s')
                .help("rps per worker")
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
        ])
        .get_matches();
    let rps = matches.get_one::<u32>("rps").unwrap();
    let producers = matches.get_one::<usize>("producers").unwrap();
    let consumers = matches.get_one::<usize>("consumers").unwrap();

    let producer_handles = LocalExecutorPoolBuilder::new(glommio::PoolPlacement::MaxSpread(
        *producers,
        Some(CpuSet::online().unwrap()),
    ))
    .on_all_shards(|| async {
        println!("producer {}", glommio::executor().id());
    })
    .unwrap();
    producer_handles.join_all();

    let consumer_handles = LocalExecutorPoolBuilder::new(glommio::PoolPlacement::MaxSpread(
        *consumers,
        Some(CpuSet::online().unwrap()),
    ))
    .on_all_shards(|| async {
        println!("consumer {}", glommio::executor().id());
    })
    .unwrap();
    consumer_handles.join_all();
}

#[derive(Clone, PartialEq, Message)]
struct Probe {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(bytes, tag = "2")]
    payload: Vec<u8>,
}

async fn dispatcher(in_msg: async_channel::Receiver<Probe>, out_msg: async_channel::Sender<Probe>) {
    todo!()
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
