use clap::{Arg, Command};
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
                .action(clap::ArgAction::Set),
            Arg::new("workers")
                .short('w')
                .help("number of workers")
                .required(true)
                .action(clap::ArgAction::Set),
        ])
        .get_matches();
    let rps = matches.get_one::<u64>("rps").unwrap();
    let workers = matches.get_one::<usize>("workers").unwrap();
    let ex = LocalExecutorPoolBuilder::new(glommio::PoolPlacement::MaxSpread(
        *workers,
        Some(CpuSet::online().unwrap()),
    ))
    .on_all_shards(|| async { todo!() });
}

#[derive(Clone, PartialEq, Message)]
struct Probe {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(bytes, tag = "2")]
    payload: Vec<u8>,
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

async fn consumer(out_msg: async_channel::Sender<Probe>) {
    let mut client = client::Client::connect("localhost:8137").await.unwrap();
    let messages = client.consume("test_topic", 0, 0, None).await.unwrap();
    pin_mut!(messages);
    while let Some(msg) = messages.next().await {
        let msg = msg.unwrap();
        println!("{msg:?}")
    }
}
