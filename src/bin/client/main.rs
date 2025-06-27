use clap::{Arg, Command};
use futures_lite::StreamExt;
use futures_util::pin_mut;
use glommio::LocalExecutor;
use prost::Message;
use toob::client;

fn main() {
    let matches = Command::new("toob client")
        .args([
            Arg::new("producer")
                .short('p')
                .help("start producer")
                .action(clap::ArgAction::SetTrue),
            Arg::new("consumer")
                .short('c')
                .help("start consumer")
                .action(clap::ArgAction::SetTrue),
        ])
        .get_matches();
    let producer = matches.get_flag("producer");
    let consumer = matches.get_flag("consumer");
    let ex = LocalExecutor::default();
    ex.run(async {
        if producer {
            start_producer().await
        }
        if consumer {
            start_consumer().await
        }
    });
}

async fn start_producer() {
    let mut client = client::Client::connect("localhost:8137").await.unwrap();
    let input = std::io::stdin();
    loop {
        let mut line = String::new();
        if input.read_line(&mut line).unwrap() == 0 {
            break;
        }
        println!("read {} bytes", line.len());
        let offset = client
            .produce("test_topic", vec![line.encode_to_vec()])
            .await
            .unwrap();
        println!("wrote message to {offset}");
    }
}

async fn start_consumer() {
    let mut client = client::Client::connect("localhost:8137").await.unwrap();
    let messages = client.consume("test_topic", 0, 0, None).await.unwrap();
    pin_mut!(messages);
    while let Some(msg) = messages.next().await {
        let msg = msg.unwrap();
        println!("{msg:?}")
    }
}
