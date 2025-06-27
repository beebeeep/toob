use bytes::BytesMut;
use clap::{Arg, Command};
use futures_lite::AsyncWriteExt;
use glommio::{LocalExecutor, net::TcpStream};
use prost::Message;
use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};
use toob::{client, pb, util};

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
    let mut stream = TcpStream::connect("localhost:8137").await.unwrap();
    let req = pb::ConsumeRequest {
        topic: "test_topic".to_string(),
        partition: 0,
        start_offset: 0,
        max_messages: Some(100),
    };
    let mut buf = BytesMut::with_capacity(128);
    req.encode_length_delimited(&mut buf).unwrap();
    let header = pb::RequestHeader {
        request_id: pb::Request::Consume as i32,
        request: buf.to_vec(),
    };
    buf.truncate(0);
    header.encode_length_delimited(&mut buf).unwrap();
    stream.write(&buf).await.unwrap();
    loop {
        let msg = util::read_delimited_message(&mut stream).await.unwrap();
        let msg = pb::Message::decode_length_delimited(msg.as_ref()).unwrap();
        println!("-> {}", String::from_utf8(msg.value).unwrap());
    }
}
