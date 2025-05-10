use bytes::{Buf, BytesMut};
use prost::Message;
use std::{
    collections::HashMap,
    io::{self, Write},
    net::TcpStream,
    time::{SystemTime, UNIX_EPOCH},
};
use toob::pb;

fn main() {
    let mut stream = TcpStream::connect("localhost:8137").unwrap();
    io::stdin().lines().for_each(|line| {
        let line = line.unwrap();
        let req = pb::ProduceRequest {
            topic: "test_topic".to_string(),
            partition: 0,
            batch_size: 1,
        };
        let mut buf = BytesMut::with_capacity(128);
        req.encode_length_delimited(&mut buf).unwrap();
        let header = pb::Header {
            request_id: pb::Request::Produce as i32,
            request: buf.to_vec(),
        };
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let msg = pb::Message {
            timestamp: Some(prost_types::Timestamp {
                seconds: ts.as_secs() as i64,
                nanos: ts.subsec_nanos() as i32,
            }),
            metadata: HashMap::new(),
            key: None,
            value: line.encode_to_vec(),
        };

        buf.truncate(0);
        header.encode_length_delimited(&mut buf).unwrap();
        msg.encode_length_delimited(&mut buf).unwrap();
        stream.write(&buf).unwrap();
    });
}
