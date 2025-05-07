use bytes::BytesMut;
use prost::Message;
use std::{io::Write, net::TcpStream};
use toob::pb;

fn main() {
    let header = pb::Header {
        request: pb::Request::Consume as i32,
    };
    let mut stream = TcpStream::connect("localhost:8137").unwrap();
    let mut buf = BytesMut::new();
    header.encode_length_delimited(&mut buf).unwrap();

    stream.write(&buf).unwrap();
    println!("CHLOS, {header:?}");
}
