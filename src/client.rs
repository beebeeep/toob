use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use async_stream::try_stream;
use bytes::BytesMut;
use futures_lite::{AsyncWriteExt, Stream};
use glommio::net::TcpStream;
use prost::Message;
use snafu::ResultExt;

use crate::{
    error::{self, Error},
    pb,
    util::{encode_request, read_delimited_message, read_response_header},
};

pub struct Client {
    endpoint: String,
    stream: TcpStream,
}

impl Client {
    pub async fn connect(endpoint: &str) -> Result<Self, Error> {
        let stream = TcpStream::connect(endpoint).await.context(error::Glommio {
            e: "connecting to endpoint",
        })?;
        Ok(Self {
            endpoint: endpoint.to_string(),
            stream,
        })
    }

    pub async fn ping(&mut self) -> Result<(), Error> {
        self.stream
            .write(encode_request(pb::Request::Noop, None::<pb::ConsumeRequest>)?.as_ref())
            .await
            .context(error::IO {
                e: "sending request",
            })?;

        let _ = read_response_header(&mut self.stream).await?;
        Ok(())
    }

    pub async fn produce(&mut self, topic: &str, messages: Vec<Vec<u8>>) -> Result<u64, Error> {
        let mut buf = BytesMut::with_capacity(512);

        let req = encode_request(
            pb::Request::Produce,
            Some(pb::ProduceRequest {
                topic: topic.to_string(),
                partition: 0, // TODO: partition discovery and partitioning
                batch_size: messages.len() as u32,
            }),
        )?;

        self.stream.write(req.as_ref()).await.context(error::IO {
            e: "sending request",
        })?;

        eprintln!("sent header, batch size: {}", messages.len());

        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let messages = messages.into_iter().map(|msg| pb::Message {
            timestamp: Some(prost_types::Timestamp {
                seconds: ts.as_secs() as i64,
                nanos: ts.as_nanos() as i32,
            }),
            key: None,
            value: msg,
            metadata: HashMap::new(),
        });

        for message in messages {
            buf.truncate(0);
            message
                .encode_length_delimited(&mut buf)
                .context(error::Encode {
                    e: "encoding message",
                })?;
            self.stream.write(&buf).await.context(error::IO {
                e: "sending message",
            })?;
            eprintln!("sent message");
        }
        eprintln!("sent batch");

        let resp = read_response_header(&mut self.stream)
            .await?
            .ok_or(Error::Other {
                e: String::from("empty produce response"),
            })?;
        eprintln!("read response");
        let resp =
            pb::ProduceResponse::decode_length_delimited(resp.as_ref()).context(error::Decode {
                e: "decoding server response",
            })?;

        Ok(resp.first_offset)
    }

    pub async fn consume(
        &mut self,
        topic: &str,
        partition: u32,
        start_offset: u64,
    ) -> Result<impl Stream<Item = Result<(u64, pb::Message), Error>>, Error> {
        let req = encode_request(
            pb::Request::Consume,
            Some(pb::ConsumeRequest {
                topic: String::from(topic),
                partition,
                start_offset,
                max_messages: None,
            }),
        )?;

        self.stream.write(req.as_ref()).await.context(error::IO {
            e: "sending request",
        })?;

        let resp = read_response_header(&mut self.stream)
            .await?
            .ok_or(Error::Other {
                e: String::from("empty produce response"),
            })?;
        let resp =
            pb::ConsumeResponse::decode_length_delimited(resp.as_ref()).context(error::Decode {
                e: "decoding response",
            })?;
        let mut offset = resp.first_offset;

        Ok(try_stream! {
            offset = offset + 1;
            let msg = read_delimited_message(&mut self.stream).await?;
            let msg = pb::Message::decode_length_delimited(msg.as_ref()).context(error::Decode{e: "decoding message"})?;
            yield (offset - 1, msg)
        })
    }
}
