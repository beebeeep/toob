use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use async_stream::try_stream;
use bytes::BytesMut;
use futures_lite::{AsyncWriteExt, Stream};
use glommio::net::TcpStream;
use prost::Message;
use rand::Rng;
use snafu::ResultExt;

use crate::{
    error::{self, Error},
    pb,
    util::{encode_request_header, read_delimited_message, read_response_header},
};

pub struct Client {
    // endpoint: String,
    stream: TcpStream,
    topic_metadata: HashMap<Box<str>, u32>,
}

impl Client {
    pub async fn connect(endpoint: &str) -> Result<Self, Error> {
        let stream = TcpStream::connect(endpoint).await.context(error::Glommio {
            e: "connecting to endpoint",
        })?;
        Ok(Self {
            // endpoint: endpoint.to_string(),
            stream,
            topic_metadata: HashMap::new(),
        })
    }

    pub async fn ping(&mut self) -> Result<(), Error> {
        self.stream
            .write(encode_request_header(pb::Request::Noop, None::<pb::ConsumeRequest>)?.as_ref())
            .await
            .context(error::IO {
                e: "sending request",
            })?;

        let _ = read_response_header::<pb::MetadataResponse>(&mut self.stream).await?;
        Ok(())
    }

    pub async fn metadata(&mut self, topic: &str) -> Result<u32, Error> {
        if let Some(m) = self.topic_metadata.get(topic) {
            return Ok(*m); // TODO periodic refresh
        }

        let req = encode_request_header(
            pb::Request::Metadata,
            Some(pb::MetadataRequest {
                topic: topic.to_string(),
            }),
        )?;
        self.stream.write(req.as_ref()).await.context(error::IO {
            e: "sending request",
        })?;

        let resp: pb::MetadataResponse =
            read_response_header(&mut self.stream)
                .await?
                .ok_or(Error::Other {
                    e: String::from("empty response from server"),
                })?;

        self.topic_metadata.insert(topic.into(), resp.partitions);
        Ok(resp.partitions)
    }

    pub async fn produce(&mut self, topic: &str, messages: Vec<Vec<u8>>) -> Result<u64, Error> {
        let mut buf = BytesMut::with_capacity(512);
        let partitions = self.metadata(topic).await?;

        // TODO: implement partitioner that would distribute batch across partitions
        let req = encode_request_header(
            pb::Request::Produce,
            Some(pb::ProduceRequest {
                topic: topic.to_string(),
                partition: rand::rng().random_range(0..partitions),
                batch_size: messages.len() as u32,
            }),
        )?;

        self.stream.write(req.as_ref()).await.context(error::IO {
            e: "sending request",
        })?;

        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let messages = messages.into_iter().map(|msg| pb::Message {
            timestamp: Some(prost_types::Timestamp {
                seconds: ts.as_secs() as i64,
                nanos: ts.as_nanos() as i32,
            }),
            key: None, // TODO: message type encapsulating key and value
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
        }

        let resp: pb::ProduceResponse =
            read_response_header(&mut self.stream)
                .await?
                .ok_or(Error::Other {
                    e: String::from("empty produce response"),
                })?;

        Ok(resp.first_offset)
    }

    pub async fn consume(
        &mut self,
        topic: &str,
        partition: u32,
        start_offset: u64,
        max_messages: Option<u64>,
    ) -> Result<impl Stream<Item = Result<(u64, pb::Message), Error>>, Error> {
        let req = encode_request_header(
            pb::Request::Consume,
            Some(pb::ConsumeRequest {
                topic: String::from(topic),
                partition,
                start_offset,
                max_messages,
            }),
        )?;

        self.stream.write(req.as_ref()).await.context(error::IO {
            e: "sending request",
        })?;

        let resp: pb::ConsumeResponse =
            read_response_header(&mut self.stream)
                .await?
                .ok_or(Error::Other {
                    e: String::from("empty produce response"),
                })?;

        let mut offset = resp.first_offset;
        let mut count = 0;

        Ok(try_stream! {
            while max_messages.is_none_or(|max| max > count) {
                offset = offset + 1;
                count = count + 1;
                let msg = read_delimited_message(&mut self.stream).await?;
                let msg = pb::Message::decode_length_delimited(msg.as_ref()).context(error::Decode{e: "decoding message"})?;
                yield (offset - 1, msg)
            }
        })
    }
}
