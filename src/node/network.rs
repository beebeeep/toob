use async_channel::Sender;
use snafu::{IntoError, ResultExt};
use std::{fmt::Debug, time::Duration};

use crate::{
    IOMessage, TopicPartition,
    error::{self, Error},
    pb, util,
};
use bytes::{Buf, BytesMut};
use futures_lite::{AsyncRead, AsyncWrite, AsyncWriteExt};
use glommio::{io::ReadResult, net::TcpStream, timer::sleep};
use prost::Message;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Server {
    io_chans: HashMap<TopicPartition, Sender<IOMessage>>,
}

impl Server {
    pub async fn new(io_chans: HashMap<TopicPartition, Sender<IOMessage>>) -> Result<Self, Error> {
        Ok(Self { io_chans })
    }

    pub async fn accept(&self, stream: &mut TcpStream) -> Result<(), Error> {
        let resp = self.process_request(stream).await;
        let mut buf = BytesMut::with_capacity(64);
        resp.encode_length_delimited(&mut buf)
            .context(error::Encode {
                e: "encoding response",
            })?;
        stream.write(buf.as_ref()).await.context(error::IO {
            e: "sending response header",
        })?;
        Ok(())
    }
    pub async fn process_request(&self, mut stream: &mut TcpStream) -> pb::ResponseHeader {
        // incoming request starts with Header protobuf message prepended with varint-encoded length
        let header = if let Ok(v) = util::read_delimited_message(&mut stream).await {
            v
        } else {
            return pb::ResponseHeader {
                error: pb::ErrorCode::InvalidRequest as i32,
                ..Default::default()
            };
        };
        let header = if let Ok(v) = pb::RequestHeader::decode_length_delimited(header.as_ref()) {
            v
        } else {
            return pb::ResponseHeader {
                error: pb::ErrorCode::InvalidRequest as i32,
                ..Default::default()
            };
        };

        match header.request_id.try_into() {
            Ok(pb::Request::Consume) => self.req_consume(header.request.as_ref(), stream).await,
            Ok(pb::Request::Produce) => self.req_produce(header.request.as_ref(), stream).await,
            Ok(pb::Request::Noop) => pb::ResponseHeader {
                error: pb::ErrorCode::None as i32,
                ..Default::default()
            },
            Err(_) => pb::ResponseHeader {
                error: pb::ErrorCode::InvalidRequest as i32,
                ..Default::default()
            },
        }
    }

    async fn req_consume(
        &self,
        req: impl Buf,
        mut stream: impl AsyncRead + AsyncWrite + Unpin,
    ) -> pb::ResponseHeader {
        let req = match pb::ConsumeRequest::decode_length_delimited(req) {
            Ok(v) => v,
            Err(e) => {
                return pb::ResponseHeader {
                    error: pb::ErrorCode::InvalidRequest as i32,
                    error_description: Some(e.to_string()),
                    response: None,
                };
            }
        };
        eprintln!("got req {req:?}");
        /*
        let mut msg_count = 0;
        let id = (req.topic, req.partition);
        loop {
            let msg = match self
                .read_message_at_offset(&id, req.start_offset + msg_count)
                .await
            {
                Ok(msg) => msg,
                Err(Error::OffsetNotFound) => {
                    // no messages, wait a bit
                    // TODO: this shall be signalled somehow from producer?
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }
                Err(e) => {
                    return pb::ResponseHeader {
                        error: pb::ErrorCode::ProduceError as i32,
                        error_description: Some(e.to_string()),
                        response: None,
                    };
                }
            };

            if let Err(e) = stream.write(msg.as_ref()).await.context(error::IO {
                e: "writing message to stream",
            }) {
                return pb::ResponseHeader {
                    error: pb::ErrorCode::ProduceError as i32,
                    error_description: Some(e.to_string()),
                    response: None,
                };
            }
            eprintln!("wrote message of {} bytes", msg.len());
            msg_count += 1;
            if let Some(max) = req.max_messages {
                if msg_count >= max {
                    break;
                }
            }
        }
        */
        pb::ResponseHeader {
            error: pb::ErrorCode::None as i32,
            ..Default::default()
        }
    }

    async fn req_produce(
        &self,
        req: impl Buf,
        mut stream: impl AsyncRead + Unpin,
    ) -> pb::ResponseHeader {
        let req = if let Ok(v) = pb::ProduceRequest::decode_length_delimited(req) {
            v
        } else {
            return pb::ResponseHeader {
                error: pb::ErrorCode::InvalidRequest as i32,
                ..Default::default()
            };
        };
        eprintln!("got req {req:?}");
        // TODO: there should be single worker per each partition accepting writes via mpsc channel.
        // Perhaps it is possible to preallocate disk space in log file and have a dispatcher reserving ranges in file so that we can write to the partition concurrently,
        // but it'll be kinda hard in terms of error handling. Besides, we partition the topics exactly as a way to parallelize IO,
        // It doesn't seem reasonable to drop the notion of partitions, because apart for parallelism they are also useful for providing ordering semantics.

        /*
        let id = (req.topic, req.partition); // TODO: writes should be sharded between IO threads to ensure nobody is writing to partition logs concurrently
        let f = if let Some(f) = self.partition_files.get(&id) {
            f
        } else {
            return pb::ResponseHeader {
                error: pb::ErrorCode::TopicPartitionNotFound as i32,
                error_description: Some(format!("topic {} partition {} not found", id.0, id.1)),
                ..Default::default()
            };
        };
        let mut file_offset = match f.file_size().await {
            Ok(v) => v,
            Err(e) => {
                return pb::ResponseHeader {
                    error: pb::ErrorCode::ProduceError as i32,
                    error_description: Some(format!("getting partition size: {e}")),
                    ..Default::default()
                };
            }
        };
        let first_offset = file_offset;

        let mut offsets = Vec::with_capacity(req.batch_size as usize);
        for _ in 0..req.batch_size {
            let msg = match util::read_delimited_message(&mut stream).await {
                Ok(v) => v,
                Err(e) => {
                    return pb::ResponseHeader {
                        error: pb::ErrorCode::InvalidRequest as i32,
                        error_description: Some(format!("reading message: {e}")),
                        ..Default::default()
                    };
                }
            };
            let msg_len = msg.len();
            eprintln!("got message: {:?}", msg);
            if let Err(e) = f.write_at(msg, 0).await {
                return pb::ResponseHeader {
                    error: pb::ErrorCode::ProduceError as i32,
                    error_description: Some(format!("writing message to partition: {e}")),
                    ..Default::default()
                };
            }
            offsets.push(MessageMeta {
                file_offset,
                size: msg_len,
            });
            file_offset += msg_len as u64;
        }

        if let Err(e) = f.fdatasync().await {
            return pb::ResponseHeader {
                error: pb::ErrorCode::ProduceError as i32,
                error_description: Some(format!("doing fsync: {e}")),
                ..Default::default()
            };
        }

        // update index
        // nb: we don't do any awaits here, so we won't be interrupted
        // by new request that will be cloning self.
        {
            let mut offset_index = self.offset_index.borrow_mut();
            let index = offset_index.get_mut(&id).unwrap();
            let next = index.last_entry().map_or(0, |x| *x.key() + 1);
            offsets.into_iter().enumerate().for_each(|(i, o)| {
                index.insert(next + i as u64, o);
            });

            eprintln!("updated index:");
            for (k, v) in index {
                eprintln!("{k} => {v:?}");
            }
        }

        let mut buf = Vec::with_capacity(8);
        match (pb::ProduceResponse { first_offset }).encode_length_delimited(&mut buf) {
            Ok(_) => pb::ResponseHeader {
                error: pb::ErrorCode::None as i32,
                error_description: None,
                response: Some(buf),
            },
            Err(e) => pb::ResponseHeader {
                error: pb::ErrorCode::ProduceError as i32,
                error_description: Some(format!("encoding result: {e}")),
                ..Default::default()
            },
        }

        */
        pb::ResponseHeader {
            error: pb::ErrorCode::None as i32,
            error_description: None,
            response: None,
        }
    }
    /*
    async fn read_message_at_offset(
        &self,
        id: &TopicPartition,
        offset: u64,
    ) -> Result<ReadResult, Error> {
        let meta = {
            let index = self.offset_index.borrow();
            *index
                .get(id)
                .ok_or(Error::TopicPartitionNotFound {
                    // TODO: use Option instead of returning error here mb?
                    topic: id.0.clone(),
                    partition: id.1,
                })?
                .get(&offset)
                .ok_or(Error::OffsetNotFound)?
        };
        let f = self
            .partition_files
            .get(id)
            .ok_or(Error::TopicPartitionNotFound {
                topic: id.0.clone(),
                partition: id.1,
            })?;

        f.read_at(meta.file_offset, meta.size as usize)
            .await
            .context(error::Glommio {
                e: "reading partition",
            })
    }
    */
}
