use async_channel::Sender;
use snafu::ResultExt;

use crate::{
    IOMessage, IOReply, IORequest, TopicPartition,
    error::{self, Error},
    pb,
    util::{self, encode_response_header},
};
use bytes::{Buf, BytesMut};
use futures_lite::{AsyncRead, AsyncWrite, AsyncWriteExt};
use glommio::net::TcpStream;
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
        if let Some(resp) = self.process_request(stream).await {
            let mut buf = BytesMut::with_capacity(64);
            resp.encode_length_delimited(&mut buf)
                .context(error::Encode {
                    e: "encoding response",
                })?;
            stream.write(buf.as_ref()).await.context(error::IO {
                e: "sending response header",
            })?;
        }
        Ok(())
    }
    pub async fn process_request(&self, mut stream: &mut TcpStream) -> Option<pb::ResponseHeader> {
        // incoming request starts with Header protobuf message prepended with varint-encoded length
        let header = if let Ok(v) = util::read_delimited_message(&mut stream).await {
            v
        } else {
            return Some(pb::ResponseHeader {
                error: pb::ErrorCode::InvalidRequest as i32,
                ..Default::default()
            });
        };
        let header = if let Ok(v) = pb::RequestHeader::decode_length_delimited(header.as_ref()) {
            v
        } else {
            return Some(pb::ResponseHeader {
                error: pb::ErrorCode::InvalidRequest as i32,
                ..Default::default()
            });
        };

        match header.request_id.try_into() {
            Ok(pb::Request::Consume) => self.req_consume(header.request.as_ref(), stream).await,
            Ok(pb::Request::Produce) => self.req_produce(header.request.as_ref(), stream).await,
            Ok(pb::Request::Metadata) => self.req_metadata(header.request.as_ref()).await,
            Ok(pb::Request::Noop) => Some(pb::ResponseHeader {
                error: pb::ErrorCode::None as i32,
                ..Default::default()
            }),
            Err(_) => Some(pb::ResponseHeader {
                error: pb::ErrorCode::InvalidRequest as i32,
                ..Default::default()
            }),
        }
    }

    async fn req_metadata(&self, req: impl Buf) -> Option<pb::ResponseHeader> {
        let req = match pb::MetadataRequest::decode_length_delimited(req) {
            Ok(v) => v,
            Err(e) => {
                return Some(pb::ResponseHeader {
                    error: pb::ErrorCode::InvalidRequest as i32,
                    error_description: Some(e.to_string()),
                    response: None,
                });
            }
        };
        let partitions = self
            .io_chans
            .keys()
            .filter(|tp| tp.topic == req.topic)
            .count();

        eprintln!("found {partitions} partitions");

        let mut buf = Vec::with_capacity(8);
        let resp = pb::MetadataResponse {
            partitions: partitions as u32,
        };
        match resp.encode_length_delimited(&mut buf) {
            Ok(_) => Some(pb::ResponseHeader {
                error: pb::ErrorCode::None as i32,
                error_description: None,
                response: Some(buf),
            }),
            Err(e) => Some(pb::ResponseHeader {
                error: pb::ErrorCode::GenericError as i32,
                error_description: Some(e.to_string()),
                ..Default::default()
            }),
        }
    }

    async fn req_consume(
        &self,
        req: impl Buf,
        mut stream: impl AsyncRead + AsyncWrite + Unpin,
    ) -> Option<pb::ResponseHeader> {
        let req = match pb::ConsumeRequest::decode_length_delimited(req) {
            Ok(v) => v,
            Err(e) => {
                return Some(pb::ResponseHeader {
                    error: pb::ErrorCode::InvalidRequest as i32,
                    error_description: Some(e.to_string()),
                    response: None,
                });
            }
        };
        eprintln!("got req {req:?}");
        let mut msg_count = 0;
        let tp = TopicPartition::new(&req.topic, &req.partition);
        let mut consume_resp_send = false;
        loop {
            let tp = tp.clone();
            let (tx, rx) = oneshot::channel();
            let io_worker = match self.io_chans.get(&tp) {
                Some(v) => v,
                None => {
                    return Some(pb::ResponseHeader {
                        error: pb::ErrorCode::TopicPartitionNotFound as i32,
                        error_description: Some(format!(
                            "topic {} partition {} not found",
                            tp.topic, tp.partition
                        )),
                        ..Default::default()
                    });
                }
            };
            let offset = req.start_offset + msg_count;
            let msg = match io_worker.send((IORequest::Get { tp, offset }, tx)).await {
                Ok(_) => match rx.await {
                    Ok(resp) => match resp {
                        Ok(IOReply::Get(v)) => v,
                        Ok(_) => {
                            panic!("unexpected reply from IO worker")
                        }
                        Err(e) => {
                            return Some(pb::ResponseHeader {
                                error: pb::ErrorCode::FetchError as i32,
                                error_description: Some(e.to_string()),
                                ..Default::default()
                            });
                        }
                    },
                    Err(e) => {
                        return Some(pb::ResponseHeader {
                            error: pb::ErrorCode::FetchError as i32,
                            error_description: Some(e.to_string()),
                            ..Default::default()
                        });
                    }
                },
                Err(e) => {
                    return Some(pb::ResponseHeader {
                        error: pb::ErrorCode::FetchError as i32,
                        error_description: Some(e.to_string()),
                        ..Default::default()
                    });
                }
            };

            if !consume_resp_send {
                consume_resp_send = true;
                match encode_response_header(
                    pb::ErrorCode::None,
                    None,
                    Some(pb::ConsumeResponse {
                        first_offset: offset,
                    }),
                ) {
                    Ok(resp) => {
                        if let Err(e) = stream.write(&resp).await.context(error::IO {
                            e: "sending response header",
                        }) {
                            return Some(pb::ResponseHeader {
                                error: pb::ErrorCode::ProduceError as i32,
                                error_description: Some(e.to_string()),
                                response: None,
                            });
                        }
                    }
                    Err(e) => {
                        return Some(pb::ResponseHeader {
                            error: pb::ErrorCode::FetchError as i32,
                            error_description: Some(e.to_string()),
                            ..Default::default()
                        });
                    }
                };
            }

            if let Err(e) = stream.write(&msg).await.context(error::IO {
                e: "writing message to stream",
            }) {
                return Some(pb::ResponseHeader {
                    error: pb::ErrorCode::ProduceError as i32,
                    error_description: Some(e.to_string()),
                    response: None,
                });
            }
            eprintln!("wrote message of {} bytes", msg.len());
            msg_count += 1;
            if let Some(max) = req.max_messages {
                if msg_count >= max {
                    return None;
                }
            }
        }
    }

    async fn req_produce(
        &self,
        req: impl Buf,
        mut stream: impl AsyncRead + Unpin,
    ) -> Option<pb::ResponseHeader> {
        let req = if let Ok(v) = pb::ProduceRequest::decode_length_delimited(req) {
            v
        } else {
            return Some(pb::ResponseHeader {
                error: pb::ErrorCode::InvalidRequest as i32,
                ..Default::default()
            });
        };
        eprintln!("got req {req:?}");
        let tp = TopicPartition::new(&req.topic, &req.partition);

        let io_worker = match self.io_chans.get(&tp) {
            Some(v) => v,
            None => {
                return Some(pb::ResponseHeader {
                    error: pb::ErrorCode::TopicPartitionNotFound as i32,
                    error_description: Some(format!(
                        "topic {} partition {} not found",
                        tp.topic, tp.partition
                    )),
                    ..Default::default()
                });
            }
        };

        if req.batch_size == 0 {
            return Some(pb::ResponseHeader {
                error: pb::ErrorCode::None as i32,
                ..Default::default()
            });
        }

        let mut first_offset = None;
        for _ in 0..req.batch_size {
            let msg = match util::read_delimited_message(&mut stream).await {
                Ok(v) => v,
                Err(e) => {
                    return Some(pb::ResponseHeader {
                        error: pb::ErrorCode::InvalidRequest as i32,
                        error_description: Some(format!("reading message: {e}")),
                        ..Default::default()
                    });
                }
            };

            let (tx, rx) = oneshot::channel();
            let tp = tp.clone();
            let offset = match io_worker.send((IORequest::Put { tp, msg }, tx)).await {
                Ok(_) => match rx.await {
                    Ok(resp) => match resp {
                        Ok(IOReply::Put(offset)) => offset,
                        Ok(_) => {
                            panic!("unexpected reply from IO worker")
                        }
                        Err(e) => {
                            return Some(pb::ResponseHeader {
                                error: pb::ErrorCode::ProduceError as i32,
                                error_description: Some(e.to_string()),
                                ..Default::default()
                            });
                        }
                    },
                    Err(e) => {
                        return Some(pb::ResponseHeader {
                            error: pb::ErrorCode::ProduceError as i32,
                            error_description: Some(e.to_string()),
                            ..Default::default()
                        });
                    }
                },
                Err(e) => {
                    return Some(pb::ResponseHeader {
                        error: pb::ErrorCode::ProduceError as i32,
                        error_description: Some(e.to_string()),
                        ..Default::default()
                    });
                }
            };
            if first_offset.is_none() {
                first_offset = Some(offset);
            }
        }

        let mut buf = Vec::with_capacity(8);
        match (pb::ProduceResponse {
            first_offset: first_offset.unwrap(),
        })
        .encode_length_delimited(&mut buf)
        {
            Ok(_) => Some(pb::ResponseHeader {
                error: pb::ErrorCode::None as i32,
                error_description: None,
                response: Some(buf),
            }),
            Err(e) => Some(pb::ResponseHeader {
                error: pb::ErrorCode::ProduceError as i32,
                error_description: Some(format!("encoding result: {e}")),
                ..Default::default()
            }),
        }
    }
}
