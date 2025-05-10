use std::rc::Rc;

use anyhow::{Context, Result, anyhow};
use bytes::{Buf, BytesMut};
use futures_lite::{AsyncRead, AsyncReadExt, AsyncWriteExt, StreamExt};
use glommio::{
    ByteSliceExt,
    io::{DmaStreamWriterBuilder, OpenOptions},
    net::TcpStream,
};
use prost::Message;

use crate::pb;
#[derive(Clone)]
pub struct Server {
    log_dir: Rc<String>,
}

impl Server {
    pub fn new(log_dir: &str) -> Self {
        Self {
            log_dir: Rc::new(log_dir.to_string()),
        }
    }

    pub async fn process_request(&self, mut stream: &mut TcpStream) -> Result<()> {
        // incoming request starts with Header protobuf message prepended with varint-encoded length
        let header = self
            .read_delimited_message(&mut stream)
            .await
            .context("reading header")?;
        let header = pb::Header::decode(header.as_ref()).context("decoding header")?;

        match header.request_id.try_into() {
            Ok(pb::Request::Consume) => self
                .req_consume(header.request.as_ref(), stream)
                .await
                .context("handling consume request")?,
            Ok(pb::Request::Produce) => self
                .req_produce(header.request.as_ref(), stream)
                .await
                .context("handling produce request")?,
            Ok(pb::Request::Noop) => {}
            Err(e) => eprintln!("unknown api request {e}"),
        };
        Ok(())
    }

    async fn req_consume(&self, _req: impl Buf, _stream: impl AsyncRead) -> Result<()> {
        todo!();
    }

    async fn req_produce(&self, req: impl Buf, mut stream: impl AsyncRead + Unpin) -> Result<()> {
        let req = pb::ProduceRequest::decode_length_delimited(req).context("decoding request")?;
        eprintln!("got req {req:?}");
        let f = match OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .buffered_open(format!(
                "{}/{}/{}.log",
                self.log_dir, req.topic, req.partition
            ))
            .await
        {
            Ok(f) => f,
            Err(e) => return Err(anyhow!("opening partition file: {e}")),
        };

        for _ in 0..req.batch_size {
            let msg = self
                .read_delimited_message(&mut stream)
                .await
                .context("reading message")?;
            if let Err(e) = f.write_at(msg, 0).await {
                return Err(anyhow!("writing message: {e}"));
            }
        }
        if let Err(e) = f.close().await {
            return Err(anyhow!("writing message: {e}"));
        }
        Ok(())
    }

    async fn read_delimited_message(&self, mut stream: impl AsyncRead + Unpin) -> Result<Vec<u8>> {
        // each message is prepended with varint (from 1 to 10 bytes) encoding its length
        let mut len_buf = BytesMut::from(&[0; 10][..]);
        stream
            .read_exact(&mut len_buf)
            .await
            .context("reading message length")?;
        let len =
            prost::decode_length_delimiter(&mut len_buf).context("decoding message length")?;

        // we already read 10 bytes, but not all of those are related to length,
        // likely we already read few bytes of message itself, so chain the remainder
        let mut buf = vec![0; len];
        AsyncReadExt::chain(len_buf.as_ref(), stream)
            .read_exact(&mut buf)
            .await
            .context("reading header")?;
        Ok(buf)
    }
}
