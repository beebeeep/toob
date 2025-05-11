use std::any::Any;
use std::rc::Rc;

use anyhow::{Context, Result, anyhow};
use bytes::{Buf, BytesMut};
use futures_lite::{AsyncRead, AsyncReadExt, AsyncWriteExt, StreamExt};
use glommio::{
    ByteSliceExt,
    io::{BufferedFile, DmaStreamWriterBuilder, OpenOptions, ReadResult},
    net::TcpStream,
};
use prost::Message;
use std::collections::{BTreeMap, HashMap};

use crate::pb;

type TopicPartition = (String, u32);
struct MessageMeta {
    file_offset: u64,
    size: u64,
}

#[derive(Clone)]
pub struct Server {
    partition_files: Rc<HashMap<TopicPartition, BufferedFile>>,
    offset_index: Rc<HashMap<TopicPartition, BTreeMap<u64, MessageMeta>>>,
}

impl Server {
    pub async fn new(log_dir: &str) -> Result<Self> {
        // discover and open all partition files
        let mut files = HashMap::new();
        for topic_entry in std::fs::read_dir(log_dir).context("reading log dir")? {
            let topic_dir = topic_entry.context("getting log dir entry")?;
            if !topic_dir
                .file_type()
                .context("getting log entry type")?
                .is_dir()
            {
                continue;
            }
            let topic = topic_dir.file_name().into_string().unwrap();
            let topic_path = std::path::Path::new(log_dir).join(topic_dir.file_name());

            for partition_entry in
                std::fs::read_dir(&topic_path).context("reading partition dir")?
            {
                let partition = partition_entry.context("reading partition dir entry")?;
                let fname = partition.file_name().into_string().unwrap();
                let ft = partition
                    .file_type()
                    .context("getting partition dir entry type")?;
                if !fname.ends_with(".log") || !ft.is_file() {
                    continue;
                }
                let partition = fname
                    .strip_suffix(".log")
                    .unwrap()
                    .parse()
                    .context("wrong partition name")?;

                let path = std::path::Path::new(&topic_path).join(fname);
                let file = match OpenOptions::new()
                    .write(true)
                    .read(true)
                    .append(true)
                    .buffered_open(path)
                    .await
                {
                    Ok(f) => f,
                    Err(e) => return Err(anyhow!("opening partition file: {e}")),
                };

                files.insert((topic.clone(), partition), file);
                eprintln!("found topic {topic} partition {partition}");
            }
        }

        // now read each partition and build index mapping logical offset to offset in file
        let mut offset_index = HashMap::new();
        for (topic_partition, partition) in files.iter() {
            let mut idx = BTreeMap::new();
            let mut file_offset = 0;
            let mut offset = 0;
            let file_size = match partition.file_size().await {
                Ok(s) => s,
                Err(e) => return Err(anyhow!("getting partition file size: {e}")),
            };
            while file_offset < file_size {
                let nibble = match partition.read_at(file_offset, 10).await {
                    Ok(r) => r,
                    Err(e) => return Err(anyhow!("reading partition: {e}")),
                };
                let mut size = prost::decode_length_delimiter(nibble.as_ref())
                    .context("decoding message length")?;
                size += prost::length_delimiter_len(size); // include length delimiter itself
                idx.insert(
                    offset,
                    MessageMeta {
                        file_offset,
                        size: size as u64,
                    },
                );
                eprintln!(
                    "indexed message of {size} bytes with offset {offset} at file offset {file_offset}"
                );
                offset += 1;
                file_offset += size as u64 + 1;
            }

            offset_index.insert(topic_partition.clone(), idx);
        }

        Ok(Self {
            partition_files: Rc::new(files),
            offset_index: Rc::new(offset_index),
        })
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

    async fn req_consume(&self, req: impl Buf, stream: impl AsyncRead) -> Result<()> {
        let req = pb::ConsumeRequest::decode_length_delimited(req).context("decoding request")?;
        eprintln!("got req {req:?}");
        let f = match self.partition_files.get(&(req.topic, req.partition)) {
            Some(f) => f,
            None => return Err(anyhow!("no such topic/partition")),
        };
        let mut msg_count = 0;
        loop {
            // let msg = self.read_delimited_message(f).await.context("reading message from disk")?
        }
        Ok(())
    }

    async fn req_produce(&self, req: impl Buf, mut stream: impl AsyncRead + Unpin) -> Result<()> {
        let req = pb::ProduceRequest::decode_length_delimited(req).context("decoding request")?;
        eprintln!("got req {req:?}");
        let f = match self.partition_files.get(&(req.topic, req.partition)) {
            Some(f) => f,
            None => return Err(anyhow!("no such topic/partition")),
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
        if let Err(e) = f.fdatasync().await {
            return Err(anyhow!("doing fsync: {e}"));
        }
        // !!!! TODO !!! update offset index here
        Ok(())
    }

    async fn read_message_at_offset(&self, id: &TopicPartition) -> Result<ReadResult> {
        let file_offset = match self.offset_index.get(id) {
            Some(v) => todo!(),
            None => return Err(anyhow!("no message at this offset")),
        };
        todo!();
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
