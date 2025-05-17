use std::{cell::RefCell, ops::Deref, rc::Rc};

use crate::{pb, util};
use anyhow::{Context, Result, anyhow};
use bytes::{Buf, BufMut, BytesMut};
use futures_lite::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use glommio::{
    io::{BufferedFile, OpenOptions, ReadResult},
    net::TcpStream,
};
use prost::Message;
use std::collections::{BTreeMap, HashMap};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("topic/partition not found")]
    TopicPartitionNotFound,
    #[error("offset not found")]
    OffsetNotFound,
}

type TopicPartition = (String, u32);

#[derive(Debug, Clone, Copy)]
struct MessageMeta {
    file_offset: u64,
    size: usize,
}

#[derive(Clone)]
pub struct Server {
    partition_files: Rc<HashMap<TopicPartition, BufferedFile>>,
    offset_index: Rc<RefCell<HashMap<TopicPartition, BTreeMap<u64, MessageMeta>>>>,
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
                idx.insert(offset, MessageMeta { file_offset, size });
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
            offset_index: Rc::new(RefCell::new(offset_index)),
        })
    }

    pub async fn process_request(&self, mut stream: &mut TcpStream) -> Result<()> {
        // incoming request starts with Header protobuf message prepended with varint-encoded length
        let header = util::read_delimited_message(&mut stream)
            .await
            .context("reading header")?;
        let header =
            pb::Header::decode_length_delimited(header.as_ref()).context("decoding header")?;

        match header.request_id.try_into() {
            Ok(pb::Request::Consume) => self
                .req_consume(header.request.as_ref(), stream)
                .await
                .context("handling consume request")?,
            Ok(pb::Request::Produce) => self
                .req_produce(header.request.as_ref(), stream)
                .await
                .context("handling produce request")?,
            Ok(pb::Request::Noop) => {
                eprintln!("got noop")
            }
            Err(e) => eprintln!("unknown api request {e}"),
        };
        Ok(())
    }

    async fn req_consume(
        &self,
        req: impl Buf,
        mut stream: impl AsyncRead + AsyncWrite + Unpin,
    ) -> Result<()> {
        let req = pb::ConsumeRequest::decode_length_delimited(req).context("decoding request")?;
        eprintln!("got req {req:?}");
        let mut msg_count = 0;
        let id = (req.topic, req.partition);
        loop {
            let msg = self
                .read_message_at_offset(&id, req.start_offset + msg_count)
                .await?;
            stream
                .write(msg.as_ref())
                .await
                .context("writing message to stream")?;
            eprintln!("wrote message of {} bytes", msg.len());
            msg_count += 1;
            if let Some(max) = req.max_messages {
                if msg_count >= max {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn req_produce(&self, req: impl Buf, mut stream: impl AsyncRead + Unpin) -> Result<()> {
        let req = pb::ProduceRequest::decode_length_delimited(req).context("decoding request")?;
        eprintln!("got req {req:?}");
        let id = (req.topic, req.partition); // TODO: writes should be sharded between IO threads to ensure nobody is writing to partition logs concurrently
        let f = match self.partition_files.get(&id) {
            Some(f) => f,
            None => return Err(anyhow!("no such topic/partition")),
        };
        let mut file_offset = f
            .file_size()
            .await
            .map_err(|e| anyhow!("getting partition size: {e}"))?;

        let mut offsets = Vec::with_capacity(req.batch_size as usize);
        for _ in 0..req.batch_size {
            let msg = util::read_delimited_message(&mut stream)
                .await
                .context("reading message")?;
            let msg_len = msg.len();
            eprintln!("got message: {:?}", msg);
            if let Err(e) = f.write_at(msg, 0).await {
                return Err(anyhow!("writing message: {e}"));
            }
            offsets.push(MessageMeta {
                file_offset,
                size: msg_len,
            });
            file_offset += msg_len as u64;
        }
        if let Err(e) = f.fdatasync().await {
            return Err(anyhow!("doing fsync: {e}"));
        }

        // update index
        {
            let mut offset_index = self.offset_index.borrow_mut();
            let index = offset_index.get_mut(&id).unwrap();
            let next = match index.last_entry() {
                Some(x) => *x.key() + 1,
                None => 0,
            };
            offsets.into_iter().enumerate().for_each(|(i, o)| {
                index.insert(next + i as u64, o);
            });
            eprintln!("updated index:");
            for (k, v) in index {
                eprintln!("{k} => {v:?}");
            }
        }
        Ok(())
    }

    async fn read_message_at_offset(&self, id: &TopicPartition, offset: u64) -> Result<ReadResult> {
        let meta = {
            let index = self.offset_index.borrow();
            *index
                .get(id)
                .ok_or(Error::TopicPartitionNotFound)?
                .get(&offset)
                .ok_or(Error::OffsetNotFound)?
        };
        let f = self
            .partition_files
            .get(id)
            .ok_or(Error::TopicPartitionNotFound)?;

        f.read_at(meta.file_offset, meta.size as usize)
            .await
            .map_err(|e| anyhow!("reading partition: {e}"))
    }
}
