use async_channel::Receiver;
use snafu::ResultExt;
use std::collections::{BTreeMap, HashMap};

use crate::{
    IOMessage, IOReply, IORequest, TopicPartition,
    error::{self, Error},
};
use glommio::io::{BufferedFile, OpenOptions, ReadResult};

#[derive(Debug, Clone, Copy, Default)]
struct MessageMeta {
    file_offset: u64,
    size: usize,
}

pub struct Server {
    partition_files: HashMap<TopicPartition, BufferedFile>,
    offset_index: HashMap<TopicPartition, BTreeMap<u64, MessageMeta>>,
    tx: Receiver<IOMessage>,
}

impl Server {
    pub async fn new(
        partitions: Vec<(TopicPartition, String)>,
        tx: Receiver<IOMessage>,
    ) -> Result<Self, Error> {
        let mut partition_files = HashMap::new();
        let mut offset_index = HashMap::new();

        // open all partition files and scan them to build offset index mapping logical offset to offset in file
        for (partition, path) in partitions {
            let partition_file = OpenOptions::new()
                .write(true)
                .read(true)
                .append(true)
                .buffered_open(path)
                .await
                .context(error::Glommio {
                    e: "opening partition file: {e}",
                })?;

            let mut idx = BTreeMap::new();
            let mut file_offset = 0;
            let mut offset = 0;
            let file_size = partition_file.file_size().await.context(error::Glommio {
                e: "getting partition file size",
            })?;
            while file_offset < file_size {
                // read length of message and shift file offset by it
                let nibble =
                    partition_file
                        .read_at(file_offset, 10)
                        .await
                        .context(error::Glommio {
                            e: "reading partition",
                        })?;
                let mut size =
                    prost::decode_length_delimiter(nibble.as_ref()).context(error::Decode {
                        e: "decoding message length",
                    })?;
                size += prost::length_delimiter_len(size); // include length delimiter itself
                idx.insert(offset, MessageMeta { file_offset, size });
                eprintln!(
                    "indexed message of {size} bytes with offset {offset} at file offset {file_offset}"
                );
                offset += 1;
                file_offset += size as u64;
            }

            offset_index.insert(partition.clone(), idx);
            partition_files.insert(partition, partition_file);
        }

        Ok(Self {
            partition_files,
            offset_index,
            tx,
        })
    }

    pub async fn serve(&mut self) -> Result<(), Error> {
        loop {
            match self.tx.recv().await.context(error::IORecv)? {
                (IORequest::Put { tp, msg }, resp) => {
                    eprintln!("got PUT to {tp}, {} bytes", msg.len());
                    let r = self.write_message(&tp, msg).await.map(|v| IOReply::Put(v));
                    if let Err(e) = resp.send(r) {
                        eprintln!("error sending PUT reply: {e}");
                    }
                }
                (IORequest::Get { tp, offset }, resp) => {
                    eprintln!("got GET at {tp}@{offset}");
                    let r = self
                        .fetch_message(&tp, &offset)
                        .await
                        // I really don't like that we have to copy ReadResult into Vec inside hot path,
                        // but I failed to find better solution on how to pass it to network thread
                        .map(|v| IOReply::Get(v.to_vec()));
                    if let Err(e) = resp.send(r) {
                        eprintln!("error sending GET reply: {e}");
                    }
                }
            }
        }
    }

    async fn write_message(&mut self, tp: &TopicPartition, message: Vec<u8>) -> Result<u64, Error> {
        let msg_len = message.len();
        let idx = self
            .offset_index
            .get_mut(tp)
            .ok_or(Error::TopicPartitionNotFound {
                topic: tp.topic.clone(),
                partition: tp.partition,
            })?;

        let (last_offset, last_meta) = match idx.last_key_value() {
            Some((k, v)) => (Some(k), Some(v)),
            None => (None, None),
        };

        // write message to partition file (it is opened in append mode)
        let f = self
            .partition_files
            .get(tp)
            .expect("getting partition file");
        f.write_at(message, 0).await.context(error::Glommio {
            e: "writing to partition file",
        })?;

        // update offset indexes
        let (message_offset, message_meta) = (
            last_offset.map_or(0, |x| x + 1),
            MessageMeta {
                file_offset: last_meta.map_or(0, |x| x.file_offset + x.size as u64),
                size: msg_len,
            },
        );
        idx.insert(message_offset, message_meta);

        // TODO: signal consumers that new message is available?

        Ok(message_offset)
    }

    async fn fetch_message(&self, tp: &TopicPartition, offset: &u64) -> Result<ReadResult, Error> {
        let meta = {
            self.offset_index
                .get(tp)
                .ok_or(Error::TopicPartitionNotFound {
                    topic: tp.topic.clone(),
                    partition: tp.partition,
                })?
                .get(&offset)
                .ok_or(Error::OffsetNotFound)? // TODO: if offset is not YET found, mb we can wait here until it'll be written?
        };
        let f = self
            .partition_files
            .get(tp)
            .ok_or(Error::TopicPartitionNotFound {
                topic: tp.topic.clone(),
                partition: tp.partition,
            })?;

        f.read_at(meta.file_offset, meta.size as usize)
            .await
            .context(error::Glommio {
                e: "reading partition",
            })
    }
}
