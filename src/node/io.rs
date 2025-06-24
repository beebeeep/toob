use async_channel::Receiver;
use snafu::ResultExt;
use std::collections::{BTreeMap, HashMap};

use crate::{
    IOMessage, IOReply, IORequest, TopicPartition,
    error::{self, Error},
};
use glommio::io::{BufferedFile, OpenOptions};

#[derive(Debug, Clone, Copy)]
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

    pub async fn serve(&self) -> Result<(), Error> {
        match self.tx.recv().await.context(error::IORecv)? {
            (IORequest::Put { tp, data }, resp) => {
                eprintln!("got PUT to {tp}, {} bytes", data.len());
                if let Err(e) = resp.send(Ok(IOReply::Put(0))) {
                    eprintln!("error sending PUT reply: {e}");
                }
            }
            (IORequest::Get { tp, offset }, resp) => {
                eprintln!("got GET at {tp}/{offset}");
                if let Err(e) = resp.send(Ok(IOReply::Get(Vec::new()))) {
                    eprintln!("error sending GET reply: {e}");
                }
            }
        }
        Ok(())
    }
}
