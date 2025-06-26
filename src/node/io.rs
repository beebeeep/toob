use async_broadcast as ab;
use async_channel as ac;
use snafu::ResultExt;
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    rc::Rc,
};

use crate::{
    IOMessage, IOReply, IORequest, TopicPartition,
    error::{self, Error},
};
use glommio::{
    io::{BufferedFile, OpenOptions, ReadResult},
    spawn_local,
};

#[derive(Debug, Clone, Copy, Default)]
struct MessageMeta {
    file_offset: u64,
    size: usize,
}

#[derive(Clone)]
pub struct Server {
    /// Mapping of topic-partitions to partition file opened in APPEND mode
    partition_files: Rc<HashMap<TopicPartition, BufferedFile>>,

    /// Broadcast channels that is used by consumers to wait for new message to appear in partition.
    /// Each write to partition emits message to relevant channel.
    partition_signals: Rc<HashMap<TopicPartition, (ab::Sender<i32>, ab::Receiver<i32>)>>,

    /// Offset indexes map logical message offset to its metadata - message size and physical offset in partition file
    offset_index: Rc<RefCell<HashMap<TopicPartition, BTreeMap<u64, MessageMeta>>>>,

    /// Channel for receiving requests from network threads
    tx: ac::Receiver<IOMessage>,
}

impl Server {
    pub async fn new(
        partitions: Vec<(TopicPartition, String)>,
        tx: ac::Receiver<IOMessage>,
    ) -> Result<Self, Error> {
        let mut partition_files = HashMap::new();
        let mut offset_index = HashMap::new();
        let mut partition_signals = HashMap::new();

        // open all partition files and scan them to build offset index mapping logical offset to offset in file
        for (partition, path) in partitions {
            let (mut sig_tx, sig_rx) = ab::broadcast(1);
            sig_tx.set_await_active(false);
            sig_tx.set_overflow(true);
            partition_signals.insert(partition.clone(), (sig_tx, sig_rx));

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
            partition_files: Rc::new(partition_files),
            partition_signals: Rc::new(partition_signals),
            offset_index: Rc::new(RefCell::new(offset_index)),
            tx,
        })
    }

    pub async fn serve(&mut self) -> Result<(), Error> {
        loop {
            let req = self.tx.recv().await.context(error::IORecv)?;

            spawn_local(self.clone().handle_request(req)).detach();
        }
    }

    async fn handle_request(mut self, req: (IORequest, oneshot::Sender<Result<IOReply, Error>>)) {
        match req {
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

    async fn write_message(&mut self, tp: &TopicPartition, msg: Vec<u8>) -> Result<u64, Error> {
        let msg_len = msg.len();

        // write message to partition file (it was opened in append mode)
        let f = self
            .partition_files
            .get(tp)
            .expect("getting partition file");
        f.write_at(msg, 0).await.context(error::Glommio {
            e: "writing to partition file",
        })?;

        // update offset indexes
        let mut offset_index = self.offset_index.borrow_mut();
        let idx = offset_index
            .get_mut(tp)
            .ok_or(Error::TopicPartitionNotFound {
                topic: tp.topic.clone(),
                partition: tp.partition,
            })?;

        let (last_offset, last_meta) = match idx.last_key_value() {
            Some((k, v)) => (Some(k), Some(v)),
            None => (None, None),
        };

        let (message_offset, message_meta) = (
            last_offset.map_or(0, |x| x + 1),
            MessageMeta {
                file_offset: last_meta.map_or(0, |x| x.file_offset + x.size as u64),
                size: msg_len,
            },
        );
        idx.insert(message_offset, message_meta);
        eprintln!(
            "wrote message {msg_len} to {tp} at offset {message_offset} with physical offset {} ",
            message_meta.file_offset
        );

        // broadcast any potential consumers that we just wrote a new message to partition
        let (tx, _) = self.partition_signals.get(tp).unwrap();
        let _ = tx.clone().broadcast(1).await;

        Ok(message_offset)
    }

    async fn fetch_message(&self, tp: &TopicPartition, offset: &u64) -> Result<ReadResult, Error> {
        let meta = {
            match {
                // ensure that scope of idx is small so we don't hold offset_index for more than necessary
                let idx = self.offset_index.borrow();
                idx.get(tp)
                    .ok_or(Error::TopicPartitionNotFound {
                        topic: tp.topic.clone(),
                        partition: tp.partition,
                    })?
                    .get(&offset)
                    .map(|x| x.clone())
            } {
                Some(m) => m,
                None => self.wait_for_message(tp, offset).await,
            }
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

    async fn wait_for_message(&self, tp: &TopicPartition, offset: &u64) -> MessageMeta {
        // offset index doesn't have requested partition, we will wait for some producer to write a new message and signal us
        let (_, signal) = self.partition_signals.get(tp).unwrap();
        let mut signal = signal.clone();
        loop {
            let _ = signal.recv().await;
            let idx = self.offset_index.borrow();
            if let Some(m) = idx.get(tp).unwrap().get(&offset) {
                return m.clone();
            }
        }
    }
}
