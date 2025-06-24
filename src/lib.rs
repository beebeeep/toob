use std::{fmt::Display, str::FromStr};

use error::Error;

pub mod error;
pub mod node;
pub mod util;
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/toob.protocol.rs"));
}

pub enum IORequest {
    Put { tp: TopicPartition, data: Vec<u8> },
    Get { tp: TopicPartition, offset: u64 },
}

pub enum IOReply {
    Put(u64),
    Get(Vec<u8>),
}

// IOMessage is type encapsulating request to IO workers with reply channel
pub type IOMessage = (IORequest, oneshot::Sender<Result<IOReply, Error>>);

#[derive(Clone, Hash, Debug, Eq, PartialEq)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: u32,
}

impl TopicPartition {
    fn new(topic: &str, partition: &u32) -> Self {
        Self {
            topic: String::from(topic),
            partition: *partition,
        }
    }
}

impl Display for TopicPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}
