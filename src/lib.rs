pub mod error;
pub mod node;
pub mod util;
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/toob.protocol.rs"));
}

#[derive(Clone, Hash, Debug, Eq, PartialEq)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: u32,
}
