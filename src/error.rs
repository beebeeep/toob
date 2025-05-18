use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("topic/partition {topic}/{partition} not found"))]
    TopicPartitionNotFound { topic: String, partition: u32 },

    #[snafu(display("offset not found"))]
    OffsetNotFound,

    #[snafu(display("IO error: {e}"))]
    IO { e: String, source: std::io::Error },

    #[snafu(display("IO error: {e}"))]
    Glommio {
        e: String,
        source: glommio::GlommioError<()>,
    },

    #[snafu(display("PD decoding error: {e}"))]
    Decode {
        e: String,
        source: prost::DecodeError,
    },

    #[snafu(whatever, display("{message}: {source:?}"))]
    GenericError {
        message: String,

        #[snafu(source(from(Box<dyn std::error::Error>, Some)))]
        source: Option<Box<dyn std::error::Error>>,
    },
}
