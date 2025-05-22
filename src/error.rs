use snafu::prelude::*;

use crate::pb;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(
        display("topic/partition {topic}/{partition} not found"),
        context(suffix(false))
    )]
    TopicPartitionNotFound { topic: String, partition: u32 },

    #[snafu(display("invalid request"), context(suffix(false)))]
    InvalidRequest,

    #[snafu(display("offset not found"), context(suffix(false)))]
    OffsetNotFound,

    #[snafu(display("IO error: {e}"), context(suffix(false)))]
    IO { e: String, source: std::io::Error },

    #[snafu(display("IO error: {e}"), context(suffix(false)))]
    Glommio {
        e: String,
        source: glommio::GlommioError<()>,
    },

    #[snafu(display("PD decoding error: {e}"), context(suffix(false)))]
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

impl Into<pb::ErrorCode> for Error {
    fn into(self) -> pb::ErrorCode {
        todo!()
    }
}
