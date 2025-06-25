use snafu::prelude::*;

use crate::pb;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
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

    #[snafu(display("PB decoding error: {e}"), context(suffix(false)))]
    Decode {
        e: String,
        source: prost::DecodeError,
    },

    #[snafu(display("PB encoding error: {e}"), context(suffix(false)))]
    Encode {
        e: String,
        source: prost::EncodeError,
    },

    #[snafu(display("IO thread request"), context(suffix(false)))]
    IORecv { source: async_channel::RecvError },
}

impl Into<pb::ErrorCode> for Error {
    fn into(self) -> pb::ErrorCode {
        todo!()
    }
}
