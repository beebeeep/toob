use crate::error;
use futures_lite::{AsyncRead, AsyncReadExt};
use snafu::ResultExt;

/// Reads length of message (encoded as varint), then message itself. Returned buffer includes original length header.
pub async fn read_delimited_message(
    mut stream: impl AsyncRead + Unpin,
) -> Result<Vec<u8>, error::Error> {
    let mut buf = Vec::with_capacity(512);
    // each message is prepended with varint (from 1 to 10 bytes) encoding its length
    buf.resize(10, 0);
    stream.read_exact(&mut buf).await.context(error::IO {
        e: "reading message length",
    })?;
    let len = prost::decode_length_delimiter(buf.as_ref()).context(error::Decode {
        e: "decoding message length",
    })?;
    let delim = prost::length_delimiter_len(len);

    // we already read 10 bytes, read the rest
    buf.resize(len + delim, 0);
    stream.read_exact(&mut buf[10..]).await.context(error::IO {
        e: "reading message",
    })?;
    Ok(buf)
}
