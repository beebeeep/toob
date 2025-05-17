use anyhow::{Context, Result};
use bytes::BytesMut;
use futures_lite::{AsyncRead, AsyncReadExt};

/*
pub async fn read_delimited_message(mut stream: impl AsyncRead + Unpin) -> Result<Vec<u8>> {
    // each message is prepended with varint (from 1 to 10 bytes) encoding its length
    let mut len_buf = BytesMut::from(&[0; 10][..]);
    stream
        .read_exact(&mut len_buf)
        .await
        .context("reading message length")?;
    let len = prost::decode_length_delimiter(&mut len_buf).context("decoding message length")?;

    // we already read 10 bytes, but not all of those are related to length,
    // likely we already read few bytes of message itself, so chain the remainder
    let delim = prost::length_delimiter_len(len);
    eprintln!("got message len {len}, delimiter {delim}");
    let mut buf = Vec::with_capacity(len + delim);
    // let mut buf = BytesMut::with_capacity(len + delim);
    prost::encode_length_delimiter(len, &mut buf).unwrap();
    eprintln!("read: {:?}", buf);
    buf.resize(len + delim, 0);
    AsyncReadExt::chain(len_buf.as_ref(), stream)
        .read_exact(&mut buf[delim..])
        .await
        .context("reading header")?;
    eprintln!("read: {:?}", buf);
    Ok(buf)
}
*/

pub async fn read_delimited_message(mut stream: impl AsyncRead + Unpin) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(512);
    // each message is prepended with varint (from 1 to 10 bytes) encoding its length
    buf.resize(10, 0);
    stream
        .read_exact(&mut buf)
        .await
        .context("reading message length")?;
    let len = prost::decode_length_delimiter(buf.as_ref()).context("decoding message length")?;
    let delim = prost::length_delimiter_len(len);

    // we already read 10 bytes, read the rest
    buf.resize(len + delim, 0);
    stream
        .read_exact(&mut buf[10..])
        .await
        .context("reading message")?;
    Ok(buf)
}
