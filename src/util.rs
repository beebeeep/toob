use crate::error::{self, Error};
use crate::pb;
use futures_lite::{AsyncRead, AsyncReadExt};
use prost::Message;
use snafu::ResultExt;

/// maximum length of varint encoding message size, 4 bytes are enough to encode 256Mi - 1
const MAX_MESSAGE_LEN: usize = 4;

/// Reads length of message (encoded as varint), then message itself. Returned buffer includes original length header.
pub async fn read_delimited_message(
    mut stream: impl AsyncRead + Unpin,
) -> Result<Vec<u8>, error::Error> {
    let mut buf = Vec::with_capacity(512);
    // each message is prepended with varint (from 1 to 10 bytes) encoding its length
    // we are reading only up to MAX_MESSAGE_LEN bytes to simplify handling of really short messages
    // TODO get rid of this limit maybe?
    buf.resize(MAX_MESSAGE_LEN, 0);
    stream.read_exact(&mut buf).await.context(error::IO {
        e: "reading message length",
    })?;
    let len = prost::decode_length_delimiter(buf.as_ref()).context(error::Decode {
        e: "decoding message length",
    })?;
    let delim = prost::length_delimiter_len(len);

    // we already read MASX_MESSAGE_LEN bytes, read the rest
    buf.resize(len + delim, 0);
    stream
        .read_exact(&mut buf[MAX_MESSAGE_LEN..])
        .await
        .context(error::IO {
            e: "reading message",
        })?;
    Ok(buf)
}

pub fn encode_response_header(
    error: pb::ErrorCode,
    error_description: Option<&str>,
    msg: Option<impl prost::Message>,
) -> Result<Vec<u8>, Error> {
    let response = match msg {
        None => None,
        Some(msg) => {
            let mut buf = Vec::with_capacity(32);
            msg.encode_length_delimited(&mut buf)
                .context(error::Encode {
                    e: "encoding response message",
                })?;
            Some(buf)
        }
    };

    let mut buf = Vec::with_capacity(64);
    (pb::ResponseHeader {
        error: error as i32,
        error_description: error_description.map(|x| String::from(x)),
        response,
    })
    .encode_length_delimited(&mut buf)
    .context(error::Encode {
        e: "encoding response header",
    })?;

    Ok(buf)
}

pub fn encode_request_header(
    request_id: pb::Request,
    req: Option<impl prost::Message>,
) -> Result<Vec<u8>, Error> {
    let mut hdr_buf = Vec::with_capacity(10);
    let mut req_buf = Vec::with_capacity(32);

    if let Some(req) = req {
        req.encode_length_delimited(&mut req_buf)
            .context(error::Encode {
                e: "encoding request",
            })?;
    }

    pb::RequestHeader {
        request_id: request_id as i32,
        request: req_buf,
    }
    .encode_length_delimited(&mut hdr_buf)
    .context(error::Encode {
        e: "encoding request",
    })?;

    Ok(hdr_buf)
}

pub async fn read_response_header<T: prost::Message + Default>(
    mut stream: impl AsyncRead + Unpin,
) -> Result<Option<T>, error::Error> {
    let resp = pb::ResponseHeader::decode_length_delimited(
        read_delimited_message(&mut stream).await?.as_ref(),
    )
    .context(error::Decode {
        e: "decoding response",
    })?;

    let resp = match pb::ErrorCode::try_from(resp.error) {
        Ok(pb::ErrorCode::None) => resp.response,
        Ok(e) => {
            return error::ServerResponse {
                e,
                desc: resp.error_description,
            }
            .fail();
        }
        Err(_) => {
            return error::Other {
                e: format!("unknown server error code: {}", resp.error),
            }
            .fail();
        }
    };
    match resp {
        Some(resp) => Ok(Some(T::decode_length_delimited(resp.as_ref()).context(
            error::Decode {
                e: "decoding response",
            },
        )?)),
        None => Ok(None),
    }
}
