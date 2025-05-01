mod binary;
mod errors;
mod request_api;

use glommio::{
    LocalExecutor, LocalExecutorBuilder,
    net::{TcpListener, TcpStream},
    prelude::*,
};

use futures_lite::{AsyncReadExt, AsyncWriteExt, stream::StreamExt};
use std::io;

use anyhow::{Context, anyhow};
use binary::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytes_varint::VarIntSupportMut;
use errors::KafkaError;
use request_api::RequestApi;

#[derive(Debug)]
struct KafkaRequestHeaderV2 {
    request: RequestApi,
    version: i16,
    correlation_id: i32,
    client: Option<String>,
}

async fn server() -> Result<(), io::Error> {
    let listener = TcpListener::bind("127.0.0.1:9092")?;
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        match stream {
            Ok(mut stream) => {
                spawn_local(async move {
                    match process_request(&mut stream).await {
                        Ok(_) => {}
                        Err(e) => eprintln!("error processing request: {e:#}"),
                    }
                })
                .detach();
            }
            Err(e) => eprintln!("error accepting connection: {e}"),
        }
    }
    Ok(())
}

fn main() {
    let exec_builder = LocalExecutorBuilder::new(glommio::Placement::Fixed(1));
    let server = exec_builder
        .name("server")
        .spawn(|| async { server().await })
        .unwrap();
    server.join().unwrap().unwrap();
}

async fn process_request(stream: &mut TcpStream) -> anyhow::Result<()> {
    let mut buf = [0u8; 4];
    stream
        .read_exact(&mut buf)
        .await
        .context("reading request size")?;
    let message_size = (&buf[..]).get_i32();
    let mut buf = vec![0; message_size as usize];
    stream
        .read_exact(&mut buf)
        .await
        .context("reading request")?;
    let request = parse_request_header(&mut buf).context("parsing request")?;
    let response = process_kafka_request(request, &mut buf).context("processing kafka request")?;

    dump(&response);
    stream.write(&response).await.context("writing response")?;

    Ok(())
}

fn parse_request_header(mut buf: &[u8]) -> anyhow::Result<KafkaRequestHeaderV2> {
    let request = KafkaRequestHeaderV2 {
        request: RequestApi::from(buf.try_get_i16().context("api key")?),
        version: buf.try_get_i16().context("api version")?,
        correlation_id: buf.try_get_i32().context("correlation ID")?,
        client: read_nullable_string(buf).context("client name")?,
    };
    if buf.try_get_u8()? != 0 {
        return Err(anyhow!("tagged fields are not supported"));
    }

    Ok(request)
}

fn dump(v: &[u8]) {
    let mut c = 0;
    for x in v {
        c += 1;
        eprint!("{:02x} ", x);
        if c % 8 == 0 {
            eprint!("\t");
        }
        if c % 16 == 0 {
            eprint!("\n")
        }
    }
    eprint!("\n");
}

fn process_kafka_request(req: KafkaRequestHeaderV2, mut buf: &[u8]) -> anyhow::Result<Bytes> {
    eprintln!("got request: {:?}", req);
    let mut header = BytesMut::with_capacity(4 + 4);

    let body = match req.request {
        RequestApi::ApiVersions => {
            kafka_ApiVersions(&req, &mut buf).context("handling ApiVersions")?
        }
        _ => {
            return Err(anyhow!("api {:?} is not supported", req.request));
        }
    };

    header.put_i32(4 + body.len() as i32); // message length
    header.put_i32(req.correlation_id);
    // header.put_u8(0); // tagged fields in response headers?

    let len = header.len() + body.len();
    Ok(header.chain(body).copy_to_bytes(len))
}

fn kafka_ApiVersions(
    req_header: &KafkaRequestHeaderV2,
    mut req_body: &[u8],
) -> anyhow::Result<BytesMut> {
    let mut resp = BytesMut::with_capacity(64);
    if req_header.version > 4 {
        resp.put_i16(KafkaError::UnsupportedVersion.into());
        resp.put_i32_varint(0); // empty array
        resp.put_i32(0); // throttle time
        resp.put_u8(0); // no tags
        return Ok(resp);
    }

    let client_name = read_compact_string(&mut req_body)?;
    let client_version = read_compact_string(&mut req_body)?;
    if req_body.try_get_u8()? != 0 {
        return Err(anyhow!("tagged fields are not supported"));
    }

    eprintln!(
        "ApiVersions v{} from client '{client_name}' version '{client_version}'",
        req_header.version
    );

    resp.put_i16(KafkaError::None.into()); // no error
    resp.put_i32_varint(2); // array of size 1
    resp.put_i16(RequestApi::ApiVersions.into());
    resp.put_i16(4); // min version
    resp.put_i16(4); // max version
    resp.put_u8(0); // no tags
    resp.put_i32(0); // throttle time
    resp.put_u8(0); // no tags

    Ok(resp)
}
