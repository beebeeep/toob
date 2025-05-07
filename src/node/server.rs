use std::rc::Rc;

use anyhow::{Context, Result};
use futures_lite::AsyncReadExt;
use glommio::net::TcpStream;
use prost::Message;

use crate::pb;
#[derive(Clone)]
pub struct Server {
    log_dir: Rc<String>,
}

impl Server {
    pub fn new(log_dir: &str) -> Self {
        Self {
            log_dir: Rc::new(log_dir.to_string()),
        }
    }

    pub async fn process_request(&self, stream: &mut TcpStream) -> Result<()> {
        let mut buf = [0u8; 64];
        stream.read(&mut buf).await.context("reading header")?;
        let header = pb::Header::decode_length_delimited(&buf[..]).context("decoding header")?;
        eprintln!("got header {header:?}");
        Ok(())
    }
}
