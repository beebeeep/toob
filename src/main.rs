mod binary;
mod kafka;

use futures_lite::StreamExt;
use glommio::{CpuSet, net::TcpListener, prelude::*};

use std::io;

async fn server() -> Result<(), io::Error> {
    let listener = TcpListener::bind("127.0.0.1:8137")?;
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        match stream {
            Ok(mut stream) => {
                spawn_local(async move {
                    match kafka::protocol::process_request(&mut stream).await {
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cpus = CpuSet::online().unwrap();

    let network_listeners = LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(4, Some(cpus)))
        .on_all_shards(|| async move {
            println!("starting executor {}", glommio::executor().id());
            server().await.expect("failed to start server");
        })?;
    for r in network_listeners.join_all() {
        if let Err(e) = r {
            return Err(Box::new(e));
        }
    }

    Ok(())
}
