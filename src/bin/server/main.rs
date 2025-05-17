use futures_lite::StreamExt;
use glommio::{CpuSet, net::TcpListener, prelude::*};
use toob::node::server;

use std::io;

async fn server(srv: server::Server) -> Result<(), io::Error> {
    let listener = TcpListener::bind("localhost:8137")?;
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        match stream {
            Ok(mut stream) => {
                let srv = srv.clone();
                spawn_local(async move {
                    loop {
                        match srv.process_request(&mut stream).await {
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("error processing request: {e:#}");
                                break;
                            }
                        }
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

    let network_listeners = LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(1, Some(cpus)))
        .on_all_shards(|| async move {
            println!("starting executor {}", glommio::executor().id());
            let srv = server::Server::new("./log_dir").await.unwrap();
            server(srv).await.expect("failed to start server");
        })?;
    for r in network_listeners.join_all() {
        if let Err(e) = r {
            return Err(Box::new(e));
        }
    }

    Ok(())
}
