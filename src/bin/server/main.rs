use async_channel;
use futures_lite::StreamExt;
use glommio::{CpuSet, net::TcpListener, prelude::*};
use snafu::ResultExt;
use toob::{
    error::{self, Error},
    node,
};

use std::{collections::HashMap, io};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cpus = CpuSet::online().unwrap();
    let io_threads_num = 8; // TODO: configurable number of io_threads

    let net_cpus = cpus.filter(|l| l.cpu >= io_threads_num);
    assert!(net_cpus.len() > 0, "not enough CPUs for given IO threads");

    let parts = discover_partitions("./log_dir").unwrap();
    let parts_per_thread = parts.len().div_ceil(io_threads_num);
    let mut parts = parts.into_iter();

    // spawn IO workers. Each worker is getting up to 'parts_per_thread' partitions to serve
    let mut io_threads = Vec::new();
    let mut io_chans = HashMap::new();
    for cpu in 0..io_threads_num {
        let builder = LocalExecutorBuilder::new(Placement::Fixed(cpu));
        let (tx, rx) = async_channel::unbounded();
        let mut thread_parts = Vec::new();
        while let Some(p) = parts.next() {
            thread_parts.push(p);
            if thread_parts.len() >= parts_per_thread {
                break;
            }
        }

        if thread_parts.is_empty() {
            break;
        }

        thread_parts.iter().for_each(|(tp, _)| {
            io_chans.insert(tp.clone(), tx.clone());
        });

        let rxc = rx.clone();
        let t = builder
            .spawn(async move || {
                let mut server = node::io::Server::new(thread_parts, rxc)
                    .await
                    .expect("starting IO thread");
                server.serve().await.expect("IO thread running");
            })
            .expect("spawning IO thread");
        io_threads.push(t);
    }

    let network_listeners =
        LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(1, Some(net_cpus))).on_all_shards(
            || async {
                println!("starting executor {}", glommio::executor().id());
                let srv = node::network::Server::new(io_chans).await.unwrap();
                server(srv).await.expect("failed to start server");
            },
        )?;

    for r in network_listeners.join_all() {
        if let Err(e) = r {
            return Err(Box::new(e));
        }
    }
    for thread in io_threads {
        if let Err(e) = thread.join() {
            return Err(Box::new(e));
        }
    }

    Ok(())
}

fn discover_partitions(log_dir: &str) -> Result<Vec<(toob::TopicPartition, String)>, Error> {
    let mut files = Vec::new();
    for topic_entry in std::fs::read_dir(log_dir).context(error::IO {
        e: "reading log dir",
    })? {
        let topic_dir = topic_entry.context(error::IO {
            e: "reading topic entry",
        })?;
        if !topic_dir
            .file_type()
            .context(error::IO {
                e: "getting log entry type",
            })?
            .is_dir()
        {
            continue;
        }
        let topic = topic_dir.file_name().into_string().unwrap();
        let topic_path = std::path::Path::new(log_dir).join(topic_dir.file_name());

        for partition_entry in std::fs::read_dir(&topic_path).context(error::IO {
            e: "reading partition dir",
        })? {
            let partition = partition_entry.context(error::IO {
                e: "reading partition dir entry",
            })?;
            let fname = partition.file_name().into_string().unwrap();
            let ft = partition.file_type().context(error::IO {
                e: "getting partition dir entry type",
            })?;
            if !fname.ends_with(".log") || !ft.is_file() {
                continue;
            }
            let partition = fname
                .strip_suffix(".log")
                .unwrap()
                .parse()
                .expect("parsing partition name");

            let path = std::path::Path::new(&topic_path).join(fname);

            files.push((
                toob::TopicPartition {
                    topic: topic.clone(),
                    partition,
                },
                String::from(path.to_str().unwrap()),
            ));
            eprintln!("found topic {topic} partition {partition}");
        }
    }
    Ok(files)
}

async fn server(srv: node::network::Server) -> Result<(), io::Error> {
    let listener = TcpListener::bind("localhost:8137")?;
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        match stream {
            Ok(mut stream) => {
                let srv = srv.clone();
                spawn_local(async move {
                    loop {
                        match srv.accept(&mut stream).await {
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("error processing request: {e}");
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
