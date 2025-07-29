use clap::Parser;
use prost::Message;
use raft::prelude::MessageType;
use raft::StateRole;
use raftifun::coordinator::CoordinatorState;
use raftifun::events::handle_event;
use raftifun::events::Event;
use raftifun::raft::{handle_committed_entries, new_node};
use raftifun::transport;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::interval;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{metadata::MetadataValue, transport::Server, Request};
use transport::{
    raftio::raft_transport_client::RaftTransportClient, RaftService, RaftTransportServer,
};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "cdc-raft", version, about = "CDC Raft Coordinator")]
struct Args {
    #[arg(long)]
    node_id: u64,

    #[arg(long)]
    listen: Option<SocketAddr>,

    #[arg(long)]
    join: Option<SocketAddr>,

    #[arg(long, default_value_t = false)]
    leave: bool,
}

enum Next {
    Tick,
    Event(Event),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use slog::{Drain, Logger};

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator)
        .use_file_location()
        .build()
        .fuse();
    let drain = slog::LevelFilter::new(drain, slog::Level::Debug).fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = Logger::root(drain, slog::o!());

    let args = Args::parse();

    match (args.node_id, args.listen, args.join, args.leave) {
        (id, Some(listen), join, false) => {
            println!("Node {} listening on {}", id, listen);

            let (tx, mut rx) = mpsc::channel::<Event>(16);
            let server_tx = tx.clone();

            let peer_addresses_shared: Arc<RwLock<HashMap<u64, String>>> =
                Arc::new(RwLock::new(HashMap::<u64, String>::new()));
            {
                let mut map = peer_addresses_shared.write().await;
                map.insert(id, listen.to_string());
            }

            // Bind the listener first so we know the port is reserved
            let listener = TcpListener::bind(listen).await.expect("bind failed");
            let (ready_tx, ready_rx) = oneshot::channel();

            let node = Arc::new(RwLock::new(
                new_node(id, listen, join, peer_addresses_shared.clone(), logger)
                    .await
                    .unwrap(),
            ));

            let service_node = node.clone();
            let service_peer_addresses = peer_addresses_shared.clone();
            // Spawn the gRPC server and signal once it starts serving
            spawn(async move {
                ready_tx.send(()).ok();
                Server::builder()
                    .add_service(RaftTransportServer::new(RaftService::new(
                        server_tx,
                        service_node,
                        service_peer_addresses,
                    )))
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            });

            // Wait for the server task to report readiness
            ready_rx.await.expect("server failed to start");

            let mut state = CoordinatorState::default();
            state.all_stream_ids = (0..16).map(|_| Uuid::new_v4().to_string()).collect();

            let mut ticker = interval(Duration::from_millis(100));

            loop {
                let next = tokio::select! {
                    _ = ticker.tick() => Next::Tick,
                    evt = rx.recv() => {
                        match evt { Some(e) => Next::Event(e), None => return Ok(()), }
                    }
                };

                match next {
                    Next::Tick => {
                        let mut raft_node = node.write().await;
                        raft_node.tick();
                    }
                    Next::Event(e) => {
                        handle_event(e, &node, &peer_addresses_shared).await;
                        while let Ok(ev) = rx.try_recv() {
                            handle_event(ev, &node, &peer_addresses_shared).await;
                        }
                    }
                }

                let mut messages = Vec::new();
                let mut persisted_messages = Vec::new();
                let mut light_messages = Vec::new();

                {
                    let mut raft_node = node.write().await;

                    if raft_node.raft.state != StateRole::Leader && raft_node.raft.msgs.len() > 0 {
                        println!("hello from node {id}, {:?}", raft_node.raft.msgs);
                    }

                    if !raft_node.has_ready() {
                        continue;
                    }

                    let mut rd = raft_node.ready();

                    if !rd.snapshot().is_empty() {
                        raft_node
                            .mut_store()
                            .wl()
                            .apply_snapshot(rd.snapshot().clone())?;
                    }

                    if !rd.entries().is_empty() {
                        raft_node.mut_store().wl().append(rd.entries())?;
                    }

                    if let Some(hs) = rd.hs() {
                        raft_node.mut_store().wl().set_hardstate(hs.clone());
                    }

                    messages.extend(rd.take_messages());
                    persisted_messages.extend(rd.take_persisted_messages());

                    handle_committed_entries(&mut raft_node, &mut state, rd.take_committed_entries());

                    let mut light_rd = raft_node.advance(rd);

                    light_messages.extend(light_rd.take_messages());

                    handle_committed_entries(
                        &mut raft_node,
                        &mut state,
                        light_rd.take_committed_entries(),
                    );

                    raft_node.advance_apply();
                }

                for msg in messages {
                    if let Some(addr) = {
                        let map = peer_addresses_shared.read().await;
                        map.get(&msg.to).cloned()
                    } {
                        if msg.msg_type() != MessageType::MsgHeartbeat {
                            println!("sending message to peer {}, {:?}", addr, msg.msg_type());
                        }
                        if let Ok(mut client) =
                            RaftTransportClient::connect(format!("http://{}", addr)).await
                        {
                            let mut data = Vec::new();
                            msg.encode(&mut data).unwrap();
                            let raft_msg = transport::raftio::RaftMessage { data };
                            let mut req = Request::new(raft_msg);
                            req.metadata_mut().insert(
                                "x-raft-from",
                                MetadataValue::try_from(listen.to_string()).unwrap(),
                            );
                            let result = client.send_message(req).await;
                            if msg.msg_type() != MessageType::MsgHeartbeat {
                                println!("hello? {:?}", result.err());
                            }
                        } else {
                            eprintln!("cant connect to {}", addr);
                        }
                    }
                }

                for msg in persisted_messages {
                    if let Some(addr) = {
                        let map = peer_addresses_shared.read().await;
                        map.get(&msg.to).cloned()
                    } {
                        if msg.msg_type() != MessageType::MsgHeartbeat {
                            println!("sending message to peer {}, {:?}", addr, msg.msg_type());
                        }
                        if let Ok(mut client) =
                            RaftTransportClient::connect(format!("http://{}", addr)).await
                        {
                            let mut data = Vec::new();
                            msg.encode(&mut data).unwrap();
                            let raft_msg = transport::raftio::RaftMessage { data };
                            let mut req = Request::new(raft_msg);
                            req.metadata_mut().insert(
                                "x-raft-from",
                                MetadataValue::try_from(listen.to_string()).unwrap(),
                            );
                            let result = client.send_message(req).await;
                            if msg.msg_type() != MessageType::MsgHeartbeat {
                                println!("hello? {:?}", result.err());
                            }
                        } else {
                            eprintln!("cant connect to {}", addr);
                        }
                    }
                }

                for msg in light_messages {
                    if let Some(addr) = {
                        let map = peer_addresses_shared.read().await;
                        map.get(&msg.to).cloned()
                    } {
                        if msg.msg_type() != MessageType::MsgHeartbeat {
                            println!(
                                "sending light message to peer {}, {:?}",
                                addr,
                                msg.msg_type()
                            );
                        }
                        if let Ok(mut client) =
                            RaftTransportClient::connect(format!("http://{}", addr)).await
                        {
                            let mut data = Vec::new();
                            msg.encode(&mut data).unwrap();
                            let raft_msg = transport::raftio::RaftMessage { data };
                            let mut req = Request::new(raft_msg);
                            req.metadata_mut().insert(
                                "x-raft-from",
                                MetadataValue::try_from(listen.to_string()).unwrap(),
                            );
                            let result = client.send_message(req).await;
                            if msg.msg_type() != MessageType::MsgHeartbeat {
                                println!("hi {:?}", result.err());
                            }
                        }
                    }
                }
            };
        },
        (id, None, Some(join), true) => {
            let mut client = RaftTransportClient::connect(format!("http://{}", join)).await?;
            let req = transport::raftio::LeaveRequest { id };
            let _ = client.leave(req).await?;
        }
        _ => (),
    };
    Ok(())
}
