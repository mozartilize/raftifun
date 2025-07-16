mod coordinator;
mod events;
mod membership;
mod transport;

use clap::Parser;
use coordinator::{Command, CoordinatorState};
use events::Event;
use membership::MembershipChange;
use prost::Message;
use raft::prelude::{ConfChangeSingle, ConfChangeType, ConfChangeV2, MessageType};
use raft::{storage::MemStorage, Config, RawNode};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;
use tokio::sync::{mpsc, oneshot};
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
    listen: SocketAddr,

    #[arg(long)]
    join: Option<SocketAddr>,

    #[arg(long, default_value_t = false)]
    leave: bool,
}

use raft::eraftpb::Entry;

fn handle_committed_entries(
    node: &mut RawNode<MemStorage>,
    state: &mut CoordinatorState,
    entries: Vec<Entry>,
) {
    use raft::eraftpb::EntryType;
    // println!("process {} commited entries", entries.len());
    for entry in entries {
        if entry.data.is_empty() {
            continue;
        }
        match entry.get_entry_type() {
            EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                if let Ok(cc) = ConfChangeV2::decode(entry.get_data()) {
                    println!("Applying conf change: {:?}", cc);
                    if let Ok(cs) = node.apply_conf_change(&cc) {
                        node.mut_store().wl().set_conf_state(cs);
                    }
                }
            }
            _ => {
                if let Ok(cmd) = Command::decode(entry.get_data()) {
                    println!("Applying command: {:?}", cmd);
                    state.apply_command(cmd);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("Node {} listening on {}", args.node_id, args.listen);

    // If the user requested to leave the cluster, send a leave request and exit
    if args.leave {
        if let Some(target) = args.join {
            let mut client = RaftTransportClient::connect(format!("http://{}", target)).await?;
            let req = transport::raftio::LeaveRequest { id: args.node_id };
            let _ = client.leave(req).await?;
        } else {
            eprintln!("--leave requires --join to specify a peer");
        }
        // Do not start the service when leaving
        return Ok(());
    }

    let listen = args.listen;
    let id = args.node_id;

    let (tx, mut rx) = mpsc::channel::<Event>(16);
    let server_tx = tx.clone();

    // Bind the listener first so we know the port is reserved
    let listener = TcpListener::bind(listen).await.expect("bind failed");
    let addr = listener.local_addr().expect("listener addr");
    let (ready_tx, ready_rx) = oneshot::channel();

    // Spawn the gRPC server and signal once it starts serving
    spawn(async move {
        ready_tx.send(()).ok();
        Server::builder()
            .add_service(RaftTransportServer::new(RaftService::new(server_tx)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Wait for the server task to report readiness
    ready_rx.await.expect("server failed to start");

    // Ensure the server is actually accepting connections before continuing
    loop {
        match TcpStream::connect(addr).await {
            Ok(_) => break,
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }

    let config = Config {
        id,
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    };

    use slog::{Drain, Logger};

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog::LevelFilter::new(drain, slog::Level::Info).fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = Logger::root(drain, slog::o!());

    let storage: MemStorage;


    if let Some(join) = args.join {
        let mut client = RaftTransportClient::connect(format!("http://{}", join)).await?;
        let req = transport::raftio::JoinRequest {
            id,
            address: listen.to_string(),
        };
        let _ = client.join(req).await?;
        storage = MemStorage::new_with_conf_state((vec![], vec![]));
    } else {
        storage = MemStorage::new_with_conf_state((vec![id], vec![]));
    }

    let mut peer_addresses: HashMap<u64, String> = HashMap::new();

    let mut node = RawNode::new(&config, storage, &logger).unwrap();

    let mut state = CoordinatorState::default();
    state.all_stream_ids = (0..16).map(|_| Uuid::new_v4().to_string()).collect();

    let mut last_tick = Instant::now();

    loop {
        if last_tick.elapsed() >= Duration::from_millis(100) {
            node.tick();
            last_tick = Instant::now();

            // Log current voters and leader each tick
            let mut _voters: Vec<u64> = node.raft.prs().conf().voters().ids().iter().collect();
            _voters.sort_unstable();
            let _leader_id = node.raft.leader_id;
            println!(
                "[cluster state] voters: {:?}, leader: {} (me: {})",
                _voters, _leader_id, node.raft.id
            );
            println!(
                "[progress] applied: {}, committed: {}",
                node.raft.raft_log.applied, node.raft.raft_log.committed
            );
        }

        while let Ok(change) = rx.try_recv() {
            match change {
                Event::Membership(MembershipChange::AddNode { id: nid, address }) => {
                    peer_addresses.insert(nid, address.clone());
                    let cc = ConfChangeV2 {
                        changes: vec![ConfChangeSingle {
                            node_id: nid,
                            change_type: ConfChangeType::AddNode.into(),
                        }],
                        ..Default::default()
                    };
                    if let Err(e) = node.propose_conf_change(vec![], cc) {
                        eprintln!("Failed to propose add node: {e}");
                    }
                }
                Event::Membership(MembershipChange::RemoveNode(nid)) => {
                    peer_addresses.remove(&nid);
                    let cc = ConfChangeV2 {
                        changes: vec![ConfChangeSingle {
                            node_id: nid,
                            change_type: ConfChangeType::RemoveNode.into(),
                        }],
                        ..Default::default()
                    };
                    if let Err(e) = node.propose_conf_change(vec![], cc) {
                        eprintln!("Failed to propose remove node: {e}");
                    }
                }
                Event::Raft(msg, addr) => {
                    if msg.msg_type() != MessageType::MsgHeartbeat {
                        println!("receive a message {:?}", msg.msg_type());
                    }
                    if let Some(a) = addr {
                        peer_addresses.entry(msg.from).or_insert(a.to_string());
                    }
                    if let Err(e) = node.step(msg) {
                        eprintln!("Error stepping raft message: {e}");
                    }
                }
            }
        }

        let mut rd = node.ready();

        if !rd.snapshot().is_empty() {
            node.mut_store()
                .wl()
                .apply_snapshot(rd.snapshot().clone())?;
        }

        if !rd.entries().is_empty() {
            node.mut_store().wl().append(rd.entries())?;
        }

        if let Some(hs) = rd.hs() {
            node.mut_store().wl().set_hardstate(hs.clone());
        }

        for msg in rd.take_messages() {
            println!("hello?");
            if let Some(addr) = peer_addresses.get(&msg.to) {
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
                    println!("{:?}", result.err());
                } else {
                    eprintln!("cant connect to {}", addr);
                }
            }
        }

        handle_committed_entries(&mut node, &mut state, rd.take_committed_entries());

        let mut light_rd = node.advance(rd);

        for msg in light_rd.take_messages() {
            println!("hi?");
            if let Some(addr) = peer_addresses.get(&msg.to) {
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
                    println!("{:?}", result.err());
                }
            }
        }

        handle_committed_entries(&mut node, &mut state, light_rd.take_committed_entries());

        node.advance_apply();
    }
}
