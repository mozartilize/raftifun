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
use raft::StateRole;
use raft::{storage::MemStorage, Config, RawNode};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{metadata::MetadataValue, transport::Server, Request};
use transport::{
    raftio::raft_transport_client::RaftTransportClient, RaftService,
    RaftTransportServer,
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
                for (node_id, prs) in node.raft.prs().iter() {
                    println!("post apply {node_id} {:?}", prs);
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

enum Next {
    Tick,
    Event(Event),
}

async fn handle_event(
    evt: Event,
    node: &Arc<RwLock<RawNode<MemStorage>>>,
    peer_addresses: &Arc<RwLock<HashMap<u64, String>>>,
) {
    match evt {
        Event::Membership(MembershipChange::AddNode { id, address }) => {
            {
                let mut map = peer_addresses.write().await;
                map.insert(id, address);
            }
            let mut n = node.write().await;
            let cc = ConfChangeV2 {
                changes: vec![ConfChangeSingle {
                    node_id: id,
                    change_type: ConfChangeType::AddLearnerNode.into(),
                }],
                ..Default::default()
            };
            if let Err(e) = n.propose_conf_change(vec![], cc) {
                eprintln!("Failed to propose add learner node: {e}");
            }
        }
        Event::Membership(MembershipChange::RemoveNode(id)) => {
            {
                let mut map = peer_addresses.write().await;
                map.remove(&id);
            }
            let mut n = node.write().await;
            let cc = ConfChangeV2 {
                changes: vec![ConfChangeSingle {
                    node_id: id,
                    change_type: ConfChangeType::RemoveNode.into(),
                }],
                ..Default::default()
            };
            if let Err(e) = n.propose_conf_change(vec![], cc) {
                eprintln!("Failed to propose remove node: {e}");
            }
        }
        Event::Raft(msg, addr) => {
            if let Some(a) = addr {
                let mut map = peer_addresses.write().await;
                map.entry(msg.from).or_insert(a.to_string());
            }
            let mut n = node.write().await;
            if let Err(e) = n.step(msg) {
                eprintln!("Error stepping raft message: {e}");
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

    let peer_addresses_shared = Arc::new(RwLock::new(HashMap::<u64, String>::new()));
    {
        let mut map = peer_addresses_shared.write().await;
        map.insert(id, listen.to_string());
    }

    // Bind the listener first so we know the port is reserved
    let listener = TcpListener::bind(listen).await.expect("bind failed");
    let (ready_tx, ready_rx) = oneshot::channel();


    let config = Config {
        id,
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    };

    use slog::{Drain, Logger};

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator)
        .use_file_location()
        .build()
        .fuse();
    let drain = slog::LevelFilter::new(drain, slog::Level::Debug).fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = Logger::root(drain, slog::o!());

    let storage: MemStorage;

    if let Some(join) = args.join {
        let mut client = RaftTransportClient::connect(format!("http://{}", join)).await?;
        let req = transport::raftio::JoinRequest {
            id,
            address: listen.to_string(),
        };
        if let Ok(resp) = client.join(req).await {
            let resp = resp.into_inner();
            for (id, addr) in resp.peer_addresses {
                let mut map = peer_addresses_shared.write().await;
                map.insert(id, addr);
            }
            storage = MemStorage::new_with_conf_state((resp.voters, resp.learners));
            if !resp.snapshot.is_empty() {
                if let Ok(snapshot) = raft::eraftpb::Snapshot::decode(&*resp.snapshot) {
                    println!("apply_snapshot {:?}", snapshot);
                    storage.wl().apply_snapshot(snapshot)?;
                }
            }
        } else {
            storage = MemStorage::new_with_conf_state((vec![], vec![]));
        }
    } else {
        storage = MemStorage::new_with_conf_state((vec![id], vec![]));
    }

    let node = Arc::new(RwLock::new(RawNode::new(&config, storage, &logger).unwrap()));

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
                raft_node.mut_store()
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

            handle_committed_entries(&mut raft_node, &mut state, light_rd.take_committed_entries());

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
                    println!("sending light message to peer {}, {:?}", addr, msg.msg_type());
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
    }
}
