mod coordinator;
mod transport;

use prost::Message;
use clap::Parser;
use coordinator::{CoordinatorState, Command};
use raft::StateRole;
use raft::{Config, RawNode, storage::MemStorage};
use std::time::{Duration, Instant};
use std::net::SocketAddr;
use tokio::spawn;
use tonic::transport::Server;
use transport::{RaftService, RaftTransportServer};
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("Node {} listening on {}", args.node_id, args.listen);

    let listen = args.listen;
    let id = args.node_id;

    // Start gRPC server for Raft transport
    spawn(async move {
        Server::builder()
            .add_service(RaftTransportServer::new(RaftService::default()))
            .serve(listen)
            .await
            .unwrap();
    });

    let config = Config {
        id,
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    };

    use slog::{Drain, Logger};

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = Logger::root(drain, slog::o!());

    let storage = MemStorage::new_with_conf_state((vec![id], vec![]));
    let mut node = RawNode::new(&config, storage, &logger).unwrap();

    let mut state = CoordinatorState::default();
    state.all_stream_ids = (0..16).map(|_| Uuid::new_v4().to_string()).collect();

    let mut last_tick = Instant::now();
    let mut last_role = node.raft.state;

    loop {
        if last_tick.elapsed() >= Duration::from_millis(100) {
            node.tick();
            last_tick = Instant::now();
        }

        // Check for leadership change
        let current_role = node.raft.state;
        if current_role == StateRole::Leader && last_role != StateRole::Leader {
            println!("Node {} became leader; proposing no-op entry", id);
            // Propose a no-op entry (empty context, data can be empty or some marker)
            if let Err(e) = node.propose(vec![], b"no-op".to_vec()) {
                eprintln!("Failed to propose no-op: {e:?}");
            }
        }
        last_role = current_role;

        let rd = node.ready();

        for entry in rd.entries() {
            if entry.get_data().is_empty() {
                continue;
            }

            if let Ok(cmd) = Command::decode(entry.get_data()) {
                println!("Applying command: {:?}", cmd);
                state.apply_command(cmd);
            }
        }

        node.advance(rd);
    }
}
