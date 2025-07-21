use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use crate::{
    coordinator::{Command, CoordinatorState},
    transport::{self, raftio::raft_transport_client::RaftTransportClient},
};
use prost::Message as _;
use raft::eraftpb::Entry;
use raft::{prelude::ConfChangeV2, storage::MemStorage, Config, RawNode};
use slog::Logger;
use tokio::sync::RwLock;

pub async fn new_node(
    id: u64,
    listen: SocketAddr,
    join_addr: Option<SocketAddr>,
    peer_addresses_shared: Arc<RwLock<HashMap<u64, String>>>,
    logger: Logger,
) -> anyhow::Result<RawNode<MemStorage>> {
    let config = Config {
        id,
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    };
    let storage: MemStorage;

    if let Some(join) = join_addr {
        let mut client = RaftTransportClient::connect(format!("http://{}", join)).await?;
        let req = transport::raftio::JoinRequest {
            id,
            address: listen.to_string(),
        };
        if let Ok(resp) = client.join(req).await {
            let resp = resp.into_inner();
            let mut peer_addresses_shared_wl = peer_addresses_shared.write().await;
            for (id, addr) in resp.peer_addresses {
                peer_addresses_shared_wl.insert(id, addr);
            }
            peer_addresses_shared_wl.insert(id, listen.to_string());
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

    let node = RawNode::new(&config, storage, &logger).unwrap();
    Ok(node)
}

pub fn handle_committed_entries(
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
