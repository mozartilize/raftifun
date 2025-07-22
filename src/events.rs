use raft::eraftpb::Message as RaftMessage;
use raft::prelude::{ConfChangeSingle, ConfChangeType, ConfChangeV2};
use raft::{storage::MemStorage, RawNode};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub enum MembershipChange {
    AddNode { id: u64, address: String },
    RemoveNode(u64),
}

#[derive(Debug)]
pub enum Event {
    /// A Raft protocol message received from a peer along with the sender's address
    Raft(RaftMessage, Option<SocketAddr>),
    Membership(MembershipChange),
}

pub async fn handle_event(
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
