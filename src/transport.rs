use prost::Message;
use raft::eraftpb::Message as RaftProtoMessage;
use raft::{storage::MemStorage, RawNode, Storage};
use std::net::SocketAddr;
use tonic::{Request, Response, Status};

pub mod raftio {
    tonic::include_proto!("raftio");
}

use raftio::raft_transport_client::RaftTransportClient;
use raftio::raft_transport_server::RaftTransport;
pub use raftio::raft_transport_server::RaftTransportServer;
use raftio::{JoinRequest, JoinResponse, LeaveRequest, LeaveResponse, RaftMessage};

use crate::events::{Event, MembershipChange};
use raft::StateRole;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc::Sender, RwLock};

pub struct RaftService {
    pub tx: Sender<Event>,
    pub node: Arc<RwLock<RawNode<MemStorage>>>,
    pub peer_addresses: Arc<RwLock<HashMap<u64, String>>>,
}

impl RaftService {
    pub fn new(
        tx: Sender<Event>,
        node: Arc<RwLock<RawNode<MemStorage>>>,
        peer_addresses: Arc<RwLock<HashMap<u64, String>>>,
    ) -> Self {
        Self {
            tx,
            node,
            peer_addresses,
        }
    }
}

#[tonic::async_trait]
impl RaftTransport for RaftService {
    async fn send_message(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        let remote = request
            .metadata()
            .get("x-raft-from")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<SocketAddr>().ok());
        let msg = request.into_inner();

        match RaftProtoMessage::decode(&*msg.data) {
            Ok(parsed) => {
                if let Err(e) = self.tx.send(Event::Raft(parsed, remote)).await {
                    eprintln!("Failed to forward raft message: {e}");
                }
            }
            Err(e) => println!("Failed to decode raft message: {e}"),
        }

        Ok(Response::new(RaftMessage { data: vec![] }))
    }

    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        let jr = request.into_inner();
        println!("Join request for node {}", jr.id);

        {
            let node = self.node.read().await;
            if node.raft.state != StateRole::Leader {
                let leader_id = node.raft.leader_id;
                drop(node);
                if leader_id != 0 {
                    if let Some(addr) = self.peer_addresses.read().await.get(&leader_id).cloned() {
                        println!(
                            "Forwarding join request to leader {} at {}",
                            leader_id, addr
                        );
                        let mut client = RaftTransportClient::connect(format!("http://{}", addr))
                            .await
                            .map_err(|e| {
                                Status::failed_precondition(format!("forward join: {e}"))
                            })?;
                        return client.join(Request::new(jr)).await;
                    }
                }
                return Err(Status::failed_precondition(
                    "node is not leader and leader unknown",
                ));
            }
        }

        if let Err(e) = self
            .tx
            .send(Event::Membership(MembershipChange::AddNode {
                id: jr.id,
                address: jr.address.clone(),
            }))
            .await
        {
            return Err(Status::internal(format!("forward join request: {e}")));
        }

        let mut node = self.node.write().await;
        let voters = node.raft.prs().conf().voters().ids().iter().collect();
        let learners = node.raft.prs().conf().learners().iter().cloned().collect();
        let applied = node.raft.raft_log.applied;
        let node_id = node.raft.id;
        let mut snapshot = Vec::new();
        if let Ok(mut snap) = node.mut_store().snapshot(applied, node_id) {
            if let Ok(term) = node.mut_store().term(applied) {
                snap.mut_metadata().term = term;
                snapshot = snap.encode_to_vec();
            }
        }
        drop(node);

        let peer_addresses = self.peer_addresses.read().await.clone();

        Ok(Response::new(JoinResponse {
            voters,
            learners,
            peer_addresses,
            snapshot,
        }))
    }

    async fn leave(
        &self,
        request: Request<LeaveRequest>,
    ) -> Result<Response<LeaveResponse>, Status> {
        let lr = request.into_inner();
        println!("Leave request for node {}", lr.id);

        {
            let node = self.node.read().await;
            if node.raft.state != StateRole::Leader {
                let leader_id = node.raft.leader_id;
                drop(node);
                if leader_id != 0 {
                    if let Some(addr) = self.peer_addresses.read().await.get(&leader_id).cloned() {
                        println!(
                            "Forwarding leave request to leader {} at {}",
                            leader_id, addr
                        );
                        let mut client = RaftTransportClient::connect(format!("http://{}", addr))
                            .await
                            .map_err(|e| {
                                Status::failed_precondition(format!("forward leave: {e}"))
                            })?;
                        return client.leave(Request::new(lr)).await;
                    }
                }
                return Err(Status::failed_precondition(
                    "node is not leader and leader unknown",
                ));
            }
        }
        
        if let Err(e) = self
            .tx
            .send(Event::Membership(MembershipChange::RemoveNode(lr.id)))
            .await
        {
            eprintln!("Failed to forward leave request: {e}");
        }
        Ok(Response::new(LeaveResponse {}))
    }
}
