use prost::Message;
use raft::eraftpb::Message as RaftProtoMessage;
use tonic::{Request, Response, Status};
use std::net::SocketAddr;

pub mod raftio {
    tonic::include_proto!("raftio");
}

use raftio::raft_transport_server::RaftTransport;
pub use raftio::raft_transport_server::RaftTransportServer;
use raftio::{JoinRequest, JoinResponse, LeaveRequest, LeaveResponse, RaftMessage};
use raft::storage::MemStorage;
use raft::Storage;

use crate::events::Event;
use crate::membership::MembershipChange;
use tokio::sync::mpsc::Sender;

pub struct RaftService {
    pub tx: Sender<Event>,
    pub storage: MemStorage,
    // pub node: Arc<Mutex<RawNode<MemStorage>>>,
}

impl RaftService {
    pub fn new(tx: Sender<Event>, storage: MemStorage) -> Self {
        Self { tx, storage }
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
        if let Err(e) = self
            .tx
            .send(Event::Membership(MembershipChange::AddNode {
                id: jr.id,
                address: jr.address,
            }))
            .await
        {
            eprintln!("Failed to forward join request: {e}");
        }
        let snap = self
            .storage
            .snapshot(0, jr.id)
            .map_err(|e| Status::internal(format!("failed to get snapshot: {e}")))?;
        let mut data = Vec::new();
        snap.get_metadata().get_conf_state().encode(&mut data).unwrap();
        Ok(Response::new(JoinResponse { conf_state: data }))
    }

    async fn leave(
        &self,
        request: Request<LeaveRequest>,
    ) -> Result<Response<LeaveResponse>, Status> {
        let lr = request.into_inner();
        println!("Leave request for node {}", lr.id);
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
