use tonic::{Request, Response, Status};
use prost::Message;
use raft::{eraftpb::Message as RaftProtoMessage};

pub mod raftio {
    tonic::include_proto!("raftio");
}

use raftio::raft_transport_server::RaftTransport;
pub use raftio::raft_transport_server::RaftTransportServer;
use raftio::{RaftMessage, JoinRequest, JoinResponse, LeaveRequest, LeaveResponse};

use tokio::sync::mpsc::Sender;
use crate::membership::MembershipChange;

pub struct RaftService {
    pub tx: Sender<MembershipChange>,
    // pub node: Arc<Mutex<RawNode<MemStorage>>>,
}

impl RaftService {
    pub fn new(tx: Sender<MembershipChange>) -> Self {
        Self { tx }
    }
}

#[tonic::async_trait]
impl RaftTransport for RaftService {
    async fn send_message(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        let msg = request.into_inner();
        println!("Received RaftMessage: {} bytes", msg.data.len());

        // This will work if ProstMessage trait is in scope and raft 0.7+ is used!
        match RaftProtoMessage::decode(&*msg.data) {
            Ok(parsed) => println!("Parsed Raft message: {:?}", parsed.get_msg_type()),
            Err(e) => println!("Failed to decode raft message: {e}"),
        }

        Ok(Response::new(RaftMessage { data: vec![] }))
    }

    async fn join(
        &self,
        request: Request<JoinRequest>,
    ) -> Result<Response<JoinResponse>, Status> {
        let jr = request.into_inner();
        println!("Join request for node {}", jr.id);
        if let Err(e) = self.tx.send(MembershipChange::AddNode(jr.id)).await {
            eprintln!("Failed to forward join request: {e}");
        }
        Ok(Response::new(JoinResponse {}))
    }

    async fn leave(
        &self,
        request: Request<LeaveRequest>,
    ) -> Result<Response<LeaveResponse>, Status> {
        let lr = request.into_inner();
        println!("Leave request for node {}", lr.id);
        if let Err(e) = self.tx.send(MembershipChange::RemoveNode(lr.id)).await {
            eprintln!("Failed to forward leave request: {e}");
        }
        Ok(Response::new(LeaveResponse {}))
    }
}
