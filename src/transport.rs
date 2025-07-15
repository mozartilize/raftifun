use prost::Message;
use raft::eraftpb::Message as RaftProtoMessage;
use tonic::{Request, Response, Status};

pub mod raftio {
    tonic::include_proto!("raftio");
}

use raftio::raft_transport_server::RaftTransport;
pub use raftio::raft_transport_server::RaftTransportServer;
use raftio::{JoinRequest, JoinResponse, LeaveRequest, LeaveResponse, RaftMessage};

use crate::events::Event;
use crate::membership::MembershipChange;
use tokio::sync::mpsc::Sender;

pub struct RaftService {
    pub tx: Sender<Event>,
    // pub node: Arc<Mutex<RawNode<MemStorage>>>,
}

impl RaftService {
    pub fn new(tx: Sender<Event>) -> Self {
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

        match RaftProtoMessage::decode(&*msg.data) {
            Ok(parsed) => {
                if let Err(e) = self.tx.send(Event::Raft(parsed)).await {
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
        Ok(Response::new(JoinResponse {}))
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
