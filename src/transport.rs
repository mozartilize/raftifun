use tonic::{Request, Response, Status};
use prost::Message;
use raft::eraftpb::Message as RaftProtoMessage;

pub mod raftio {
    tonic::include_proto!("raftio");
}

use raftio::raft_transport_server::{RaftTransport};
pub use raftio::raft_transport_server::RaftTransportServer;
use raftio::RaftMessage;

#[derive(Debug, Default)]
pub struct RaftService;

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
}
