use crate::membership::MembershipChange;
use raft::eraftpb::Message as RaftMessage;
use std::net::SocketAddr;

#[derive(Debug)]
pub enum Event {
    /// A Raft protocol message received from a peer along with the sender's address
    Raft(RaftMessage, Option<SocketAddr>),
    Membership(MembershipChange),
}
