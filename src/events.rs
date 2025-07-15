use crate::membership::MembershipChange;
use raft::eraftpb::Message as RaftMessage;

#[derive(Debug)]
pub enum Event {
    Raft(RaftMessage),
    Membership(MembershipChange),
}

