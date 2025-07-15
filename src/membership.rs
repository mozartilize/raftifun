#[derive(Debug)]
pub enum MembershipChange {
    AddNode(u64),
    RemoveNode(u64),
}