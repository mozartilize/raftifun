#[derive(Debug)]
pub enum MembershipChange {
    AddNode { id: u64, address: String },
    RemoveNode(u64),
}
