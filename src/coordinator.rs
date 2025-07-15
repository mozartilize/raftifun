use prost::Message;
use std::collections::{HashMap, HashSet};

#[derive(Clone, PartialEq, Message)]
pub struct Command {
    #[prost(oneof = "command::Cmd", tags = "1, 2, 3")]
    pub cmd: Option<command::Cmd>,
}

pub mod command {
    use prost::alloc::vec::Vec;
    use prost::alloc::string::String;
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Cmd {
        #[prost(string, tag = "1")]
        ConsumerJoin(String),
        #[prost(string, tag = "2")]
        ConsumerLeave(String),
        #[prost(message, tag = "3")]
        AssignStreams(AssignStreams),
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct AssignStreams {
        #[prost(string, tag = "1")]
        pub consumer_id: String,
        #[prost(string, repeated, tag = "2")]
        pub stream_ids: Vec<String>,
    }
}

#[derive(Debug, Default)]
pub struct CoordinatorState {
    pub consumers: HashSet<String>,
    pub assignments: HashMap<String, Vec<String>>,
    pub all_stream_ids: Vec<String>,
}

impl CoordinatorState {
    pub fn rebalance(&mut self) {
        let num_consumers = self.consumers.len();
        if num_consumers == 0 {
            return;
        }

        let mut assignments: HashMap<String, Vec<String>> = HashMap::new();
        let consumers: Vec<&String> = self.consumers.iter().collect();

        for (i, stream_id) in self.all_stream_ids.iter().enumerate() {
            let target = consumers[i % num_consumers];
            assignments.entry(target.clone()).or_default().push(stream_id.clone());
        }

        self.assignments = assignments;
    }

    pub fn apply_command(&mut self, cmd: Command) {
        use command::Cmd::*;
        match cmd.cmd {
            Some(ConsumerJoin(cid)) => {
                self.consumers.insert(cid);
                self.rebalance();
            }
            Some(ConsumerLeave(cid)) => {
                self.consumers.remove(&cid);
                self.rebalance();
            }
            Some(AssignStreams(_as)) => {
                // Usually handled internally
            }
            None => {}
        }
    }
}
