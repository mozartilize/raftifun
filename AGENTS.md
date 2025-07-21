# Raftifun - Contributor Guide
---

## Intro

Distributed consumers (using raft protocol) for ScyllaDB CDC.

The project uses Rust programing language, ultilizes raft-rs, tonic to build
the cluster of consumers.

For now I only focus on building the raft cluster.

The application provides a command line interface (cli) as example below:

```
# start the first leader node
cargo run -- --node-id 1 --listen 127.0.0.1:19099

# start followers
cargo run -- --node-id 2 --listen 127.0.0.1:19100 --join 127.0.0.1:19099
cargo run -- --node-id 3 --listen 127.0.0.1:19101 --join 127.0.0.1:19099
```

## Structure in `src/`

### raft.rs

For handling everything relates to Raft

`new_node` funtion creates a new raft node.
If no `join` argument, it'll self elect to be a leader.
Otherwise, it'll ask the peer to join and get the cluster's snapshot in join
response to bootstrap.

### transport.rs

For handling comunication between nodes.
The service's defined in `struct RaftService` implements protobuf from
`proto/` directory.

### events.rs

`Event` enum wraps raft-rs messages `raft::eraftpb::Message` and the
application custom events.

Events generated when nodes comunicate or per user request (via command line)
through `RaftService` in transport layer.

### coordinator.rs

For handling business logic where ScyllaDB CDC has a set of stream ids per
generation.

Each node will handle a subset of stream ids and automatically relabanced when
a node join/leave the cluster.
