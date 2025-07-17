Distributed consumers (using raft protocol) for ScyllaDB CDC.

The project uses Rust programing language, ultilizes raft-rs, tonic to build the cluster of consumers.

For now I only focus on building the raft cluster.

```
# start the first leader node
cargo run -- --node-id 1 --listen 127.0.0.1:19099

# start followers
cargo run -- --node-id 2 --listen 127.0.0.1:19100 --join 127.0.0.1:19099
cargo run -- --node-id 3 --listen 127.0.0.1:19101 --join 127.0.0.1:19099
```
