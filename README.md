# Distributed Redis in Go

A simple Redis-like server implemented in Go from scratch with distributed capabilities. This project demonstrates both single-node and distributed key-value storage, including the RESP (REdis Serialization Protocol) and concurrent client handling.

## Features

### Single-Node Mode (Backward Compatible)
*   In-memory key-value store.
*   Handles multiple client connections concurrently.
*   Implements a subset of Redis commands:
    *   `PING`
    *   `SET`
    *   `GET`
    *   `DEL`

### Distributed Mode
*   **Sharding**: Static FNV hash modulo key distribution across multiple nodes
*   **Replication**: Master-replica setup with automatic synchronization
*   **Persistence**: [BoltDB](https://github.com/etcd-io/bbolt)-based storage for data durability
*   **Cluster Communication**: HTTP-based inter-node communication
*   **Transparent Routing**: Clients connect to any node, requests automatically routed
*   **Configuration**: TOML-based cluster configuration

## Getting Started

### Prerequisites

*   Go 1.24 or later.
*   `redis-cli` (for testing the server).

### Installation & Running the Server

1.  **Clone the repository:**
    ```sh
    git clone https://github.com/your-username/redis-go.git
    cd redis-go
    ```

2.  **Build the executable:**
    ```sh
    go build -o redis-go ./cmd/main.go
    ```

### Single-Node Mode (Default)

3.  **Run the server:**
    ```sh
    ./redis-go
    ```
    The server will start and listen on port `6380`.

4.  **Connect with redis-cli:**
    ```sh
    redis-cli -p 6380
    ```

### Distributed Mode

3.  **Start the distributed cluster:**
    ```sh
    ./launch.sh
    ```
    This starts 4 shards with replicas across different ports.

4.  **Connect to any shard:**
    ```sh
    redis-cli -p 6380  # Hanoi shard
    redis-cli -p 6381  # Sai Gon shard
    redis-cli -p 6382  # Da Nang shard
    redis-cli -p 6383  # Thanh Hoa shard
    ```

    **Or connect to replicas (read-only):**
    ```sh
    redis-cli -p 6390  # Hanoi replica
    redis-cli -p 6391  # Sai Gon replica
    redis-cli -p 6392  # Da Nang replica
    redis-cli -p 6393  # Thanh Hoa replica
    ```

### Supported Commands

Once connected to either mode, you can use the following commands:

```
127.0.0.1:6380> PING
PONG
127.0.0.1:6380> SET mykey "Hello, Redis-Go!"
OK
127.0.0.1:6380> GET mykey
"Hello, Redis-Go!"
127.0.0.1:6380> DEL mykey
OK
127.0.0.1:6380> GET mykey
(nil)
```

In distributed mode, keys are automatically routed to the correct shard based on static FNV hash modulo sharding, so you can connect to any shard and the system will handle routing transparently.

## Current Contract and Guarantees

This project targets a Redis-compatible string key-value subset with durable local storage and statically sharded cluster routing.

Current guarantees:

*   Supported commands are `PING`, `SET`, `GET`, and `DEL`.
*   Shard ownership is deterministic: `fnv64(key) % shardCount`.
*   Writes are accepted by the shard master that owns the key.
*   Master writes are persisted to local BoltDB before success is returned.
*   Replication is asynchronous, so replicas may lag behind masters.
*   Replica writes are rejected through the Redis command path.
*   Optional Redis `AUTH` is available with `-auth-token`.
*   Optional bearer authentication for internal HTTP APIs is available with `-internal-auth-token`.
*   Optional TLS for Redis and HTTP listeners is available with `-tls-cert-file` and `-tls-key-file`.
*   The system does not yet provide automatic failover, leader election, strong consistency, or online resharding.

Durability and replication notes:

*   BoltDB commits are completed before local write success is returned.
*   Replication uses a persistent ordered log stored in BoltDB.
*   Replicas acknowledge log entries by sequence ID after applying them locally.
*   A master crash after local commit but before replica acknowledgement can leave replicas behind until the master recovers.
*   A master disk loss can still lose acknowledged writes that were not replicated elsewhere.

Operational endpoints:

*   `GET /healthz`: process liveness.
*   `GET /readyz`: DB readiness, shard metadata, and replication queue depth.
*   `GET /debug/vars`: expvar metrics, including command counts, HTTP handler counts, routing failures, and replication counters.

## Project Structure

```
.
├── cmd/
│   └── main.go         # Main application entry point with distributed support
├── internal/
│   ├── config/
│   │   └── config.go   # TOML configuration parsing and sharding logic
│   ├── db/
│   │   └── db.go       # BoltDB persistence layer
│   ├── replication/
│   │   └── replication.go # Master-replica synchronization
│   ├── resp/
│   │   └── parser.go   # RESP protocol parser
│   ├── server/
│   │   └── server.go   # Server logic with sharding and routing
│   ├── store/
│   │   └── kv.go       # In-memory key-value store with persistence
│   └── web/
│       └── web.go      # HTTP handlers for cluster communication
├── persistence/        # Database files directory (ignored by git)
├── sharding.toml       # Cluster configuration
├── launch.sh          # Multi-node deployment script
├── .gitignore         # Git ignore file
├── go.mod
└── README.md
```

### Key Components

*   **`cmd/main.go`**: Enhanced entry point supporting both single-node and distributed modes
*   **`internal/config`**: Handles TOML configuration parsing and static FNV modulo sharding
*   **`internal/db`**: BoltDB-based persistence layer for data durability
*   **`internal/replication`**: Implements master-replica synchronization for high availability
*   **`internal/server`**: Enhanced server with automatic request routing between shards
*   **`internal/store`**: Thread-safe key-value store with optional persistence backend
*   **`internal/web`**: HTTP handlers for inter-node communication and cluster management
*   **`persistence/`**: Directory containing all BoltDB database files (ignored by git)
*   **`sharding.toml`**: Configuration file defining cluster topology and shard assignments
*   **`launch.sh`**: Convenient script to start the entire distributed cluster
*   **`.gitignore`**: Git ignore file excluding database files and build artifacts

## Production Readiness Backlog

This project is currently a learning/demo distributed key-value store. The backlog below tracks the major work needed before it could be considered production-ready.

### P0: Correctness and Safety

- [x] Define the production contract: Redis-compatible cache, durable KV, or strongly consistent KV.
- [x] Replace the RESP parser with a robust incremental parser that supports partial reads, pipelining, malformed inputs, and max payload limits.
- [x] Add initial RESP read/write deadlines, HTTP server timeouts, and internal HTTP client timeouts.
- [x] Add context propagation, cancellation, and broader bounded request handling.
- [x] Stop using HTTP GET for mutations; use proper write methods or a dedicated internal RPC protocol.
- [x] Replace one-key polling replication with an ordered write log, offsets, acknowledgements, retries, replay, and persistent replica progress.
- [x] Define durability guarantees for BoltDB transactions, fsync behavior, crash recovery, and partially replicated writes.
- [x] Add authentication and TLS for both client traffic and node-to-node traffic.
- [x] Add health and readiness endpoints for shard ownership, DB status, and replication status.
- [x] Add baseline metrics for commands, command errors, HTTP handlers, routing failures, replication events, and replication errors.
- [x] Add detailed latency, QPS, replication lag, DB write, queue depth, and active connection metrics.
- [x] Add integration tests for multi-node routing, persistence after restart, replica catch-up, delete propagation, empty values, concurrent clients, and node restarts.

### P1: Availability and Distributed Behavior

- [ ] Add leader election or explicit failover with fencing to prevent split-brain writes.
- [ ] Introduce cluster membership with node IDs, roles, shard ownership, and versioned cluster metadata.
- [ ] Replace `hash % shardCount` with consistent hashing or slot-based sharding.
- [ ] Implement resharding, shard migration, ownership handoff, and cleanup of moved keys.
- [ ] Add write fencing so old masters cannot keep accepting writes after failover or ownership changes.
- [ ] Add backpressure and resource limits for connections, command size, value size, queues, memory, and overload behavior.
- [ ] Define replica read policy: leader-only, stale replica reads, or bounded-staleness reads.
- [ ] Add backup, restore, and backup verification procedures.
- [ ] Add graceful shutdown that drains active requests and stops replication cleanly.
- [ ] Add operational packaging such as Docker, config validation, Kubernetes/systemd examples, documented ports, volumes, and upgrade steps.

### P2: Performance and Compatibility

- [ ] Add benchmarks and profiling for RESP parsing, GET/SET/DEL, routing, BoltDB writes, replication throughput, and concurrent clients.
- [ ] Batch or stream replication instead of polling one key at a time.
- [ ] Add internal connection pooling for routed commands and replication.
- [ ] Decide Redis compatibility scope and improve command behavior, including `DEL` integer replies, multi-key behavior, TTLs, and command metadata.
- [ ] Add expiration support with active/passive expiration and replicated TTL state.
- [ ] Decide whether the data model stays string-only or expands to hashes, lists, sets, and other Redis types.
- [ ] Add admin APIs for cluster state, replication status, shard ownership, and node draining.
- [ ] Add chaos testing for process kills, network partitions, slow disks, delayed replication, corrupt DB files, partial writes, clock changes, and restart loops.
- [ ] Add security hardening: input validation, rate limiting, audit logs, secret management, and secure defaults.
- [ ] Document architecture, guarantees, configuration, failure modes, runbooks, backup/restore, and known limitations.

## Acknowledgements

This project distributed mode was implemented by following this repo: https://github.com/YuriyNasretdinov/distribkv
