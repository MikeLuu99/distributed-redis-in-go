# Distributed Redis in Go

A Redis-like key-value server in Go. It runs as a single node or as a distributed cluster with sharding, replication, and persistence.

## Features

- RESP protocol parser with support for `PING`, `SET`, `GET`, and `DEL`.
- Concurrent client handling with a connection limit.
- Consistent hashing for key-to-shard routing.
- Master-replica replication with an ordered write log.
- BoltDB persistence for every write.
- Optional TLS and authentication for client and internal traffic.

## Getting Started

You need Go 1.24 or later and `redis-cli`. For the Docker option, you also need Docker.

### Build

1. Clone the repository:

   ```sh
   git clone https://github.com/MikeLuu99/distributed-redis-in-go.git
   cd distributed-redis-in-go
   ```

2. Build the server:

   ```sh
   go build -o redis-go ./cmd/main.go
   ```

### Run a single node

```sh
./redis-go
redis-cli -p 6380
```

### Run a cluster

```sh
./launch.sh
redis-cli -p 6380
```

`launch.sh` starts 4 shards on ports 6380-6383 and 4 read-only replicas on ports 6390-6393. Connect to any node. The server routes each key to the shard that owns it.

### Run with Docker

1. Start the cluster:

   ```sh
   docker compose up -d --build
   ```

2. Check readiness:

   ```sh
   curl -fsS http://127.0.0.1:8080/readyz
   ```

3. Stop the cluster:

   ```sh
   docker compose down
   ```

   To remove data volumes as well, use `docker compose down -v`.

## Example Session

```
127.0.0.1:6380> PING
PONG
127.0.0.1:6380> SET mykey "Hello"
OK
127.0.0.1:6380> GET mykey
"Hello"
127.0.0.1:6380> DEL mykey
OK
127.0.0.1:6380> GET mykey
(nil)
```

## Guarantees

- The master persists each write to BoltDB before it confirms the write.
- Replication is asynchronous. Replicas can lag behind the master.
- Replicas reject write commands and serve reads for keys that their shard owns.
- The system does not yet provide automatic failover or online resharding.

## HTTP Endpoints

Each shard serves these endpoints on its HTTP port:

- `GET /healthz`: process liveness.
- `GET /readyz`: database status, shard metadata, and replication queue depth.
- `GET /backup`: BoltDB snapshot.
- `GET /debug/vars`: metrics.

## Project Structure

```
.
├── cmd/            # Entry point for both modes
├── internal/
│   ├── config/     # TOML config and shard routing
│   ├── db/         # BoltDB persistence layer
│   ├── replication/ # Master-replica synchronization
│   ├── resp/       # RESP protocol parser
│   ├── ring/       # Consistent hash ring
│   ├── server/     # Redis command handling and routing
│   ├── store/      # Key-value store
│   └── web/        # Internal HTTP handlers
├── sharding.toml   # Cluster configuration
├── launch.sh       # Multi-node start script
├── Dockerfile
└── docker-compose.yml
```

## Roadmap

Completed:

- Ordered replication log with acknowledgements and retries.
- Timeouts, connection limits, and payload size limits.
- Authentication, TLS, metrics, and health endpoints.
- Consistent hashing for shard routing.
- Integration tests and Docker packaging.

Next:

- Leader election and failover.
- Dynamic cluster membership.
- Online resharding with key migration.

## Acknowledgements

This project started from Yuri's tutorial on distributed KV databases:

- https://www.youtube.com/watch?v=EdPkmJrtTWQ
- https://github.com/YuriyNasretdinov/distribkv
