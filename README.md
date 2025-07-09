# Redis-Go

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
*   **Sharding**: Consistent hash-based key distribution across multiple nodes
*   **Replication**: Master-replica setup with automatic synchronization
*   **Persistence**: BoltDB-based storage for data durability
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
    redis-cli -p 6381  # Saigon shard
    redis-cli -p 6382  # Danang shard
    redis-cli -p 6383  # Thanh Hoa shard
    ```

    **Or connect to replicas (read-only):**
    ```sh
    redis-cli -p 6390  # Hanoi replica
    redis-cli -p 6391  # Saigon replica
    redis-cli -p 6392  # Danang replica
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

In distributed mode, keys are automatically routed to the correct shard based on consistent hashing, so you can connect to any shard and the system will handle routing transparently.

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
*   **`internal/config`**: Handles TOML configuration parsing and consistent hashing for sharding
*   **`internal/db`**: BoltDB-based persistence layer for data durability
*   **`internal/replication`**: Implements master-replica synchronization for high availability
*   **`internal/server`**: Enhanced server with automatic request routing between shards
*   **`internal/store`**: Thread-safe key-value store with optional persistence backend
*   **`internal/web`**: HTTP handlers for inter-node communication and cluster management
*   **`persistence/`**: Directory containing all BoltDB database files (ignored by git)
*   **`sharding.toml`**: Configuration file defining cluster topology and shard assignments
*   **`launch.sh`**: Convenient script to start the entire distributed cluster
*   **`.gitignore`**: Git ignore file excluding database files and build artifacts

## Acknowledgements

This project distributed mode was implemented following this repo: https://github.com/YuriyNasretdinov/distribkv
