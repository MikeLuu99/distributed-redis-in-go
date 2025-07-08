# Redis-Go

A simple Redis-like server implemented in Go from scratch. This project is intended as a learning exercise to understand the basics of how Redis works, including the RESP (REdis Serialization Protocol) and concurrent client handling.

## Features

*   In-memory key-value store.
*   Handles multiple client connections concurrently.
*   Implements a subset of Redis commands:
    *   `PING`
    *   `SET`
    *   `GET`
    *   `DEL`

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

3.  **Run the server:**
    ```sh
    ./redis-go
    ```
    The server will start and listen on port `6380`.

### Connecting to the Server

You can use `redis-cli` to connect to the server and execute commands:

```sh
redis-cli -p 6380
```

Once connected, you can try the supported commands:

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

## Project Structure

```
.
├── cmd/
│   └── main.go         # Main application entry point
├── internal/
│   ├── resp/
│   │   └── parser.go   # RESP protocol parser
│   ├── server/
│   │   └── server.go   # Server logic for handling connections
│   └── store/
│       └── kv.go       # In-memory key-value store
├── go.mod
└── README.md
```

*   **`cmd/main.go`**: The entry point of the application. It initializes the key-value store and starts the TCP server.
*   **`internal/server`**: Contains the server implementation, which handles incoming client connections and command execution.
*   **`internal/resp`**: Implements the parser for the RESP protocol, which is used for communication between the client and the server.
*   **`internal/store`**: Provides a thread-safe, in-memory key-value store.
