#!/bin/bash
set -e

trap 'killall redis-go' SIGINT

cd $(dirname $0)

# Kill any existing redis-go processes
killall redis-go || true
sleep 0.1

# Create persistence directory if it doesn't exist
mkdir -p persistence

# Build the application
go build -o redis-go ./cmd/main.go

# Start the shards and their replicas
echo "Starting Redis-Go distributed cluster..."

# Hanoi shard (master)
./redis-go -db-location=persistence/hanoi.db -http-addr=127.0.0.1:8080 -redis-addr=127.0.0.1:6380 -config-file=sharding.toml -shard=Hanoi &
echo "Started Hanoi shard on 127.0.0.1:6380 (HTTP: 127.0.0.1:8080)"

# Hanoi replica
./redis-go -db-location=persistence/hanoi-r.db -http-addr=127.0.0.1:8090 -redis-addr=127.0.0.1:6390 -config-file=sharding.toml -shard=Hanoi -replica &
echo "Started Hanoi replica on 127.0.0.1:6390 (HTTP: 127.0.0.1:8090)"

# Sai Gon shard (master)
./redis-go -db-location=persistence/sai-gon.db -http-addr=127.0.0.1:8081 -redis-addr=127.0.0.1:6381 -config-file=sharding.toml -shard="Sai Gon" &
echo "Started Sai Gon shard on 127.0.0.1:6381 (HTTP: 127.0.0.1:8081)"

# Sai Gon replica
./redis-go -db-location=persistence/sai-gon-r.db -http-addr=127.0.0.1:8091 -redis-addr=127.0.0.1:6391 -config-file=sharding.toml -shard="Sai Gon" -replica &
echo "Started Sai Gon replica on 127.0.0.1:6391 (HTTP: 127.0.0.1:8091)"

# Da Nang shard (master)
./redis-go -db-location=persistence/da-nang.db -http-addr=127.0.0.1:8082 -redis-addr=127.0.0.1:6382 -config-file=sharding.toml -shard="Da Nang" &
echo "Started Da Nang shard on 127.0.0.1:6382 (HTTP: 127.0.0.1:8082)"

# Da Nang replica
./redis-go -db-location=persistence/da-nang-r.db -http-addr=127.0.0.1:8092 -redis-addr=127.0.0.1:6392 -config-file=sharding.toml -shard="Da Nang" -replica &
echo "Started Da Nang replica on 127.0.0.1:6392 (HTTP: 127.0.0.1:8092)"

# Thanh Hoa shard (master)
./redis-go -db-location=persistence/thanh-hoa.db -http-addr=127.0.0.1:8083 -redis-addr=127.0.0.1:6383 -config-file=sharding.toml -shard="Thanh Hoa" &
echo "Started Thanh Hoa shard on 127.0.0.1:6383 (HTTP: 127.0.0.1:8083)"

# Thanh Hoa replica
./redis-go -db-location=persistence/thanh-hoa-r.db -http-addr=127.0.0.1:8093 -redis-addr=127.0.0.1:6393 -config-file=sharding.toml -shard="Thanh Hoa" -replica &
echo "Started Thanh Hoa replica on 127.0.0.1:6393 (HTTP: 127.0.0.1:8093)"

echo ""
echo "Redis-Go distributed cluster is running!"
echo "Connect to any shard with redis-cli:"
echo "  redis-cli -p 6380  # Hanoi shard"
echo "  redis-cli -p 6381  # Sai Gon shard"
echo "  redis-cli -p 6382  # Da Nang shard"
echo "  redis-cli -p 6383  # Thanh Hoa shard"
echo ""
echo "Replicas (read-only):"
echo "  redis-cli -p 6390  # Hanoi replica"
echo "  redis-cli -p 6391  # Sai Gon replica"
echo "  redis-cli -p 6392  # Da Nang replica"
echo "  redis-cli -p 6393  # Thanh Hoa replica"
echo ""
echo "For single-node mode (backward compatibility):"
echo "  ./redis-go"
echo "  redis-cli -p 6380"
echo ""
echo "Press Ctrl+C to stop all nodes..."

wait
