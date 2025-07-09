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

# Moscow shard (master)
./redis-go -db-location=persistence/moscow.db -http-addr=127.0.0.1:8080 -redis-addr=127.0.0.1:6380 -config-file=sharding.toml -shard=Moscow &
echo "Started Moscow shard on 127.0.0.1:6380 (HTTP: 127.0.0.1:8080)"

# Moscow replica
./redis-go -db-location=persistence/moscow-r.db -http-addr=127.0.0.1:8090 -redis-addr=127.0.0.1:6390 -config-file=sharding.toml -shard=Moscow -replica &
echo "Started Moscow replica on 127.0.0.1:6390 (HTTP: 127.0.0.1:8090)"

# Minsk shard (master)
./redis-go -db-location=persistence/minsk.db -http-addr=127.0.0.1:8081 -redis-addr=127.0.0.1:6381 -config-file=sharding.toml -shard=Minsk &
echo "Started Minsk shard on 127.0.0.1:6381 (HTTP: 127.0.0.1:8081)"

# Minsk replica
./redis-go -db-location=persistence/minsk-r.db -http-addr=127.0.0.1:8091 -redis-addr=127.0.0.1:6391 -config-file=sharding.toml -shard=Minsk -replica &
echo "Started Minsk replica on 127.0.0.1:6391 (HTTP: 127.0.0.1:8091)"

# Kiev shard (master)
./redis-go -db-location=persistence/kiev.db -http-addr=127.0.0.1:8082 -redis-addr=127.0.0.1:6382 -config-file=sharding.toml -shard=Kiev &
echo "Started Kiev shard on 127.0.0.1:6382 (HTTP: 127.0.0.1:8082)"

# Kiev replica
./redis-go -db-location=persistence/kiev-r.db -http-addr=127.0.0.1:8092 -redis-addr=127.0.0.1:6392 -config-file=sharding.toml -shard=Kiev -replica &
echo "Started Kiev replica on 127.0.0.1:6392 (HTTP: 127.0.0.1:8092)"

# Tashkent shard (master)
./redis-go -db-location=persistence/tashkent.db -http-addr=127.0.0.1:8083 -redis-addr=127.0.0.1:6383 -config-file=sharding.toml -shard=Tashkent &
echo "Started Tashkent shard on 127.0.0.1:6383 (HTTP: 127.0.0.1:8083)"

# Tashkent replica
./redis-go -db-location=persistence/tashkent-r.db -http-addr=127.0.0.1:8093 -redis-addr=127.0.0.1:6393 -config-file=sharding.toml -shard=Tashkent -replica &
echo "Started Tashkent replica on 127.0.0.1:6393 (HTTP: 127.0.0.1:8093)"

echo ""
echo "Redis-Go distributed cluster is running!"
echo "Connect to any shard with redis-cli:"
echo "  redis-cli -p 6380  # Moscow shard"
echo "  redis-cli -p 6381  # Minsk shard"
echo "  redis-cli -p 6382  # Kiev shard"
echo "  redis-cli -p 6383  # Tashkent shard"
echo ""
echo "Replicas (read-only):"
echo "  redis-cli -p 6390  # Moscow replica"
echo "  redis-cli -p 6391  # Minsk replica"
echo "  redis-cli -p 6392  # Kiev replica"
echo "  redis-cli -p 6393  # Tashkent replica"
echo ""
echo "For single-node mode (backward compatibility):"
echo "  ./redis-go"
echo "  redis-cli -p 6380"
echo ""
echo "Press Ctrl+C to stop all nodes..."

wait
