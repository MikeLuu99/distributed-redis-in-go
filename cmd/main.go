package main

import (
	"flag"
	"log"
	"net"
	"net/http"

	"redis-go/internal/config"
	"redis-go/internal/db"
	"redis-go/internal/replication"
	"redis-go/internal/server"
	"redis-go/internal/store"
	"redis-go/internal/web"
)

var (
	dbLocation = flag.String("db-location", "", "The path to the bolt db database")
	httpAddr   = flag.String("http-addr", "127.0.0.1:8080", "HTTP host and port")
	redisAddr  = flag.String("redis-addr", ":6380", "Redis RESP protocol host and port")
	configFile = flag.String("config-file", "sharding.toml", "Config file for static sharding")
	shard      = flag.String("shard", "", "The name of the shard for the data")
	replica    = flag.Bool("replica", false, "Whether or not run as a read-only replica")
)

func parseFlags() {
	flag.Parse()
}

func main() {
	parseFlags()

	// Single-node mode (backward compatibility)
	if *dbLocation == "" && *shard == "" {
		startSingleNode()
		return
	}

	// Distributed mode
	startDistributedNode()
}

func startSingleNode() {
	log.Println("Starting in single-node mode...")
	
	listener, err := net.Listen("tcp", *redisAddr)
	if err != nil {
		log.Fatal("error listening:", err)
	}

	kv := store.NewKeyValueStore()
	log.Printf("Redis server started on %s", *redisAddr)

	for {
		c, err := listener.Accept()
		if err != nil {
			log.Println("error accepting connection:", err)
			continue
		}

		log.Println("client connected")
		go server.HandleConnection(c, kv, nil)
	}
}

func startDistributedNode() {
	log.Println("Starting in distributed mode...")

	if *dbLocation == "" {
		log.Fatalf("Must provide db-location in distributed mode")
	}

	if *shard == "" {
		log.Fatalf("Must provide shard in distributed mode")
	}

	c, err := config.ParseFile(*configFile)
	if err != nil {
		log.Fatalf("Error parsing config %q: %v", *configFile, err)
	}

	shards, err := config.ParseShards(c.Shards, *shard)
	if err != nil {
		log.Fatalf("Error parsing shards config: %v", err)
	}

	log.Printf("Shard count is %d, current shard: %d", shards.Count, shards.CurIdx)

	database, close, err := db.NewDatabase(*dbLocation, *replica)
	if err != nil {
		log.Fatalf("Error creating %q: %v", *dbLocation, err)
	}
	defer close()

	// Create store with persistence
	kv := store.NewKeyValueStoreWithDB(database)

	// Start replication if this is a replica
	if *replica {
		leaderAddr, ok := shards.Addrs[shards.CurIdx]
		if !ok {
			log.Fatalf("Could not find address for leader for shard %d", shards.CurIdx)
		}
		go replication.ClientLoop(database, leaderAddr)
	}

	// Start HTTP server for cluster communication
	srv := web.NewServer(database, shards)
	http.HandleFunc("/get", srv.GetHandler)
	http.HandleFunc("/set", srv.SetHandler)
	http.HandleFunc("/del", srv.DelHandler)
	http.HandleFunc("/purge", srv.DeleteExtraKeysHandler)
	http.HandleFunc("/next-replication-key", srv.GetNextKeyForReplication)
	http.HandleFunc("/delete-replication-key", srv.DeleteReplicationKey)

	go func() {
		log.Printf("HTTP server listening on %s", *httpAddr)
		log.Fatal(http.ListenAndServe(*httpAddr, nil))
	}()

	// Start Redis RESP server
	listener, err := net.Listen("tcp", *redisAddr)
	if err != nil {
		log.Fatal("error listening:", err)
	}

	log.Printf("Redis server started on %s", *redisAddr)

	for {
		c, err := listener.Accept()
		if err != nil {
			log.Println("error accepting connection:", err)
			continue
		}

		log.Println("client connected")
		go server.HandleConnection(c, kv, shards)
	}
}
