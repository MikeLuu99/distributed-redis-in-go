package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"redis-go/internal/config"
	"redis-go/internal/db"
	"redis-go/internal/replication"
	"redis-go/internal/server"
	"redis-go/internal/store"
	"redis-go/internal/web"
)

const (
	httpReadHeaderTimeout = 5 * time.Second
	httpReadTimeout       = 10 * time.Second
	httpWriteTimeout      = 10 * time.Second
	httpIdleTimeout       = 2 * time.Minute
)

var (
	dbLocation    = flag.String("db-location", "", "The path to the bolt db database")
	httpAddr      = flag.String("http-addr", "127.0.0.1:8080", "HTTP host and port")
	redisAddr     = flag.String("redis-addr", ":6380", "Redis RESP protocol host and port")
	configFile    = flag.String("config-file", "sharding.toml", "Config file for static sharding")
	shard         = flag.String("shard", "", "The name of the shard for the data")
	replica       = flag.Bool("replica", false, "Whether or not run as a read-only replica")
	authToken     = flag.String("auth-token", "", "Optional Redis AUTH token for client connections")
	internalToken = flag.String("internal-auth-token", "", "Optional bearer token for internal HTTP APIs")
	tlsCertFile   = flag.String("tls-cert-file", "", "Optional TLS certificate file for Redis and HTTP listeners")
	tlsKeyFile    = flag.String("tls-key-file", "", "Optional TLS private key file for Redis and HTTP listeners")
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

	listener, err := listenTCP(*redisAddr, *tlsCertFile, *tlsKeyFile)
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
		go server.HandleConnectionWithOptions(c, kv, nil, server.Options{AuthToken: *authToken})
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
	server.SetInternalAuthToken(*internalToken)
	replication.SetInternalAuthToken(*internalToken)

	// Start replication if this is a replica
	if *replica {
		leaderAddr, ok := shards.Addrs[shards.CurIdx]
		if !ok {
			log.Fatalf("Could not find address for leader for shard %d", shards.CurIdx)
		}
		if !shards.IsReplicaAddr(shards.CurIdx, *httpAddr) {
			log.Fatalf("HTTP address %q is not configured as a replica for shard %q", *httpAddr, *shard)
		}
		go replication.ClientLoop(database, leaderAddr)
	}

	// Start HTTP server for cluster communication
	srv := web.NewServerWithAuth(database, shards, *internalToken)
	http.HandleFunc("/get", srv.GetHandler)
	http.HandleFunc("/set", srv.SetHandler)
	http.HandleFunc("/del", srv.DelHandler)
	http.HandleFunc("/purge", srv.DeleteExtraKeysHandler)
	http.HandleFunc("/next-replication-key", srv.GetNextKeyForReplication)
	http.HandleFunc("/delete-replication-key", srv.DeleteReplicationKey)
	http.HandleFunc("/healthz", srv.HealthHandler)
	http.HandleFunc("/readyz", srv.ReadinessHandler)

	go func() {
		log.Printf("HTTP server listening on %s", *httpAddr)
		log.Fatal(serveHTTP(newHTTPServer(*httpAddr, http.DefaultServeMux), *tlsCertFile, *tlsKeyFile))
	}()

	// Start Redis RESP server
	listener, err := listenTCP(*redisAddr, *tlsCertFile, *tlsKeyFile)
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
		go server.HandleConnectionWithOptions(c, kv, shards, server.Options{AuthToken: *authToken})
	}
}

func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
}

func listenTCP(addr, certFile, keyFile string) (net.Listener, error) {
	if certFile == "" && keyFile == "" {
		return net.Listen("tcp", addr)
	}
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("both tls-cert-file and tls-key-file are required")
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return tls.Listen("tcp", addr, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
}

func serveHTTP(server *http.Server, certFile, keyFile string) error {
	if certFile == "" && keyFile == "" {
		return server.ListenAndServe()
	}
	if certFile == "" || keyFile == "" {
		return fmt.Errorf("both tls-cert-file and tls-key-file are required")
	}
	return server.ListenAndServeTLS(certFile, keyFile)
}
