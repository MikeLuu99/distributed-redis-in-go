package main

import (
	"context"
	"crypto/tls"
	"errors"
	"expvar"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
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
	shutdownTimeout       = 10 * time.Second
	defaultMaxConnections = 1024
)

var (
	dbLocation     = flag.String("db-location", "", "The path to the bolt db database")
	httpAddr       = flag.String("http-addr", "127.0.0.1:8080", "HTTP host and port")
	redisAddr      = flag.String("redis-addr", ":6380", "Redis RESP protocol host and port")
	configFile     = flag.String("config-file", "sharding.toml", "Config file for static sharding")
	shard          = flag.String("shard", "", "The name of the shard for the data")
	replica        = flag.Bool("replica", false, "Whether or not run as a read-only replica")
	authToken      = flag.String("auth-token", "", "Optional Redis AUTH token for client connections")
	internalToken  = flag.String("internal-auth-token", "", "Optional bearer token for internal HTTP APIs")
	tlsCertFile    = flag.String("tls-cert-file", "", "Optional TLS certificate file for Redis and HTTP listeners")
	tlsKeyFile     = flag.String("tls-key-file", "", "Optional TLS private key file for Redis and HTTP listeners")
	maxConnections = flag.Int("max-connections", defaultMaxConnections, "Maximum concurrent Redis client connections")
)

func parseFlags() {
	flag.Parse()
}

func main() {
	parseFlags()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Single-node mode (backward compatibility)
	if *dbLocation == "" && *shard == "" {
		startSingleNode(ctx)
		return
	}

	// Distributed mode
	startDistributedNode(ctx)
}

func startSingleNode(ctx context.Context) {
	log.Println("Starting in single-node mode...")

	listener, err := listenTCP(*redisAddr, *tlsCertFile, *tlsKeyFile)
	if err != nil {
		log.Fatal("error listening:", err)
	}

	kv := store.NewKeyValueStore()
	log.Printf("Redis server started on %s", *redisAddr)

	respServer := newRESPServer(listener, *maxConnections, func(c net.Conn) {
		server.HandleConnectionWithOptions(c, kv, nil, server.Options{AuthToken: *authToken})
	})
	if err := respServer.Serve(ctx); err != nil {
		log.Fatal("RESP server error:", err)
	}
}

func startDistributedNode(ctx context.Context) {
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
		go replication.ClientLoopWithContext(ctx, database, leaderAddr)
	}

	// Start HTTP server for cluster communication
	srv := web.NewServerWithAuth(database, shards, *internalToken)
	mux := newHTTPMux(srv)
	httpServer := newHTTPServer(*httpAddr, mux)

	go func() {
		log.Printf("HTTP server listening on %s", *httpAddr)
		if err := serveHTTP(httpServer, *tlsCertFile, *tlsKeyFile); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP shutdown error: %v", err)
		}
	}()

	// Start Redis RESP server
	listener, err := listenTCP(*redisAddr, *tlsCertFile, *tlsKeyFile)
	if err != nil {
		log.Fatal("error listening:", err)
	}

	log.Printf("Redis server started on %s", *redisAddr)

	respServer := newRESPServer(listener, *maxConnections, func(c net.Conn) {
		server.HandleConnectionWithOptions(c, kv, shards, server.Options{
			AuthToken: *authToken,
			ReadOnly:  *replica,
		})
	})
	if err := respServer.Serve(ctx); err != nil {
		log.Fatal("RESP server error:", err)
	}
}

type respServer struct {
	listener       net.Listener
	maxConnections int
	handler        func(net.Conn)

	mu     sync.Mutex
	active map[net.Conn]struct{}
	wg     sync.WaitGroup
}

func newRESPServer(listener net.Listener, maxConnections int, handler func(net.Conn)) *respServer {
	if maxConnections <= 0 {
		maxConnections = defaultMaxConnections
	}
	return &respServer{
		listener:       listener,
		maxConnections: maxConnections,
		handler:        handler,
		active:         make(map[net.Conn]struct{}),
	}
}

func (s *respServer) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		s.listener.Close()
		s.closeActive()
	}()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				s.wg.Wait()
				return nil
			}
			return err
		}

		if !s.addActive(conn) {
			conn.Write([]byte("-ERR max connections reached\r\n"))
			conn.Close()
			continue
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.removeActive(conn)
			s.handler(conn)
		}()
	}
}

func (s *respServer) addActive(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.active) >= s.maxConnections {
		return false
	}
	s.active[conn] = struct{}{}
	return true
}

func (s *respServer) removeActive(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.active, conn)
}

func (s *respServer) closeActive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for conn := range s.active {
		conn.Close()
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

func newHTTPMux(srv *web.Server) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/get", srv.GetHandler)
	mux.HandleFunc("/set", srv.SetHandler)
	mux.HandleFunc("/del", srv.DelHandler)
	mux.HandleFunc("/purge", srv.DeleteExtraKeysHandler)
	mux.HandleFunc("/next-replication-key", srv.GetNextKeyForReplication)
	mux.HandleFunc("/delete-replication-key", srv.DeleteReplicationKey)
	mux.HandleFunc("/backup", srv.BackupHandler)
	mux.HandleFunc("/healthz", srv.HealthHandler)
	mux.HandleFunc("/readyz", srv.ReadinessHandler)
	mux.Handle("/debug/vars", expvar.Handler())
	return mux
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
