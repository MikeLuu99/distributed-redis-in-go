
================================================
FILE: README.md
================================================
# Distrib KV
Sources for the "distributed key-value database series" on YouTube: https://www.youtube.com/playlist?list=PLWwSgbaBp9XrMkjEhmTIC37WX2JfwZp7I



================================================
FILE: launch.sh
================================================
#!/bin/bash
set -e

trap 'killall distribkv' SIGINT

cd $(dirname $0)

killall distribkv || true
sleep 0.1

go install -v

distribkv -db-location=moscow.db -http-addr=127.0.0.2:8080 -config-file=sharding.toml -shard=Moscow &
distribkv -db-location=moscow-r.db -http-addr=127.0.0.22:8080 -config-file=sharding.toml -shard=Moscow -replica &

distribkv -db-location=minsk.db -http-addr=127.0.0.3:8080 -config-file=sharding.toml -shard=Minsk &
distribkv -db-location=minsk-r.db -http-addr=127.0.0.33:8080 -config-file=sharding.toml -shard=Minsk -replica &

distribkv -db-location=kiev.db -http-addr=127.0.0.4:8080 -config-file=sharding.toml -shard=Kiev &
distribkv -db-location=kiev-r.db -http-addr=127.0.0.44:8080 -config-file=sharding.toml -shard=Kiev -replica &

distribkv -db-location=tashkent.db -http-addr=127.0.0.5:8080 -config-file=sharding.toml -shard=Tashkent &
distribkv -db-location=tashkent-r.db -http-addr=127.0.0.55:8080 -config-file=sharding.toml -shard=Tashkent -replica &

wait



================================================
FILE: main.go
================================================
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/YuriyNasretdinov/distribkv/config"
	"github.com/YuriyNasretdinov/distribkv/db"
	"github.com/YuriyNasretdinov/distribkv/replication"
	"github.com/YuriyNasretdinov/distribkv/web"
)

var (
	dbLocation = flag.String("db-location", "", "The path to the bolt db database")
	httpAddr   = flag.String("http-addr", "127.0.0.1:8080", "HTTP host and port")
	configFile = flag.String("config-file", "sharding.toml", "Config file for static sharding")
	shard      = flag.String("shard", "", "The name of the shard for the data")
	replica    = flag.Bool("replica", false, "Whether or not run as a read-only replica")
)

func parseFlags() {
	flag.Parse()

	if *dbLocation == "" {
		log.Fatalf("Must provide db-location")
	}

	if *shard == "" {
		log.Fatalf("Must provide shard")
	}
}

func main() {
	parseFlags()

	c, err := config.ParseFile(*configFile)
	if err != nil {
		log.Fatalf("Error parsing config %q: %v", *configFile, err)
	}

	shards, err := config.ParseShards(c.Shards, *shard)
	if err != nil {
		log.Fatalf("Error parsing shards config: %v", err)
	}

	log.Printf("Shard count is %d, current shard: %d", shards.Count, shards.CurIdx)

	db, close, err := db.NewDatabase(*dbLocation, *replica)
	if err != nil {
		log.Fatalf("Error creating %q: %v", *dbLocation, err)
	}
	defer close()

	if *replica {
		leaderAddr, ok := shards.Addrs[shards.CurIdx]
		if !ok {
			log.Fatalf("Could not find address for leader for shard %d", shards.CurIdx)
		}
		go replication.ClientLoop(db, leaderAddr)
	}

	srv := web.NewServer(db, shards)

	http.HandleFunc("/get", srv.GetHandler)
	http.HandleFunc("/set", srv.SetHandler)
	http.HandleFunc("/purge", srv.DeleteExtraKeysHandler)
	http.HandleFunc("/next-replication-key", srv.GetNextKeyForReplication)
	http.HandleFunc("/delete-replication-key", srv.DeleteReplicationKey)

	log.Fatal(http.ListenAndServe(*httpAddr, nil))
}



================================================
FILE: populate.sh
================================================
#!/bin/bash

for shard in 127.0.0.2:8080; do
    echo $shard
    for i in {1..10000}; do
        curl "http://$shard/set?key=key-$RANDOM&value=value-$RANDOM"
    done
done



================================================
FILE: sharding.toml
================================================
[[shards]]
name = "Moscow"
idx = 0
address = "127.0.0.2:8080"
replicas = ["127.0.0.22:8080"]

[[shards]]
name = "Minsk"
idx = 1
address = "127.0.0.3:8080"
replicas = ["127.0.0.33:8080"]

[[shards]]
name = "Kiev"
idx = 2
address = "127.0.0.4:8080"
replicas = ["127.0.0.44:8080"]

[[shards]]
name = "Tashkent"
idx = 3
address = "127.0.0.5:8080"
replicas = ["127.0.0.55:8080"]



================================================
FILE: cmd/bench/main.go
================================================
package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"
)

var (
	addr           = flag.String("addr", "localhost:8080", "The HTTP host port for the instance that is benchmarked.")
	iterations     = flag.Int("iterations", 1000, "The number of iterations for writing")
	readIterations = flag.Int("read-iterations", 100000, "The number of iterations for reading")
	concurrency    = flag.Int("concurrency", 1, "How many goroutines to run in parallel when doing writes")
)

var httpClient = &http.Client{
	Transport: &http.Transport{
		IdleConnTimeout:     time.Second * 60,
		MaxIdleConns:        300,
		MaxConnsPerHost:     300,
		MaxIdleConnsPerHost: 300,
	},
}

func benchmark(name string, iter int, fn func() string) (qps float64, strs []string) {
	var max time.Duration
	var min = time.Hour

	start := time.Now()
	for i := 0; i < iter; i++ {
		iterStart := time.Now()
		strs = append(strs, fn())
		iterTime := time.Since(iterStart)
		if iterTime > max {
			max = iterTime
		}
		if iterTime < min {
			min = iterTime
		}
	}

	avg := time.Since(start) / time.Duration(iter)
	qps = float64(iter) / (float64(time.Since(start)) / float64(time.Second))
	fmt.Printf("Func %s took %s avg, %.1f QPS, %s max, %s min\n", name, avg, qps, max, min)

	return qps, strs
}

func writeRand() (key string) {
	key = fmt.Sprintf("key-%d", rand.Intn(1000000))
	value := fmt.Sprintf("value-%d", rand.Intn(1000000))

	values := url.Values{}
	values.Set("key", key)
	values.Set("value", value)

	resp, err := httpClient.Get("http://" + (*addr) + "/set?" + values.Encode())
	if err != nil {
		log.Fatalf("Error during set: %v", err)
	}

	io.Copy(ioutil.Discard, resp.Body)
	defer resp.Body.Close()

	return key
}

func readRand(allKeys []string) (key string) {
	key = allKeys[rand.Intn(len(allKeys))]

	values := url.Values{}
	values.Set("key", key)

	resp, err := httpClient.Get("http://" + (*addr) + "/get?" + values.Encode())
	if err != nil {
		log.Fatalf("Error during get: %v", err)
	}
	io.Copy(ioutil.Discard, resp.Body)
	defer resp.Body.Close()

	return key
}

func benchmarkWrite() (allKeys []string) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var totalQPS float64

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			qps, strs := benchmark("write", *iterations, writeRand)
			mu.Lock()
			totalQPS += qps
			allKeys = append(allKeys, strs...)
			mu.Unlock()

			wg.Done()
		}()
	}

	wg.Wait()

	log.Printf("Write total QPS: %.1f, set %d keys", totalQPS, len(allKeys))

	return allKeys
}

func benchmarkRead(allKeys []string) {
	var totalQPS float64
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			qps, _ := benchmark("read", *readIterations, func() string { return readRand(allKeys) })
			mu.Lock()
			totalQPS += qps
			mu.Unlock()

			wg.Done()
		}()
	}

	wg.Wait()

	log.Printf("Read total QPS: %.1f", totalQPS)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()

	fmt.Printf("Running with %d iterations and concurrency level %d\n", *iterations, *concurrency)

	allKeys := benchmarkWrite()

	go benchmarkWrite()
	benchmarkRead(allKeys)
}



================================================
FILE: config/config.go
================================================
package config

import (
	"fmt"
	"hash/fnv"

	"github.com/BurntSushi/toml"
)

// Shard describes a shard that holds the appropriate set of keys.
// Each shard has unique set of keys.
type Shard struct {
	Name    string
	Idx     int
	Address string
}

// Config describes the sharding config.
type Config struct {
	Shards []Shard
}

// ParseFile parses the config and returns it upon success.
func ParseFile(filename string) (Config, error) {
	var c Config
	if _, err := toml.DecodeFile(filename, &c); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Shards represents an easier-to-use representation of
// the sharding config: the shards count, current index and
// the addresses of all other shards too.
type Shards struct {
	Count  int
	CurIdx int
	Addrs  map[int]string
}

// ParseShards converts and verifies the list of shards
// specified in the config into a form that can be used
// for routing.
func ParseShards(shards []Shard, curShardName string) (*Shards, error) {
	shardCount := len(shards)
	shardIdx := -1
	addrs := make(map[int]string)

	for _, s := range shards {
		if _, ok := addrs[s.Idx]; ok {
			return nil, fmt.Errorf("duplicate shard index: %d", s.Idx)
		}

		addrs[s.Idx] = s.Address
		if s.Name == curShardName {
			shardIdx = s.Idx
		}
	}

	for i := 0; i < shardCount; i++ {
		if _, ok := addrs[i]; !ok {
			return nil, fmt.Errorf("shard %d is not found", i)
		}
	}

	if shardIdx < 0 {
		return nil, fmt.Errorf("shard %q was not found", curShardName)
	}

	return &Shards{
		Addrs:  addrs,
		Count:  shardCount,
		CurIdx: shardIdx,
	}, nil
}

// Index returns the shard number for the corresponding key.
func (s *Shards) Index(key string) int {
	h := fnv.New64()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(s.Count))
}



================================================
FILE: config/config_test.go
================================================
package config_test

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/YuriyNasretdinov/distribkv/config"
)

func createConfig(t *testing.T, contents string) config.Config {
	t.Helper()

	f, err := ioutil.TempFile(os.TempDir(), "config.toml")
	if err != nil {
		t.Fatalf("Couldn't create a temp file: %v", err)
	}
	defer f.Close()

	name := f.Name()
	defer os.Remove(name)

	_, err = f.WriteString(contents)
	if err != nil {
		t.Fatalf("Could not write the config contents: %v", err)
	}

	c, err := config.ParseFile(name)
	if err != nil {
		t.Fatalf("Could not parse config: %v", err)
	}

	return c
}

func TestConfigParse(t *testing.T) {
	got := createConfig(t, `[[shards]]
		name = "Moscow"
		idx = 0
		address = "localhost:8080"`)

	want := config.Config{
		Shards: []config.Shard{
			{
				Name:    "Moscow",
				Idx:     0,
				Address: "localhost:8080",
			},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("The config does match: got: %#v, want: %#v", got, want)
	}
}

func TestParseShards(t *testing.T) {
	c := createConfig(t, `
	[[shards]]
		name = "Moscow"
		idx = 0
		address = "localhost:8080"
	[[shards]]
		name = "Minsk"
		idx = 1
		address = "localhost:8081"`)

	got, err := config.ParseShards(c.Shards, "Minsk")
	if err != nil {
		t.Fatalf("Could not parse shards %#v: %v", c.Shards, err)
	}

	want := &config.Shards{
		Count:  2,
		CurIdx: 1,
		Addrs: map[int]string{
			0: "localhost:8080",
			1: "localhost:8081",
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("The shards config does match: got: %#v, want: %#v", got, want)
	}
}



================================================
FILE: db/db.go
================================================
package db

import (
	"bytes"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

var defaultBucket = []byte("default")
var replicaBucket = []byte("replication")

// Database is an open bolt database.
type Database struct {
	db       *bolt.DB
	readOnly bool
}

// NewDatabase returns an instance of a database that we can work with.
func NewDatabase(dbPath string, readOnly bool) (db *Database, closeFunc func() error, err error) {
	boltDb, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, nil, err
	}

	db = &Database{db: boltDb, readOnly: readOnly}
	closeFunc = boltDb.Close

	if err := db.createBuckets(); err != nil {
		closeFunc()
		return nil, nil, fmt.Errorf("creating default bucket: %w", err)
	}

	return db, closeFunc, nil
}

func (d *Database) createBuckets() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(defaultBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(replicaBucket); err != nil {
			return err
		}
		return nil
	})
}

// SetKey sets the key to the requested value into the default database or returns an error.
func (d *Database) SetKey(key string, value []byte) error {
	if d.readOnly {
		return errors.New("read-only mode")
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(defaultBucket).Put([]byte(key), value); err != nil {
			return err
		}

		return tx.Bucket(replicaBucket).Put([]byte(key), value)
	})
}

// SetKeyOnReplica sets the key to the requested value into the default database and does not write
// to the replication queue.
// This method is intended to be used only on replicas.
func (d *Database) SetKeyOnReplica(key string, value []byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(defaultBucket).Put([]byte(key), value)
	})
}

func copyByteSlice(b []byte) []byte {
	if b == nil {
		return nil
	}
	res := make([]byte, len(b))
	copy(res, b)
	return res
}

// GetNextKeyForReplication returns the key and value for the keys that have
// changed and have not yet been applied to replicas.
// If there are no new keys, nil key and value will be returned.
func (d *Database) GetNextKeyForReplication() (key, value []byte, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicaBucket)
		k, v := b.Cursor().First()
		key = copyByteSlice(k)
		value = copyByteSlice(v)
		return nil
	})

	if err != nil {
		return nil, nil, err
	}

	return key, value, nil
}

// DeleteReplicationKey deletes the key from the replication queue
// if the value matches the contents or if the key is already absent.
func (d *Database) DeleteReplicationKey(key, value []byte) (err error) {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicaBucket)

		v := b.Get(key)
		if v == nil {
			return errors.New("key does not exist")
		}

		if !bytes.Equal(v, value) {
			return errors.New("value does not match")
		}

		return b.Delete(key)
	})
}

// GetKey get the value of the requested from a default database.
func (d *Database) GetKey(key string) ([]byte, error) {
	var result []byte
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		result = copyByteSlice(b.Get([]byte(key)))
		return nil
	})

	if err == nil {
		return result, nil
	}
	return nil, err
}

// DeleteExtraKeys deletes the keys that do not belong to this shard.
func (d *Database) DeleteExtraKeys(isExtra func(string) bool) error {
	var keys []string

	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		return b.ForEach(func(k, v []byte) error {
			ks := string(k)
			if isExtra(ks) {
				keys = append(keys, ks)
			}
			return nil
		})
	})

	if err != nil {
		return err
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)

		for _, k := range keys {
			if err := b.Delete([]byte(k)); err != nil {
				return err
			}
		}
		return nil
	})
}



================================================
FILE: db/db_test.go
================================================
package db_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/YuriyNasretdinov/distribkv/db"
)

func createTempDb(t *testing.T, readOnly bool) *db.Database {
	t.Helper()

	f, err := ioutil.TempFile(os.TempDir(), "kvdb")
	if err != nil {
		t.Fatalf("Could not create temp file: %v", err)
	}
	name := f.Name()
	f.Close()
	t.Cleanup(func() { os.Remove(name) })

	db, closeFunc, err := db.NewDatabase(name, readOnly)
	if err != nil {
		t.Fatalf("Could not create a new database: %v", err)
	}
	t.Cleanup(func() { closeFunc() })

	return db
}

func TestGetSet(t *testing.T) {
	db := createTempDb(t, false)

	if err := db.SetKey("party", []byte("Great")); err != nil {
		t.Fatalf("Could not write key: %v", err)
	}

	value, err := db.GetKey("party")
	if err != nil {
		t.Fatalf(`Could not get the key "party": %v`, err)
	}

	if !bytes.Equal(value, []byte("Great")) {
		t.Errorf(`Unexpected value for key "party": got %q, want %q`, value, "Great")
	}

	k, v, err := db.GetNextKeyForReplication()
	if err != nil {
		t.Fatalf(`Unexpected error for GetNextKeyForReplication(): %v`, err)
	}

	if !bytes.Equal(k, []byte("party")) || !bytes.Equal(v, []byte("Great")) {
		t.Errorf(`GetNextKeyForReplication(): got %q, %q; want %q, %q`, k, v, "party", "Great")
	}
}

func TestDeleteReplicationKey(t *testing.T) {
	db := createTempDb(t, false)

	setKey(t, db, "party", "Great")

	k, v, err := db.GetNextKeyForReplication()
	if err != nil {
		t.Fatalf(`Unexpected error for GetNextKeyForReplication(): %v`, err)
	}

	if !bytes.Equal(k, []byte("party")) || !bytes.Equal(v, []byte("Great")) {
		t.Errorf(`GetNextKeyForReplication(): got %q, %q; want %q, %q`, k, v, "party", "Great")
	}

	if err := db.DeleteReplicationKey([]byte("party"), []byte("Bad")); err == nil {
		t.Fatalf(`DeleteReplicationKey("party", "Bad"): got nil error, want non-nil error`)
	}

	if err := db.DeleteReplicationKey([]byte("party"), []byte("Great")); err != nil {
		t.Fatalf(`DeleteReplicationKey("party", "Great"): got %q, want nil error`, err)
	}

	k, v, err = db.GetNextKeyForReplication()
	if err != nil {
		t.Fatalf(`Unexpected error for GetNextKeyForReplication(): %v`, err)
	}

	if k != nil || v != nil {
		t.Errorf(`GetNextKeyForReplication(): got %v, %v; want nil, nil`, k, v)
	}
}

func TestSetReadOnly(t *testing.T) {
	db := createTempDb(t, true)

	if err := db.SetKey("party", []byte("Bad")); err == nil {
		t.Fatalf("SetKey(%q, %q): got nil error, want non-nil error", "party", []byte("Bad"))
	}
}

func setKey(t *testing.T, d *db.Database, key, value string) {
	t.Helper()

	if err := d.SetKey(key, []byte(value)); err != nil {
		t.Fatalf("SetKey(%q, %q) failed: %v", key, value, err)
	}
}

func getKey(t *testing.T, d *db.Database, key string) string {
	t.Helper()

	value, err := d.GetKey(key)
	if err != nil {
		t.Fatalf("GetKey(%q) failed: %v", key, err)
	}

	return string(value)
}

func TestDeleteExtraKeys(t *testing.T) {
	db := createTempDb(t, false)

	setKey(t, db, "party", "Great")
	setKey(t, db, "us", "CapitalistPigs")

	if err := db.DeleteExtraKeys(func(name string) bool { return name == "us" }); err != nil {
		t.Fatalf("Could not delete extra keys: %v", err)
	}

	if value := getKey(t, db, "party"); value != "Great" {
		t.Errorf(`Unexpected value for key "party": got %q, want %q`, value, "Great")
	}

	if value := getKey(t, db, "us"); value != "" {
		t.Errorf(`Unexpected value for key "us": got %q, want %q`, value, "")
	}
}



================================================
FILE: replication/replication.go
================================================
package replication

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/YuriyNasretdinov/distribkv/db"
)

// NextKeyValue contains the response for GetNextKeyForReplication.
type NextKeyValue struct {
	Key   string
	Value string
	Err   error
}

type client struct {
	db         *db.Database
	leaderAddr string
}

// ClientLoop continuously downloads new keys from the master and applies them.
func ClientLoop(db *db.Database, leaderAddr string) {
	c := &client{db: db, leaderAddr: leaderAddr}
	for {
		present, err := c.loop()
		if err != nil {
			log.Printf("Loop error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		if !present {
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (c *client) loop() (present bool, err error) {
	resp, err := http.Get("http://" + c.leaderAddr + "/next-replication-key")
	if err != nil {
		return false, err
	}

	var res NextKeyValue
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if res.Err != nil {
		return false, err
	}

	if res.Key == "" {
		return false, nil
	}

	if err := c.db.SetKeyOnReplica(res.Key, []byte(res.Value)); err != nil {
		return false, err
	}

	if err := c.deleteFromReplicationQueue(res.Key, res.Value); err != nil {
		log.Printf("DeleteKeyFromReplication failed: %v", err)
	}

	return true, nil
}

func (c *client) deleteFromReplicationQueue(key, value string) error {
	u := url.Values{}
	u.Set("key", key)
	u.Set("value", value)

	log.Printf("Deleting key=%q, value=%q from replication queue on %q", key, value, c.leaderAddr)

	resp, err := http.Get("http://" + c.leaderAddr + "/delete-replication-key?" + u.Encode())
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if !bytes.Equal(result, []byte("ok")) {
		return errors.New(string(result))
	}

	return nil
}



================================================
FILE: web/web.go
================================================
package web

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/YuriyNasretdinov/distribkv/config"
	"github.com/YuriyNasretdinov/distribkv/db"
	"github.com/YuriyNasretdinov/distribkv/replication"
)

// Server contains HTTP method handlers to be used for the database.
type Server struct {
	db     *db.Database
	shards *config.Shards
}

// NewServer creates a new instance with HTTP handlers to be used to get and set values.
func NewServer(db *db.Database, s *config.Shards) *Server {
	return &Server{
		db:     db,
		shards: s,
	}
}

func (s *Server) redirect(shard int, w http.ResponseWriter, r *http.Request) {
	url := "http://" + s.shards.Addrs[shard] + r.RequestURI
	fmt.Fprintf(w, "redirecting from shard %d to shard %d (%q)\n", s.shards.CurIdx, shard, url)

	resp, err := http.Get(url)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error redirecting the request: %v", err)
		return
	}
	defer resp.Body.Close()

	io.Copy(w, resp.Body)
}

// GetHandler handles read requests from the database.
func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")

	shard := s.shards.Index(key)

	if shard != s.shards.CurIdx {
		s.redirect(shard, w, r)
		return
	}

	value, err := s.db.GetKey(key)

	fmt.Fprintf(w, "Shard = %d, current shard = %d, addr = %q, Value = %q, error = %v", shard, s.shards.CurIdx, s.shards.Addrs[shard], value, err)
}

// SetHandler handles write requests from the database.
func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")

	shard := s.shards.Index(key)
	if shard != s.shards.CurIdx {
		s.redirect(shard, w, r)
		return
	}

	err := s.db.SetKey(key, []byte(value))
	fmt.Fprintf(w, "Error = %v, shardIdx = %d, current shard = %d", err, shard, s.shards.CurIdx)
}

// DeleteExtraKeysHandler deletes keys that don't belong to the current shard.
func (s *Server) DeleteExtraKeysHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Error = %v", s.db.DeleteExtraKeys(func(key string) bool {
		return s.shards.Index(key) != s.shards.CurIdx
	}))
}

// GetNextKeyForReplication returns the next key for replication.
func (s *Server) GetNextKeyForReplication(w http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(w)
	k, v, err := s.db.GetNextKeyForReplication()
	enc.Encode(&replication.NextKeyValue{
		Key:   string(k),
		Value: string(v),
		Err:   err,
	})
}

// DeleteReplicationKey deletes the key from replica queue.
func (s *Server) DeleteReplicationKey(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	key := r.Form.Get("key")
	value := r.Form.Get("value")

	err := s.db.DeleteReplicationKey([]byte(key), []byte(value))
	if err != nil {
		w.WriteHeader(http.StatusExpectationFailed)
		fmt.Fprintf(w, "error: %v", err)
		return
	}

	fmt.Fprintf(w, "ok")
}



================================================
FILE: web/web_test.go
================================================
package web_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/YuriyNasretdinov/distribkv/config"
	"github.com/YuriyNasretdinov/distribkv/web"

	"github.com/YuriyNasretdinov/distribkv/db"
)

func createShardDb(t *testing.T, idx int) *db.Database {
	t.Helper()

	tmpFile, err := ioutil.TempFile(os.TempDir(), fmt.Sprintf("db%d", idx))
	if err != nil {
		t.Fatalf("Could not create a temp db %d: %v", idx, err)
	}

	tmpFile.Close()

	name := tmpFile.Name()
	t.Cleanup(func() { os.Remove(name) })

	db, closeFunc, err := db.NewDatabase(name, false)
	if err != nil {
		t.Fatalf("Could not create new database %q: %v", name, err)
	}
	t.Cleanup(func() { closeFunc() })

	return db
}

func createShardServer(t *testing.T, idx int, addrs map[int]string) (*db.Database, *web.Server) {
	t.Helper()

	db := createShardDb(t, idx)

	cfg := &config.Shards{
		Addrs:  addrs,
		Count:  len(addrs),
		CurIdx: idx,
	}

	s := web.NewServer(db, cfg)
	return db, s
}

func TestWebServer(t *testing.T) {
	var ts1GetHandler, ts1SetHandler func(w http.ResponseWriter, r *http.Request)
	var ts2GetHandler, ts2SetHandler func(w http.ResponseWriter, r *http.Request)

	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.RequestURI, "/get") {
			ts1GetHandler(w, r)
		} else if strings.HasPrefix(r.RequestURI, "/set") {
			ts1SetHandler(w, r)
		}
	}))
	defer ts1.Close()

	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.RequestURI, "/get") {
			ts2GetHandler(w, r)
		} else if strings.HasPrefix(r.RequestURI, "/set") {
			ts2SetHandler(w, r)
		}
	}))
	defer ts2.Close()

	addrs := map[int]string{
		0: strings.TrimPrefix(ts1.URL, "http://"),
		1: strings.TrimPrefix(ts2.URL, "http://"),
	}

	db1, web1 := createShardServer(t, 0, addrs)
	db2, web2 := createShardServer(t, 1, addrs)

	// Calculated manually and depends on the sharding function.
	keys := map[string]int{
		"Soviet": 1,
		"USA":    0,
	}

	ts1GetHandler = web1.GetHandler
	ts1SetHandler = web1.SetHandler
	ts2GetHandler = web2.GetHandler
	ts2SetHandler = web2.SetHandler

	for key := range keys {
		// Send all to first shard to test redirects.
		_, err := http.Get(fmt.Sprintf(ts1.URL+"/set?key=%s&value=value-%s", key, key))
		if err != nil {
			t.Fatalf("Could not set the key %q: %v", key, err)
		}
	}

	for key := range keys {
		// Send all to first shard to test redirects.
		resp, err := http.Get(fmt.Sprintf(ts1.URL+"/get?key=%s", key))
		if err != nil {
			t.Fatalf("Get key %q error: %v", key, err)
		}
		contents, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("Could read contents of the key %q: %v", key, err)
		}

		want := []byte("value-" + key)
		if !bytes.Contains(contents, want) {
			t.Errorf("Unexpected contents of the key %q: got %q, want the result to contain %q", key, contents, want)
		}

		log.Printf("Contents of key %q: %s", key, contents)
	}

	value1, err := db1.GetKey("USA")
	if err != nil {
		t.Fatalf("USA key error: %v", err)
	}

	want1 := "value-USA"
	if !bytes.Equal(value1, []byte(want1)) {
		t.Errorf("Unexpected value of USA key: got %q, want %q", value1, want1)
	}

	value2, err := db2.GetKey("Soviet")
	if err != nil {
		t.Fatalf("Soviet key error: %v", err)
	}

	want2 := "value-Soviet"
	if !bytes.Equal(value2, []byte(want2)) {
		t.Errorf("Unexpected value of Soviet key: got %q, want %q", value2, want2)
	}
}

