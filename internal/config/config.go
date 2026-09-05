package config

import (
	"fmt"
	"sync"

	"github.com/BurntSushi/toml"

	"redis-go/internal/ring"
)

// Shard describes a shard that holds the appropriate set of keys.
// Each shard has unique set of keys.
type Shard struct {
	Name     string   `toml:"name"`
	Idx      int      `toml:"idx"`
	Address  string   `toml:"address"`
	Replicas []string `toml:"replicas"`
}

// Config describes the sharding config.
type Config struct {
	Shards []Shard `toml:"shards"`
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
	Count      int
	CurIdx     int
	Addrs      map[int]string
	ReplicaMap map[int][]string

	mu   sync.RWMutex
	ring *ring.Ring
}

// ParseShards converts and verifies the list of shards
// specified in the config into a form that can be used
// for routing.
func ParseShards(shards []Shard, curShardName string) (*Shards, error) {
	shardCount := len(shards)
	shardIdx := -1
	addrs := make(map[int]string)
	replicaMap := make(map[int][]string)

	for _, s := range shards {
		if _, ok := addrs[s.Idx]; ok {
			return nil, fmt.Errorf("duplicate shard index: %d", s.Idx)
		}

		addrs[s.Idx] = s.Address
		replicaMap[s.Idx] = append([]string(nil), s.Replicas...)
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
		Addrs:      addrs,
		Count:      shardCount,
		CurIdx:     shardIdx,
		ReplicaMap: replicaMap,
	}, nil
}

// Index returns the shard number for the corresponding key
// using consistent hashing: each shard occupies many virtual
// positions on a hash ring, so adding or removing a shard only
// remaps the small fraction of keys nearest to its positions
// instead of nearly all keys as modulo hashing would.
func (s *Shards) Index(key string) int {
	s.mu.RLock()
	r := s.ring
	s.mu.RUnlock()
	if r == nil {
		s.mu.Lock()
		if s.ring == nil {
			members := make([]int, s.Count)
			for i := range members {
				members[i] = i
			}
			s.ring = ring.New(ring.DefaultVnodes, members...)
		}
		r = s.ring
		s.mu.Unlock()
	}
	return r.Get(key)
}

// IsReplicaAddr reports whether addr is configured as a replica for shard.
func (s *Shards) IsReplicaAddr(shard int, addr string) bool {
	for _, replicaAddr := range s.ReplicaMap[shard] {
		if replicaAddr == addr {
			return true
		}
	}
	return false
}
