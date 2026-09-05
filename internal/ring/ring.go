// Package ring implements consistent hashing for mapping keys
// to shards. Each shard is placed on a hash ring multiple times
// (virtual nodes) so that with few physical shards the keyspace
// is still distributed evenly. When a shard joins or leaves, only
// the keys adjacent to its ring positions are remapped instead of
// nearly all keys as with modulo hashing.
package ring

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

// DefaultVnodes is the number of virtual nodes placed on the
// ring per physical shard.
const DefaultVnodes = 180

// point is a single virtual node position on the ring.
type point struct {
	hash uint64
	idx  int
}

// Ring maps keys to shard indexes using consistent hashing.
// It is safe for concurrent use.
type Ring struct {
	mu     sync.RWMutex
	vnodes int
	points []point
}

// New returns a ring containing the given shard indexes with
// vnodes virtual nodes each. If vnodes is not positive,
// DefaultVnodes is used.
func New(vnodes int, members ...int) *Ring {
	if vnodes <= 0 {
		vnodes = DefaultVnodes
	}
	r := &Ring{vnodes: vnodes}
	for _, m := range members {
		r.insert(m)
	}
	r.sortPoints()
	return r
}

// Add places a shard on the ring.
func (r *Ring) Add(member int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.insert(member)
	r.sortPoints()
}

// Remove takes a shard off the ring. Keys owned by it move to
// the neighboring shards; all other keys keep their owner.
func (r *Ring) Remove(member int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	kept := r.points[:0]
	for _, p := range r.points {
		if p.idx != member {
			kept = append(kept, p)
		}
	}
	r.points = kept
}

// Members returns the sorted, distinct shard indexes on the ring.
func (r *Ring) Members() []int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := make(map[int]struct{}, len(r.points)/max(r.vnodes, 1))
	members := make([]int, 0, len(seen))
	for _, p := range r.points {
		if _, ok := seen[p.idx]; !ok {
			seen[p.idx] = struct{}{}
			members = append(members, p.idx)
		}
	}
	sort.Ints(members)
	return members
}

// Get returns the shard index that owns key, or -1 if the ring
// is empty. The same key always maps to the same shard as long
// as the ring membership does not change.
func (r *Ring) Get(key string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return -1
	}
	h := hash64(key)
	i := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].hash >= h
	})
	if i == len(r.points) {
		i = 0
	}
	return r.points[i].idx
}

func (r *Ring) insert(member int) {
	for i := 0; i < r.vnodes; i++ {
		r.points = append(r.points, point{
			hash: hash64(fmt.Sprintf("shard:%d:%d", member, i)),
			idx:  member,
		})
	}
}

func (r *Ring) sortPoints() {
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash == r.points[j].hash {
			return r.points[i].idx < r.points[j].idx
		}
		return r.points[i].hash < r.points[j].hash
	})
}

// hash64 hashes s with FNV-64 and then applies a splitmix64
// finalizer. FNV alone has weak avalanche behavior on the high
// bits, which produces badly skewed arc sizes when the raw hash
// values are ordered on the ring.
func hash64(s string) uint64 {
	h := fnv.New64()
	h.Write([]byte(s))
	x := h.Sum64()
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
