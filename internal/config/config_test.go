package config

import (
	"fmt"
	"testing"
)

func TestParseShardsKeepsReplicaAddresses(t *testing.T) {
	shards, err := ParseShards([]Shard{
		{Name: "a", Idx: 0, Address: "127.0.0.1:8080", Replicas: []string{"127.0.0.1:8090"}},
		{Name: "b", Idx: 1, Address: "127.0.0.1:8081", Replicas: []string{"127.0.0.1:8091"}},
	}, "a")
	if err != nil {
		t.Fatalf("ParseShards() error = %v", err)
	}

	if !shards.IsReplicaAddr(0, "127.0.0.1:8090") {
		t.Fatal("expected configured replica address to be recognized")
	}
	if shards.IsReplicaAddr(0, "127.0.0.1:8091") {
		t.Fatal("unexpected replica address recognized for shard")
	}
}

func testShards(t *testing.T, count int) *Shards {
	t.Helper()

	shardList := make([]Shard, count)
	for i := range shardList {
		shardList[i] = Shard{
			Name:    fmt.Sprintf("shard-%d", i),
			Idx:     i,
			Address: fmt.Sprintf("127.0.0.1:%d", 8080+i),
		}
	}

	shards, err := ParseShards(shardList, "shard-0")
	if err != nil {
		t.Fatalf("ParseShards() error = %v", err)
	}
	return shards
}

func TestIndexIsDeterministicAndInRange(t *testing.T) {
	shards := testShards(t, 4)

	for _, key := range []string{"same-key", "hello", ""} {
		first := shards.Index(key)
		if first < 0 || first >= shards.Count {
			t.Fatalf("Index(%q) = %d, outside shard range", key, first)
		}
		if second := shards.Index(key); second != first {
			t.Fatalf("Index(%q) not deterministic: %d != %d", key, first, second)
		}
	}
}

func TestIndexUsesConsistentHashingRing(t *testing.T) {
	shards := testShards(t, 4)

	counts := make(map[int]int)
	const keys = 20000
	for i := 0; i < keys; i++ {
		counts[shards.Index(fmt.Sprintf("key-%d", i))]++
	}

	if len(counts) != shards.Count {
		t.Fatalf("keys reached %d shards, want all %d", len(counts), shards.Count)
	}
	want := keys / shards.Count
	for idx, count := range counts {
		if count < want/2 || count > want*3/2 {
			t.Fatalf("shard %d owns %d/%d keys, outside tolerance around %d", idx, count, keys, want)
		}
	}
}
