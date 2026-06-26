package config

import "testing"

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

func TestIndexUsesStaticModuloHashing(t *testing.T) {
	shards := &Shards{Count: 4}

	first := shards.Index("same-key")
	second := shards.Index("same-key")
	if first != second {
		t.Fatalf("Index() not deterministic: %d != %d", first, second)
	}
	if first < 0 || first >= shards.Count {
		t.Fatalf("Index() = %d, outside shard range", first)
	}
}
