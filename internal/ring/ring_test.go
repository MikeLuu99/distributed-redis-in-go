package ring

import (
	"fmt"
	"sort"
	"testing"
)

func TestGetIsDeterministicAndReturnsMember(t *testing.T) {
	r := New(DefaultVnodes, 0, 1, 2, 3)

	for _, key := range []string{"a", "hello", "user:42:profile", ""} {
		first := r.Get(key)
		if first < 0 || first > 3 {
			t.Fatalf("Get(%q) = %d, outside member set", key, first)
		}
		if second := r.Get(key); second != first {
			t.Fatalf("Get(%q) not deterministic: %d != %d", key, first, second)
		}
	}
}

func TestGetOnEmptyRingReturnsNegativeOne(t *testing.T) {
	r := New(DefaultVnodes)
	if got := r.Get("key"); got != -1 {
		t.Fatalf("Get() on empty ring = %d, want -1", got)
	}
}

func TestDistributionIsRoughlyEven(t *testing.T) {
	const members = 4
	const keys = 20000
	r := New(DefaultVnodes, 0, 1, 2, 3)

	counts := make(map[int]int)
	for i := 0; i < keys; i++ {
		counts[r.Get(fmt.Sprintf("key-%d", i))]++
	}

	want := keys / members
	for idx, count := range counts {
		if count < want/2 || count > want*3/2 {
			t.Fatalf("shard %d owns %d/%d keys, outside tolerance around %d", idx, count, keys, want)
		}
	}
	if len(counts) != members {
		t.Fatalf("keys reached %d shards, want all %d", len(counts), members)
	}
}

func TestAddRemapsOnlySmallFractionOfKeys(t *testing.T) {
	const keys = 20000
	before := New(DefaultVnodes, 0, 1, 2)
	after := New(DefaultVnodes, 0, 1, 2, 3)

	remapped := 0
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("key-%d", i)
		if before.Get(key) != after.Get(key) {
			remapped++
		}
	}

	// Adding one shard to three should remap ~25%; with virtual
	// nodes allow a generous margin well below modulo's ~75%.
	if remapped > keys/3 {
		t.Fatalf("adding a shard remapped %d/%d keys, want <= 1/3", remapped, keys)
	}
}

func TestRemoveKeepsOtherKeysInPlace(t *testing.T) {
	const keys = 20000
	full := New(DefaultVnodes, 0, 1, 2, 3)
	reduced := New(DefaultVnodes, 0, 1, 2)
	reduced.Remove(3)

	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("key-%d", i)
		if full.Get(key) == 3 {
			continue
		}
		if got, want := reduced.Get(key), full.Get(key); got != want {
			t.Fatalf("key %q moved from shard %d to %d after removing an unrelated shard", key, want, got)
		}
	}
}

func TestAddRemoveRoundTripRestoresOwnership(t *testing.T) {
	const keys = 5000
	full := New(DefaultVnodes, 0, 1, 2, 3)
	work := New(DefaultVnodes, 0, 1, 2)

	work.Add(3)
	work.Remove(3)
	work.Add(3)

	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("key-%d", i)
		if got, want := work.Get(key), full.Get(key); got != want {
			t.Fatalf("key %q = shard %d, want shard %d after round trip", key, got, want)
		}
	}
}

func TestRemoveMissingMemberIsNoop(t *testing.T) {
	r := New(DefaultVnodes, 0, 1, 2)
	snapshot := append([]int(nil), r.Members()...)

	r.Remove(99)
	if got, want := r.Members(), snapshot; len(got) != len(want) {
		t.Fatalf("Members() = %v, want %v", got, want)
	}
}

func TestMembersReturnsSortedDistinctIndexes(t *testing.T) {
	r := New(DefaultVnodes, 3, 1, 2, 1, 0)
	got := r.Members()
	if !sort.IntsAreSorted(got) {
		t.Fatalf("Members() = %v, not sorted", got)
	}
	if len(got) != 4 {
		t.Fatalf("Members() = %v, want 4 distinct members", got)
	}
	for i, m := range got {
		if m != i {
			t.Fatalf("Members() = %v, want [0 1 2 3]", got)
		}
	}
}

func TestNewFallsBackToDefaultVnodes(t *testing.T) {
	if got := New(0, 0).vnodes; got != DefaultVnodes {
		t.Fatalf("vnodes = %d, want default %d", got, DefaultVnodes)
	}
	if got := New(-5, 0).vnodes; got != DefaultVnodes {
		t.Fatalf("vnodes = %d, want default %d", got, DefaultVnodes)
	}
}
