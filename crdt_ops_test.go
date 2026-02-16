package crdt

import (
	"testing"
	"time"
)

func TestGCounter(t *testing.T) {
	gc1 := NewGCounter("replica-1")
	gc2 := NewGCounter("replica-2")

	gc1.Increment(5)
	gc2.Increment(3)

	if gc1.Value() != 5 {
		t.Fatalf("expected 5, got %d", gc1.Value())
	}

	gc1.Merge(gc2)
	if gc1.Value() != 8 {
		t.Fatalf("expected 8 after merge, got %d", gc1.Value())
	}
}

func TestGCounterEncodeDecode(t *testing.T) {
	gc1 := NewGCounter("replica-1")
	gc1.Increment(10)

	data, err := gc1.Encode()
	if err != nil {
		t.Fatal(err)
	}

	gc2 := NewGCounter("replica-2")
	if err := gc2.Decode(data); err != nil {
		t.Fatal(err)
	}

	if gc2.Value() != 10 {
		t.Fatalf("expected 10 after decode, got %d", gc2.Value())
	}
}

func TestPNCounter(t *testing.T) {
	pn := NewPNCounter("replica-1")

	pn.Increment(10)
	pn.Decrement(3)

	if pn.Value() != 7 {
		t.Fatalf("expected 7, got %d", pn.Value())
	}
}

func TestPNCounterMerge(t *testing.T) {
	pn1 := NewPNCounter("replica-1")
	pn2 := NewPNCounter("replica-2")

	pn1.Increment(10)
	pn2.Increment(5)
	pn2.Decrement(2)

	pn1.Merge(pn2)

	if pn1.Value() != 13 { // 10 + 5 - 2
		t.Fatalf("expected 13, got %d", pn1.Value())
	}
}

func TestLWWRegister(t *testing.T) {
	lww := NewLWWRegister("replica-1")

	lww.Set("hello")
	if lww.Get() != "hello" {
		t.Fatalf("expected 'hello', got '%v'", lww.Get())
	}

	lww.Set("world")
	if lww.Get() != "world" {
		t.Fatalf("expected 'world', got '%v'", lww.Get())
	}
}

func TestLWWRegisterMerge(t *testing.T) {
	lww1 := NewLWWRegister("replica-1")
	lww2 := NewLWWRegister("replica-2")

	lww1.Set("old")
	time.Sleep(10 * time.Millisecond)
	lww2.Set("new")

	lww1.Merge(lww2)

	if lww1.Get() != "new" {
		t.Fatalf("expected 'new' (last write wins), got '%v'", lww1.Get())
	}
}

func TestORSet(t *testing.T) {
	ors := NewORSet("replica-1")

	ors.Add("apple")
	ors.Add("banana")

	if !ors.Contains("apple") {
		t.Fatal("expected set to contain 'apple'")
	}
	if !ors.Contains("banana") {
		t.Fatal("expected set to contain 'banana'")
	}

	ors.Remove("apple")
	if ors.Contains("apple") {
		t.Fatal("expected 'apple' to be removed")
	}
	if !ors.Contains("banana") {
		t.Fatal("expected 'banana' to still be present")
	}
}

func TestORSetMerge(t *testing.T) {
	ors1 := NewORSet("replica-1")
	ors2 := NewORSet("replica-2")

	ors1.Add("apple")
	ors2.Add("banana")

	ors1.Merge(ors2)

	if !ors1.Contains("apple") {
		t.Fatal("expected merged set to contain 'apple'")
	}
	if !ors1.Contains("banana") {
		t.Fatal("expected merged set to contain 'banana'")
	}
}

func TestORSetElements(t *testing.T) {
	ors := NewORSet("replica-1")
	ors.Add("a")
	ors.Add("b")
	ors.Add("c")

	elements := ors.Elements()
	if len(elements) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(elements))
	}
}

func TestORSetEncodeDecode(t *testing.T) {
	ors1 := NewORSet("replica-1")
	ors1.Add("hello")
	ors1.Add("world")

	data, err := ors1.Encode()
	if err != nil {
		t.Fatal(err)
	}

	ors2 := NewORSet("replica-2")
	if err := ors2.Decode(data); err != nil {
		t.Fatal(err)
	}

	if !ors2.Contains("hello") {
		t.Fatal("decoded set should contain 'hello'")
	}
}

func TestVectorClock(t *testing.T) {
	vc1 := NewVectorClock()
	vc1.Increment("replica-1")
	vc1.Increment("replica-1")

	vc2 := NewVectorClock()
	vc2.Increment("replica-2")

	// Concurrent
	cmp := vc1.Compare(vc2)
	if cmp != 0 {
		t.Fatalf("expected concurrent (0), got %d", cmp)
	}

	vc1.Merge(vc2)
	// Now vc1 dominates vc2
	cmp = vc1.Compare(vc2)
	if cmp != 1 {
		t.Fatalf("expected greater (1), got %d", cmp)
	}
}

func TestVectorClockClone(t *testing.T) {
	vc := NewVectorClock()
	vc.Increment("r1")

	clone := vc.Clone()
	clone.Increment("r1")

	if vc["r1"] == clone["r1"] {
		t.Fatal("clone should be independent")
	}
}

func TestOperationEncodeDecode(t *testing.T) {
	op := &Operation{
		Type:      OpGCounterInc,
		ReplicaID: "test-replica",
		Timestamp: time.Now(),
		Data:      []byte("test-data"),
	}

	encoded, err := op.Encode()
	if err != nil {
		t.Fatal(err)
	}

	decoded := &Operation{}
	if err := decoded.Decode(encoded); err != nil {
		t.Fatal(err)
	}

	if decoded.Type != op.Type {
		t.Fatalf("type mismatch: %v != %v", decoded.Type, op.Type)
	}
	if decoded.ReplicaID != op.ReplicaID {
		t.Fatalf("replica ID mismatch: %v != %v", decoded.ReplicaID, op.ReplicaID)
	}
	if string(decoded.Data) != string(op.Data) {
		t.Fatalf("data mismatch")
	}
}

func TestStateManagerMerge(t *testing.T) {
	sm1 := NewStateManager("replica-1")
	sm2 := NewStateManager("replica-2")

	gc1 := sm1.GetOrCreateGCounter("visits")
	gc1.Increment(10)

	gc2 := sm2.GetOrCreateGCounter("visits")
	gc2.Increment(5)

	sm1.MergeFrom(sm2)

	merged := sm1.GetOrCreateGCounter("visits")
	if merged.Value() != 15 {
		t.Fatalf("expected 15, got %d", merged.Value())
	}
}

func TestStateManagerSnapshot(t *testing.T) {
	sm := NewStateManager("replica-1")

	gc := sm.GetOrCreateGCounter("counter")
	gc.Increment(42)

	ors := sm.GetOrCreateORSet("tags")
	ors.Add("test")

	snapshot, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	if len(snapshot) != 2 {
		t.Fatalf("expected 2 entries in snapshot, got %d", len(snapshot))
	}
}

func TestGarbageCollect(t *testing.T) {
	ors := NewORSet("replica-1")
	ors.Add("item")
	ors.Remove("item")

	ors.GarbageCollect([]ReplicaID{"replica-1"})

	// After GC, removals should be cleaned up
	if ors.Contains("item") {
		t.Fatal("item should still be removed after GC")
	}
}
