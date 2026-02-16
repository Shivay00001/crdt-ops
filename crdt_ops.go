// Pure CRDT Operations Library
// Conflict-free Replicated Data Types with binary operation format
package crdt

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrInvalidOperation = errors.New("invalid operation")
	ErrMergeConflict    = errors.New("merge conflict")
)

// ReplicaID uniquely identifies a replica
type ReplicaID string

// VectorClock represents logical time across replicas
type VectorClock map[ReplicaID]uint64

// NewVectorClock creates a new vector clock
func NewVectorClock() VectorClock {
	return make(VectorClock)
}

// Increment advances the clock for a replica
func (vc VectorClock) Increment(replica ReplicaID) {
	vc[replica]++
}

// Merge combines two vector clocks
func (vc VectorClock) Merge(other VectorClock) {
	for replica, timestamp := range other {
		if vc[replica] < timestamp {
			vc[replica] = timestamp
		}
	}
}

// Compare returns -1 (less), 0 (concurrent), or 1 (greater)
func (vc VectorClock) Compare(other VectorClock) int {
	less := false
	greater := false
	
	allReplicas := make(map[ReplicaID]bool)
	for r := range vc {
		allReplicas[r] = true
	}
	for r := range other {
		allReplicas[r] = true
	}
	
	for replica := range allReplicas {
		v1 := vc[replica]
		v2 := other[replica]
		
		if v1 < v2 {
			less = true
		} else if v1 > v2 {
			greater = true
		}
	}
	
	if less && greater {
		return 0 // Concurrent
	} else if less {
		return -1
	} else if greater {
		return 1
	}
	return 0
}

// Clone creates a copy of the vector clock
func (vc VectorClock) Clone() VectorClock {
	clone := make(VectorClock)
	for k, v := range vc {
		clone[k] = v
	}
	return clone
}

// --- G-Counter (Grow-only Counter) ---

// GCounter is a grow-only counter CRDT
type GCounter struct {
	replicaID ReplicaID
	counts    map[ReplicaID]uint64
	mu        sync.RWMutex
}

// NewGCounter creates a new G-Counter
func NewGCounter(replicaID ReplicaID) *GCounter {
	return &GCounter{
		replicaID: replicaID,
		counts:    make(map[ReplicaID]uint64),
	}
}

// Increment increases the counter
func (gc *GCounter) Increment(delta uint64) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.counts[gc.replicaID] += delta
}

// Value returns the current counter value
func (gc *GCounter) Value() uint64 {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	
	total := uint64(0)
	for _, count := range gc.counts {
		total += count
	}
	return total
}

// Merge combines this counter with another
func (gc *GCounter) Merge(other *GCounter) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	
	for replica, count := range other.counts {
		if gc.counts[replica] < count {
			gc.counts[replica] = count
		}
	}
}

// Encode serializes the counter
func (gc *GCounter) Encode() ([]byte, error) {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	return json.Marshal(gc.counts)
}

// Decode deserializes the counter
func (gc *GCounter) Decode(data []byte) error {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	return json.Unmarshal(data, &gc.counts)
}

// --- PN-Counter (Positive-Negative Counter) ---

// PNCounter is a counter that can increment and decrement
type PNCounter struct {
	replicaID ReplicaID
	positive  *GCounter
	negative  *GCounter
	mu        sync.RWMutex
}

// NewPNCounter creates a new PN-Counter
func NewPNCounter(replicaID ReplicaID) *PNCounter {
	return &PNCounter{
		replicaID: replicaID,
		positive:  NewGCounter(replicaID),
		negative:  NewGCounter(replicaID),
	}
}

// Increment increases the counter
func (pn *PNCounter) Increment(delta uint64) {
	pn.mu.Lock()
	defer pn.mu.Unlock()
	pn.positive.Increment(delta)
}

// Decrement decreases the counter
func (pn *PNCounter) Decrement(delta uint64) {
	pn.mu.Lock()
	defer pn.mu.Unlock()
	pn.negative.Increment(delta)
}

// Value returns the current counter value
func (pn *PNCounter) Value() int64 {
	pn.mu.RLock()
	defer pn.mu.RUnlock()
	return int64(pn.positive.Value()) - int64(pn.negative.Value())
}

// Merge combines this counter with another
func (pn *PNCounter) Merge(other *PNCounter) {
	pn.mu.Lock()
	defer pn.mu.Unlock()
	pn.positive.Merge(other.positive)
	pn.negative.Merge(other.negative)
}

// Encode serializes the counter
func (pn *PNCounter) Encode() ([]byte, error) {
	pn.mu.RLock()
	defer pn.mu.RUnlock()
	
	posData, _ := pn.positive.Encode()
	negData, _ := pn.negative.Encode()
	
	return json.Marshal(map[string]json.RawMessage{
		"positive": posData,
		"negative": negData,
	})
}

// --- LWW-Register (Last-Write-Wins Register) ---

// LWWRegister is a last-write-wins register CRDT
type LWWRegister struct {
	replicaID ReplicaID
	value     interface{}
	timestamp time.Time
	clock     VectorClock
	mu        sync.RWMutex
}

// NewLWWRegister creates a new LWW register
func NewLWWRegister(replicaID ReplicaID) *LWWRegister {
	return &LWWRegister{
		replicaID: replicaID,
		clock:     NewVectorClock(),
	}
}

// Set updates the register value
func (lww *LWWRegister) Set(value interface{}) {
	lww.mu.Lock()
	defer lww.mu.Unlock()
	
	lww.value = value
	lww.timestamp = time.Now()
	lww.clock.Increment(lww.replicaID)
}

// Get returns the current value
func (lww *LWWRegister) Get() interface{} {
	lww.mu.RLock()
	defer lww.mu.RUnlock()
	return lww.value
}

// Merge combines this register with another
func (lww *LWWRegister) Merge(other *LWWRegister) {
	lww.mu.Lock()
	defer lww.mu.Unlock()
	
	// Compare timestamps
	if other.timestamp.After(lww.timestamp) {
		lww.value = other.value
		lww.timestamp = other.timestamp
		lww.clock = other.clock.Clone()
	} else if other.timestamp.Equal(lww.timestamp) {
		// Use vector clock for tie-breaking
		cmp := lww.clock.Compare(other.clock)
		if cmp < 0 || (cmp == 0 && lww.replicaID < other.replicaID) {
			lww.value = other.value
			lww.timestamp = other.timestamp
			lww.clock.Merge(other.clock)
		}
	}
}

// --- OR-Set (Observed-Remove Set) ---

// Element represents a set element with unique tag
type Element struct {
	Value interface{}
	Tag   string
}

// ORSet is an observed-remove set CRDT
type ORSet struct {
	replicaID ReplicaID
	additions map[string]Element
	removals  map[string]bool
	mu        sync.RWMutex
}

// NewORSet creates a new OR-Set
func NewORSet(replicaID ReplicaID) *ORSet {
	return &ORSet{
		replicaID: replicaID,
		additions: make(map[string]Element),
		removals:  make(map[string]bool),
	}
}

// generateTag creates a unique tag for an element
func (ors *ORSet) generateTag(value interface{}) string {
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("%s-%d-%v", ors.replicaID, timestamp, value)
}

// Add inserts an element into the set
func (ors *ORSet) Add(value interface{}) {
	ors.mu.Lock()
	defer ors.mu.Unlock()
	
	tag := ors.generateTag(value)
	ors.additions[tag] = Element{
		Value: value,
		Tag:   tag,
	}
}

// Remove deletes an element from the set
func (ors *ORSet) Remove(value interface{}) {
	ors.mu.Lock()
	defer ors.mu.Unlock()
	
	// Mark all tags with this value as removed
	for tag, elem := range ors.additions {
		if elem.Value == value {
			ors.removals[tag] = true
		}
	}
}

// Contains checks if a value is in the set
func (ors *ORSet) Contains(value interface{}) bool {
	ors.mu.RLock()
	defer ors.mu.RUnlock()
	
	for tag, elem := range ors.additions {
		if elem.Value == value && !ors.removals[tag] {
			return true
		}
	}
	return false
}

// Elements returns all elements in the set
func (ors *ORSet) Elements() []interface{} {
	ors.mu.RLock()
	defer ors.mu.RUnlock()
	
	seen := make(map[interface{}]bool)
	elements := make([]interface{}, 0)
	
	for tag, elem := range ors.additions {
		if !ors.removals[tag] && !seen[elem.Value] {
			seen[elem.Value] = true
			elements = append(elements, elem.Value)
		}
	}
	
	return elements
}

// Merge combines this set with another
func (ors *ORSet) Merge(other *ORSet) {
	ors.mu.Lock()
	defer ors.mu.Unlock()
	
	// Merge additions
	for tag, elem := range other.additions {
		ors.additions[tag] = elem
	}
	
	// Merge removals
	for tag := range other.removals {
		ors.removals[tag] = true
	}
}

// Encode serializes the set
func (ors *ORSet) Encode() ([]byte, error) {
	ors.mu.RLock()
	defer ors.mu.RUnlock()
	
	return json.Marshal(map[string]interface{}{
		"additions": ors.additions,
		"removals":  ors.removals,
	})
}

// Decode deserializes the set
func (ors *ORSet) Decode(data []byte) error {
	ors.mu.Lock()
	defer ors.mu.Unlock()
	
	var decoded map[string]interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	
	// Reconstruct additions
	if adds, ok := decoded["additions"].(map[string]interface{}); ok {
		for tag, elem := range adds {
			if elemMap, ok := elem.(map[string]interface{}); ok {
				e := Element{
					Value: elemMap["Value"],
					Tag:   fmt.Sprintf("%v", elemMap["Tag"]),
				}
				ors.additions[tag] = e
			}
		}
	}
	
	// Reconstruct removals
	if rems, ok := decoded["removals"].(map[string]interface{}); ok {
		for tag := range rems {
			ors.removals[tag] = true
		}
	}
	
	return nil
}

// --- GC for OR-Set (Tombstone Management) ---

// GarbageCollect removes tombstones for elements not in any active set
func (ors *ORSet) GarbageCollect(knownReplicas []ReplicaID) {
	ors.mu.Lock()
	defer ors.mu.Unlock()
	
	// Remove tombstones that are safely garbage collected
	for tag := range ors.removals {
		if _, exists := ors.additions[tag]; !exists {
			delete(ors.removals, tag)
		}
	}
}

// --- Operation Log for Replication ---

// OpType represents the type of CRDT operation
type OpType byte

const (
	OpGCounterInc OpType = iota
	OpPNCounterInc
	OpPNCounterDec
	OpLWWSet
	OpORSetAdd
	OpORSetRemove
)

// Operation represents a serializable CRDT operation
type Operation struct {
	Type      OpType
	ReplicaID ReplicaID
	Timestamp time.Time
	Data      []byte
}

// Encode serializes an operation
func (op *Operation) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	
	// Write type
	if err := binary.Write(buf, binary.LittleEndian, op.Type); err != nil {
		return nil, err
	}
	
	// Write replica ID length and value
	replicaIDBytes := []byte(op.ReplicaID)
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(replicaIDBytes))); err != nil {
		return nil, err
	}
	buf.Write(replicaIDBytes)
	
	// Write timestamp
	if err := binary.Write(buf, binary.LittleEndian, op.Timestamp.UnixNano()); err != nil {
		return nil, err
	}
	
	// Write data length and value
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(op.Data))); err != nil {
		return nil, err
	}
	buf.Write(op.Data)
	
	return buf.Bytes(), nil
}

// Decode deserializes an operation
func (op *Operation) Decode(data []byte) error {
	buf := bytes.NewReader(data)
	
	// Read type
	if err := binary.Read(buf, binary.LittleEndian, &op.Type); err != nil {
		return err
	}
	
	// Read replica ID
	var replicaIDLen uint16
	if err := binary.Read(buf, binary.LittleEndian, &replicaIDLen); err != nil {
		return err
	}
	replicaIDBytes := make([]byte, replicaIDLen)
	if _, err := buf.Read(replicaIDBytes); err != nil {
		return err
	}
	op.ReplicaID = ReplicaID(replicaIDBytes)
	
	// Read timestamp
	var timestamp int64
	if err := binary.Read(buf, binary.LittleEndian, &timestamp); err != nil {
		return err
	}
	op.Timestamp = time.Unix(0, timestamp)
	
	// Read data
	var dataLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &dataLen); err != nil {
		return err
	}
	op.Data = make([]byte, dataLen)
	if _, err := buf.Read(op.Data); err != nil {
		return err
	}
	
	return nil
}

// --- CRDT State Manager ---

// StateManager manages multiple CRDT instances
type StateManager struct {
	replicaID ReplicaID
	gcounters map[string]*GCounter
	pncounters map[string]*PNCounter
	lwwregs   map[string]*LWWRegister
	orsets    map[string]*ORSet
	mu        sync.RWMutex
}

// NewStateManager creates a new CRDT state manager
func NewStateManager(replicaID ReplicaID) *StateManager {
	return &StateManager{
		replicaID:  replicaID,
		gcounters:  make(map[string]*GCounter),
		pncounters: make(map[string]*PNCounter),
		lwwregs:    make(map[string]*LWWRegister),
		orsets:     make(map[string]*ORSet),
	}
}

// GetOrCreateGCounter gets or creates a G-Counter
func (sm *StateManager) GetOrCreateGCounter(key string) *GCounter {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	if gc, exists := sm.gcounters[key]; exists {
		return gc
	}
	
	gc := NewGCounter(sm.replicaID)
	sm.gcounters[key] = gc
	return gc
}

// GetOrCreatePNCounter gets or creates a PN-Counter
func (sm *StateManager) GetOrCreatePNCounter(key string) *PNCounter {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	if pn, exists := sm.pncounters[key]; exists {
		return pn
	}
	
	pn := NewPNCounter(sm.replicaID)
	sm.pncounters[key] = pn
	return pn
}

// GetOrCreateLWWRegister gets or creates an LWW register
func (sm *StateManager) GetOrCreateLWWRegister(key string) *LWWRegister {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	if lww, exists := sm.lwwregs[key]; exists {
		return lww
	}
	
	lww := NewLWWRegister(sm.replicaID)
	sm.lwwregs[key] = lww
	return lww
}

// GetOrCreateORSet gets or creates an OR-Set
func (sm *StateManager) GetOrCreateORSet(key string) *ORSet {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	if ors, exists := sm.orsets[key]; exists {
		return ors
	}
	
	ors := NewORSet(sm.replicaID)
	sm.orsets[key] = ors
	return ors
}

// MergeFrom merges state from another manager
func (sm *StateManager) MergeFrom(other *StateManager) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	// Merge G-Counters (inline get-or-create to avoid deadlock)
	for key, otherGC := range other.gcounters {
		gc, exists := sm.gcounters[key]
		if !exists {
			gc = NewGCounter(sm.replicaID)
			sm.gcounters[key] = gc
		}
		gc.Merge(otherGC)
	}
	
	// Merge PN-Counters
	for key, otherPN := range other.pncounters {
		pn, exists := sm.pncounters[key]
		if !exists {
			pn = NewPNCounter(sm.replicaID)
			sm.pncounters[key] = pn
		}
		pn.Merge(otherPN)
	}
	
	// Merge LWW-Registers
	for key, otherLWW := range other.lwwregs {
		lww, exists := sm.lwwregs[key]
		if !exists {
			lww = NewLWWRegister(sm.replicaID)
			sm.lwwregs[key] = lww
		}
		lww.Merge(otherLWW)
	}
	
	// Merge OR-Sets
	for key, otherORS := range other.orsets {
		ors, exists := sm.orsets[key]
		if !exists {
			ors = NewORSet(sm.replicaID)
			sm.orsets[key] = ors
		}
		ors.Merge(otherORS)
	}
}

// Snapshot returns a serializable snapshot of all state
func (sm *StateManager) Snapshot() (map[string][]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	snapshot := make(map[string][]byte)
	
	for key, gc := range sm.gcounters {
		data, err := gc.Encode()
		if err != nil {
			return nil, err
		}
		snapshot["gc:"+key] = data
	}
	
	for key, pn := range sm.pncounters {
		data, err := pn.Encode()
		if err != nil {
			return nil, err
		}
		snapshot["pn:"+key] = data
	}
	
	for key, ors := range sm.orsets {
		data, err := ors.Encode()
		if err != nil {
			return nil, err
		}
		snapshot["ors:"+key] = data
	}
	
	return snapshot, nil
}
