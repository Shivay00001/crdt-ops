# CRDT Operations Library

A pure Go implementation of Conflict-free Replicated Data Types (CRDTs) with binary serialization format. Build eventually-consistent distributed systems without coordination.

## Features

- **G-Counter**: Grow-only counter for monotonically increasing values
- **PN-Counter**: Positive-negative counter supporting increments and decrements
- **LWW-Register**: Last-write-wins register with vector clock tie-breaking
- **OR-Set**: Observed-remove set with proper tombstone management
- **Vector Clocks**: Full vector clock implementation for causality tracking
- **Binary Serialization**: Efficient binary operation encoding/decoding
- **State Manager**: Centralized management of multiple CRDT instances
- **Thread-Safe**: All operations are safe for concurrent access

## Installation

```bash
go get github.com/Shivay00001/crdt-ops
```

## Usage

```go
package main

import (
    "fmt"
    crdt "github.com/Shivay00001/crdt-ops"
)

func main() {
    // Create counters on two replicas
    counter1 := crdt.NewGCounter("replica-1")
    counter2 := crdt.NewGCounter("replica-2")

    counter1.Increment(5)
    counter2.Increment(3)

    // Merge replicas
    counter1.Merge(counter2)
    fmt.Printf("Merged value: %d\n", counter1.Value()) // 8
}
```

## Testing

```bash
go test -v -race ./...
```

## License

MIT
