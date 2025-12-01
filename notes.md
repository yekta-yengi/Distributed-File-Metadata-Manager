# Presentation Notes - Distributed File Metadata Manager

## Person 1: Architecture & Core Concepts

**Introduction (1-2 min)**
- "We built a Distributed File Metadata Manager that demonstrates how multiple servers can safely access shared data"
- Main challenges solved: race conditions, data consistency, fault tolerance

**Technology Stack (1 min)**
- Backend: Go + etcd (distributed key-value store)
- Frontend: Vue.js for interactive testing UI
- Docker Compose for running multiple server nodes

**Architecture Overview (2-3 min)**
- 3 file server nodes (ports 8081, 8082, 8083)
- 1 etcd cluster for coordination
- 1 Vue.js frontend for visualization
- Show architecture diagram: Frontend → Servers → etcd

**Distributed Locking Concept (2-3 min)**
- Problem: Two servers updating same file = data corruption
- Solution: Lock-before-write pattern
- etcd provides atomic lock operations using Raft consensus
- Session-based locks with TTL (30 seconds) for fault tolerance
- "If a server crashes, its locks are automatically released"

---

## Person 2: Implementation & Demo

**Key Code Components (2-3 min)**
- `metadata/store.go` - handles locking and file operations
- `server/grpc_server.go` - HTTP REST API endpoints
- `simulation/simulation.go` - 6 demo scenarios
- Explain: AcquireLock → Update → ReleaseLock flow

**Live Demo - Simulations (4-5 min)**

Run and explain each:

1. **Sim 1 - Basic Lock Contention**: "Server-1 gets lock, Server-2 waits"
2. **Sim 2 - Three-Way Race**: "All 3 race, only 1 wins at a time"
3. **Sim 3 - Lock Timeout**: "What happens when lock held too long"
4. **Sim 4 - Cascading Updates**: "Chain reaction: A→B→C"
5. **Sim 5 - Read-Write Conflict**: "Reads work without locks, writes need locks"
6. **Sim 6 - Fair Queue**: "FIFO ordering - first request, first served"

**Conclusion (1 min)**
- Distributed locking prevents data corruption
- etcd ensures consistency across all nodes
- Session TTL provides automatic failure recovery
- "The simulations visually prove these concepts work"

---

## Demo Commands (if needed)

```bash
# Start system
docker compose up -d

# Open UI
# http://localhost:3000

# Or test via curl
curl -X POST http://localhost:8081/api/files \
  -d '{"file_id":"demo","owner":"test"}'

curl -X POST http://localhost:8081/api/locks/demo
curl -X POST http://localhost:8082/api/locks/demo  # This will wait/fail
```

---

## Key Points to Emphasize

| Person 1 | Person 2 |
|----------|----------|
| Why we need distributed locks | How locks are implemented |
| Race condition problem | Live demo showing solution |
| etcd consensus mechanism | Simulation scenarios |
| Fault tolerance (TTL) | UI visualization |
