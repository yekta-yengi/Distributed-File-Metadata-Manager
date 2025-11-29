# Distributed File Metadata Manager - Technical Documentation

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Technology Stack](#3-technology-stack)
4. [Implementation Details](#4-implementation-details)
5. [Distributed Locking Mechanism](#5-distributed-locking-mechanism)
6. [Race Condition Resolution](#6-race-condition-resolution)
7. [API Reference](#7-api-reference)
8. [Running the System](#8-running-the-system)
9. [Testing Guide](#9-testing-guide)

---

## 1. Project Overview

This project implements a **Distributed File Metadata Manager** that simulates multiple file servers accessing shared file metadata. The system demonstrates:

- **Distributed Locking**: Before modifying metadata (renaming, version increment), a distributed lock must be acquired on the file ID
- **Race Condition Resolution**: When multiple nodes attempt concurrent metadata updates, the system ensures data consistency
- **Fault Tolerance**: Using etcd's built-in consensus mechanism for coordination

### Project Structure

```
cmpe541_project_1/
├── main.go                      # Application entry point
├── internal/
│   ├── metadata/
│   │   └── store.go            # Metadata storage and locking logic
│   └── server/
│       └── grpc_server.go      # HTTP REST API server
├── proto/
│   └── metadata.proto          # Protocol Buffer definitions
├── frontend/
│   ├── src/
│   │   ├── App.vue             # Vue.js UI component
│   │   └── main.js             # Vue.js entry point
│   ├── package.json
│   └── vite.config.js
├── docker-compose.yml          # Container orchestration
├── Dockerfile                  # Go application container
└── go.mod                      # Go module dependencies
```

---

## 2. Architecture

### System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Vue.js Frontend (port 3000)                  │
│                    Interactive Testing UI                            │
└─────────────────────────────┬───────────────────────────────────────┘
                              │ HTTP REST API
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        File Server Nodes                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │
│  │   Server 1      │  │   Server 2      │  │   Server 3      │     │
│  │   HTTP: 8081    │  │   HTTP: 8082    │  │   HTTP: 8083    │     │
│  │   gRPC: 50051   │  │   gRPC: 50052   │  │   gRPC: 50053   │     │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘     │
└───────────┼────────────────────┼────────────────────┼───────────────┘
            │                    │                    │
            └────────────────────┼────────────────────┘
                                 │ etcd Client Protocol
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          etcd Cluster                                │
│                     (Distributed Key-Value Store)                    │
│                                                                      │
│   ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐ │
│   │  /metadata/      │  │  /locks/         │  │  /lockinfo/      │ │
│   │  files/{id}      │  │  files/{id}      │  │  files/{id}      │ │
│   │  (File Data)     │  │  (Lock Keys)     │  │  (Lock Status)   │ │
│   └──────────────────┘  └──────────────────┘  └──────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

### Component Communication Flow

```
1. User interacts with Vue.js UI
2. UI sends HTTP request to selected file server
3. File server processes request:
   a. For read operations: directly query etcd
   b. For write operations: acquire distributed lock first
4. etcd ensures consistency across all nodes
5. Response returned to UI
```

---

## 3. Technology Stack

### Backend: Go + etcd

**Why Go?**
- Excellent concurrency support with goroutines
- Strong typing for reliability
- Native gRPC support
- Efficient memory management

**Why etcd?**
- Distributed consensus using Raft protocol
- Built-in distributed locking primitives
- Lease-based session management
- Linearizable reads and writes

### Frontend: Vue.js 3 + Vite

**Why Vue.js?**
- Reactive data binding for real-time updates
- Component-based architecture
- Simple integration with REST APIs

### Containerization: Docker Compose

**Why Docker?**
- Consistent development environment
- Easy multi-node simulation
- Network isolation between services

---

## 4. Implementation Details

### 4.1 Application Entry Point (main.go)

The `main.go` file initializes the application, connects to etcd, and starts the HTTP server.

```go
// main.go - Application Entry Point

package main

import (
    "context"
    "flag"
    "log"
    "os"
    "os/signal"
    "strconv"
    "syscall"
    "time"

    "github.com/cmpe541/file-metadata-manager/internal/server"
    clientv3 "go.etcd.io/etcd/client/v3"
)

var (
    nodeID        = flag.String("node-id", "server-1", "Unique identifier for this node")
    grpcPort      = flag.Int("grpc-port", 50051, "gRPC server port")
    httpPort      = flag.Int("http-port", 8080, "HTTP server port")
    etcdEndpoints = flag.String("etcd-endpoints", "etcd:2379", "etcd endpoints")
)

func main() {
    flag.Parse()

    // Override with environment variables if set
    if envNodeID := os.Getenv("NODE_ID"); envNodeID != "" {
        *nodeID = envNodeID
    }
    // ... additional environment variable handling

    log.Printf("[%s] Starting file metadata server", *nodeID)

    // Connect to etcd with 5 second timeout
    client, err := clientv3.New(clientv3.Config{
        Endpoints:   []string{*etcdEndpoints},
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
        log.Fatalf("[%s] Failed to connect to etcd: %v", *nodeID, err)
    }
    defer client.Close()

    // Create and start server
    srv, err := server.NewServer(client, *nodeID, *grpcPort, *httpPort)
    if err != nil {
        log.Fatalf("[%s] Failed to create server: %v", *nodeID, err)
    }
    defer srv.Close()

    // Setup graceful shutdown
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

    go func() {
        sig := <-sigChan
        log.Printf("[%s] Received signal: %v, shutting down...", *nodeID, sig)
        cancel()
    }()

    // Start server (blocks until context is cancelled)
    if err := srv.Start(ctx); err != nil {
        log.Fatalf("[%s] Server error: %v", *nodeID, err)
    }
}
```

**Key Points:**
- Each node has a unique `nodeID` for identification
- Environment variables allow configuration in Docker
- Graceful shutdown handles SIGINT/SIGTERM signals
- etcd client is shared across all operations

### 4.2 Metadata Store (internal/metadata/store.go)

The metadata store handles all data operations and distributed locking.

#### Data Structures

```go
// FileMetadata represents metadata for a file
type FileMetadata struct {
    FileID         string            `json:"file_id"`
    Owner          string            `json:"owner"`
    Version        int64             `json:"version"`
    CreatedAt      string            `json:"created_at"`
    UpdatedAt      string            `json:"updated_at"`
    LastModifiedBy string            `json:"last_modified_by"`
    Attributes     map[string]string `json:"attributes"`
}

// LockInfo represents lock status for a file
type LockInfo struct {
    FileID     string `json:"file_id"`
    Holder     string `json:"holder"`
    AcquiredAt string `json:"acquired_at"`
    IsLocked   bool   `json:"is_locked"`
}
```

#### Store Initialization

```go
// Store handles metadata storage and distributed locking
type Store struct {
    client  *clientv3.Client
    session *concurrency.Session
    nodeID  string
    locks   map[string]*concurrency.Mutex
    locksMu sync.RWMutex
}

// NewStore creates a new metadata store with a 30-second session TTL
func NewStore(client *clientv3.Client, nodeID string) (*Store, error) {
    // Create a session for distributed locking
    // TTL of 30 seconds means if the node crashes, locks are released after 30s
    session, err := concurrency.NewSession(client, concurrency.WithTTL(30))
    if err != nil {
        return nil, fmt.Errorf("failed to create session: %w", err)
    }

    return &Store{
        client:  client,
        session: session,
        nodeID:  nodeID,
        locks:   make(map[string]*concurrency.Mutex),
    }, nil
}
```

**Key Points:**
- `concurrency.Session` manages the connection to etcd with a TTL (Time-To-Live)
- If a node crashes, its session expires and all its locks are automatically released
- Local `locks` map tracks which locks this node currently holds

#### etcd Key Schema

```go
// metadataKey returns the etcd key for file metadata
func metadataKey(fileID string) string {
    return fmt.Sprintf("/metadata/files/%s", fileID)
}

// lockKey returns the etcd key for file locks
func lockKey(fileID string) string {
    return fmt.Sprintf("/locks/files/%s", fileID)
}

// lockInfoKey returns the etcd key for lock info
func lockInfoKey(fileID string) string {
    return fmt.Sprintf("/lockinfo/files/%s", fileID)
}
```

**etcd Key Hierarchy:**
```
/metadata/files/document-001    → {"file_id":"document-001","owner":"user@example.com",...}
/metadata/files/document-002    → {"file_id":"document-002","owner":"admin@example.com",...}
/locks/files/document-001       → (etcd internal lock key)
/lockinfo/files/document-001    → {"holder":"server-1","is_locked":true,...}
```

#### Creating Files with Transactional Safety

```go
// CreateFile creates a new file metadata entry
func (s *Store) CreateFile(ctx context.Context, fileID, owner string, attributes map[string]string) (*FileMetadata, error) {
    now := time.Now().UTC().Format(time.RFC3339)
    metadata := &FileMetadata{
        FileID:         fileID,
        Owner:          owner,
        Version:        1,
        CreatedAt:      now,
        UpdatedAt:      now,
        LastModifiedBy: s.nodeID,
        Attributes:     attributes,
    }

    data, err := json.Marshal(metadata)
    if err != nil {
        return nil, fmt.Errorf("failed to marshal metadata: %w", err)
    }

    key := metadataKey(fileID)

    // Use transaction to ensure file doesn't already exist
    // This is an atomic compare-and-swap operation
    resp, err := s.client.Txn(ctx).
        If(clientv3.Compare(clientv3.Version(key), "=", 0)).  // If key doesn't exist
        Then(clientv3.OpPut(key, string(data))).              // Then create it
        Commit()

    if err != nil {
        return nil, fmt.Errorf("failed to create file: %w", err)
    }

    if !resp.Succeeded {
        return nil, fmt.Errorf("file already exists: %s", fileID)
    }

    return metadata, nil
}
```

**Key Points:**
- Uses etcd's transaction API for atomic operations
- `Compare(Version(key), "=", 0)` checks if key doesn't exist (version 0)
- Prevents race conditions where two nodes try to create the same file

#### Updating Files with Version Increment

```go
// UpdateFile updates file metadata (requires lock)
func (s *Store) UpdateFile(ctx context.Context, fileID string, attributes map[string]string) (*FileMetadata, error) {
    // Get existing metadata
    metadata, err := s.GetFile(ctx, fileID)
    if err != nil {
        return nil, err
    }

    // Update fields
    metadata.Version++  // Increment version for conflict detection
    metadata.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
    metadata.LastModifiedBy = s.nodeID

    // Merge attributes (add new, update existing)
    if metadata.Attributes == nil {
        metadata.Attributes = make(map[string]string)
    }
    for k, v := range attributes {
        metadata.Attributes[k] = v
    }

    data, err := json.Marshal(metadata)
    if err != nil {
        return nil, fmt.Errorf("failed to marshal metadata: %w", err)
    }

    _, err = s.client.Put(ctx, metadataKey(fileID), string(data))
    if err != nil {
        return nil, fmt.Errorf("failed to update file: %w", err)
    }

    return metadata, nil
}
```

**Key Points:**
- Version is incremented on each update (optimistic locking pattern)
- `LastModifiedBy` tracks which node made the change
- Attributes are merged, not replaced

---

## 5. Distributed Locking Mechanism

### 5.1 How etcd Distributed Locks Work

etcd implements distributed locks using:
1. **Leases**: Time-bound ownership of keys
2. **Revisions**: Globally ordered sequence numbers
3. **Watch API**: Real-time notifications of key changes

```
Lock Acquisition Flow:
┌─────────┐    ┌─────────┐    ┌─────────┐
│ Node A  │    │  etcd   │    │ Node B  │
└────┬────┘    └────┬────┘    └────┬────┘
     │              │              │
     │ TryLock(file-001)           │
     │─────────────>│              │
     │              │              │
     │ Lock Granted │              │
     │<─────────────│              │
     │              │              │
     │              │  TryLock(file-001)
     │              │<─────────────│
     │              │              │
     │              │  Wait (blocked)
     │              │─────────────>│
     │              │              │
     │ Unlock()     │              │
     │─────────────>│              │
     │              │              │
     │              │  Lock Granted
     │              │─────────────>│
```

### 5.2 Lock Acquisition Implementation

```go
// AcquireLock acquires a distributed lock for a file
func (s *Store) AcquireLock(ctx context.Context, fileID string) (*LockInfo, error) {
    s.locksMu.Lock()
    defer s.locksMu.Unlock()

    // Check if we already hold the lock (prevent double-locking)
    if _, exists := s.locks[fileID]; exists {
        return nil, fmt.Errorf("lock already held by this node")
    }

    // Create a new mutex for this file
    // The mutex is bound to our session (with 30s TTL)
    mutex := concurrency.NewMutex(s.session, lockKey(fileID))

    // Try to acquire with 10 second timeout
    // This prevents indefinite blocking
    lockCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
    defer cancel()

    // Lock() blocks until lock is acquired or context times out
    if err := mutex.Lock(lockCtx); err != nil {
        return nil, fmt.Errorf("failed to acquire lock: %w", err)
    }

    // Store the mutex reference for later release
    s.locks[fileID] = mutex

    // Store lock info in etcd for visibility
    lockInfo := &LockInfo{
        FileID:     fileID,
        Holder:     s.nodeID,
        AcquiredAt: time.Now().UTC().Format(time.RFC3339),
        IsLocked:   true,
    }

    data, _ := json.Marshal(lockInfo)
    s.client.Put(ctx, lockInfoKey(fileID), string(data))

    return lockInfo, nil
}
```

**Key Points:**
- `concurrency.Mutex` is etcd's distributed mutex implementation
- Lock is bound to session - if session expires, lock is released
- 10-second timeout prevents deadlocks
- Lock info stored separately for UI visibility

### 5.3 Lock Release Implementation

```go
// ReleaseLock releases a distributed lock for a file
func (s *Store) ReleaseLock(ctx context.Context, fileID string) error {
    s.locksMu.Lock()
    defer s.locksMu.Unlock()

    // Check if we hold the lock
    mutex, exists := s.locks[fileID]
    if !exists {
        return fmt.Errorf("lock not held by this node")
    }

    // Release the lock in etcd
    if err := mutex.Unlock(ctx); err != nil {
        return fmt.Errorf("failed to release lock: %w", err)
    }

    // Remove from local tracking
    delete(s.locks, fileID)

    // Update lock info to show unlocked state
    lockInfo := &LockInfo{
        FileID:   fileID,
        Holder:   "",
        IsLocked: false,
    }
    data, _ := json.Marshal(lockInfo)
    s.client.Put(ctx, lockInfoKey(fileID), string(data))

    return nil
}
```

### 5.4 Session-Based Fault Tolerance

```go
// Session TTL ensures locks are released if a node crashes
session, err := concurrency.NewSession(client, concurrency.WithTTL(30))
```

**Fault Tolerance Scenario:**
```
Time T+0:  Node A acquires lock on file-001
Time T+5:  Node A crashes unexpectedly
Time T+35: Session TTL expires (30 seconds after last heartbeat)
Time T+35: etcd automatically releases lock
Time T+36: Node B can now acquire lock on file-001
```

---

## 6. Race Condition Resolution

### 6.1 The Problem: Concurrent Updates

Without distributed locking:
```
Time T+0:  Node A reads file-001 (version=1)
Time T+0:  Node B reads file-001 (version=1)
Time T+1:  Node A updates file-001 (version=2)
Time T+2:  Node B updates file-001 (version=2)  ← OVERWRITES Node A's changes!
```

### 6.2 The Solution: Lock-Before-Write

With distributed locking:
```
Time T+0:  Node A acquires lock on file-001
Time T+0:  Node B attempts to acquire lock → BLOCKED
Time T+1:  Node A reads and updates file-001 (version=2)
Time T+2:  Node A releases lock
Time T+2:  Node B acquires lock
Time T+3:  Node B reads file-001 (version=2) ← Sees Node A's changes
Time T+4:  Node B updates file-001 (version=3)
Time T+5:  Node B releases lock
```

### 6.3 HTTP Handler Enforcing Lock Requirement

```go
// handleFile handles individual file operations
func (s *Server) handleFile(w http.ResponseWriter, r *http.Request) {
    // ... extract fileID from path ...

    switch r.Method {
    case http.MethodPut:
        // Update file (requires lock)
        // CHECK: Ensure this node holds the lock before allowing update
        if !s.store.IsLockHeld(fileID) {
            json.NewEncoder(w).Encode(map[string]interface{}{
                "success": false,
                "message": "Lock must be acquired before updating",
            })
            return
        }

        // ... proceed with update ...

    case http.MethodDelete:
        // Delete file (also requires lock)
        if !s.store.IsLockHeld(fileID) {
            json.NewEncoder(w).Encode(map[string]interface{}{
                "success": false,
                "message": "Lock must be acquired before deleting",
            })
            return
        }

        // ... proceed with delete ...
    }
}
```

### 6.4 Lock Ownership Check

```go
// IsLockHeld checks if this node holds the lock for a file
func (s *Store) IsLockHeld(fileID string) bool {
    s.locksMu.RLock()
    defer s.locksMu.RUnlock()
    _, exists := s.locks[fileID]
    return exists
}
```

**Key Points:**
- Uses read lock (`RLock`) for performance
- Only checks local state - if we hold the mutex, we have the lock
- Fast O(1) lookup in the map

---

## 7. API Reference

### 7.1 REST API Endpoints

#### Health Check
```http
GET /health

Response:
{
    "status": "healthy",
    "node": "server-1"
}
```

#### Node Information
```http
GET /api/node

Response:
{
    "node_id": "server-1",
    "status": "running",
    "grpc_port": 50051,
    "http_port": 8081
}
```

#### List Files
```http
GET /api/files
GET /api/files?prefix=document

Response:
{
    "success": true,
    "files": [
        {
            "file_id": "document-001",
            "owner": "user@example.com",
            "version": 3,
            "created_at": "2025-11-29T18:00:00Z",
            "updated_at": "2025-11-29T18:30:00Z",
            "last_modified_by": "server-2",
            "attributes": {"type": "pdf", "size": "1024"}
        }
    ]
}
```

#### Create File
```http
POST /api/files
Content-Type: application/json

{
    "file_id": "document-001",
    "owner": "user@example.com",
    "attributes": {"type": "pdf"}
}

Response:
{
    "success": true,
    "metadata": {
        "file_id": "document-001",
        "owner": "user@example.com",
        "version": 1,
        ...
    }
}
```

#### Get File
```http
GET /api/files/{file_id}

Response:
{
    "success": true,
    "metadata": { ... }
}
```

#### Update File (Requires Lock)
```http
PUT /api/files/{file_id}
Content-Type: application/json

{
    "attributes": {"status": "reviewed"}
}

Response (Success):
{
    "success": true,
    "metadata": {
        "version": 2,
        ...
    }
}

Response (No Lock):
{
    "success": false,
    "message": "Lock must be acquired before updating"
}
```

#### Delete File (Requires Lock)
```http
DELETE /api/files/{file_id}

Response:
{
    "success": true,
    "message": "File deleted"
}
```

#### Acquire Lock
```http
POST /api/locks/{file_id}

Response:
{
    "success": true,
    "lock_info": {
        "file_id": "document-001",
        "holder": "server-1",
        "acquired_at": "2025-11-29T18:00:00Z",
        "is_locked": true
    }
}
```

#### Release Lock
```http
DELETE /api/locks/{file_id}

Response:
{
    "success": true,
    "message": "Lock released"
}
```

#### Get Lock Status
```http
GET /api/locks/status/{file_id}

Response:
{
    "success": true,
    "lock_info": {
        "file_id": "document-001",
        "holder": "server-2",
        "is_locked": true
    }
}
```

---

## 8. Running the System

### 8.1 Docker Compose Configuration

```yaml
# docker-compose.yml

services:
  # etcd - Distributed coordination
  etcd:
    image: quay.io/coreos/etcd:v3.5.9
    command:
      - etcd
      - --name=etcd0
      - --data-dir=/etcd-data
      - --advertise-client-urls=http://etcd:2379
      - --listen-client-urls=http://0.0.0.0:2379
    ports:
      - "2379:2379"
    healthcheck:
      test: ["CMD", "etcdctl", "endpoint", "health"]
      interval: 10s

  # File Server Node 1
  file-server-1:
    build: .
    environment:
      - NODE_ID=server-1
      - ETCD_ENDPOINTS=etcd:2379
      - HTTP_PORT=8081
    ports:
      - "8081:8081"
    depends_on:
      etcd:
        condition: service_healthy

  # File Server Node 2
  file-server-2:
    build: .
    environment:
      - NODE_ID=server-2
      - HTTP_PORT=8082
    ports:
      - "8082:8082"

  # File Server Node 3
  file-server-3:
    build: .
    environment:
      - NODE_ID=server-3
      - HTTP_PORT=8083
    ports:
      - "8083:8083"

  # Vue.js Frontend
  frontend:
    image: node:20-alpine
    working_dir: /app
    ports:
      - "3000:3000"
    volumes:
      - ./frontend:/app
    command: sh -c "npm install && npm run dev -- --host 0.0.0.0"
```

### 8.2 Starting the System

```bash
# Start all services
docker compose up -d

# View logs
docker compose logs -f

# Stop all services
docker compose down

# Stop and remove volumes (reset data)
docker compose down -v
```

### 8.3 Accessing Services

| Service | URL | Description |
|---------|-----|-------------|
| Frontend UI | http://localhost:3000 | Interactive testing interface |
| Server 1 API | http://localhost:8081 | File server node 1 |
| Server 2 API | http://localhost:8082 | File server node 2 |
| Server 3 API | http://localhost:8083 | File server node 3 |
| etcd | http://localhost:2379 | Distributed store |

---

## 9. Testing Guide

### 9.1 Manual Testing via UI

1. Open http://localhost:3000
2. Create a file using the "Create" tab
3. Switch to "Lock" tab, enter the file ID, click "Acquire Lock"
4. Switch to "Update" tab, add new attributes, click "Update File"
5. Return to "Lock" tab, click "Release Lock"
6. Switch to another server and repeat

### 9.2 Testing via curl

```bash
# Create a file via Server 1
curl -X POST http://localhost:8081/api/files \
  -H "Content-Type: application/json" \
  -d '{"file_id":"test-file","owner":"tester","attributes":{"type":"test"}}'

# Try to update without lock (should fail)
curl -X PUT http://localhost:8081/api/files/test-file \
  -H "Content-Type: application/json" \
  -d '{"attributes":{"status":"updated"}}'
# Response: {"success":false,"message":"Lock must be acquired before updating"}

# Acquire lock via Server 1
curl -X POST http://localhost:8081/api/locks/test-file
# Response: {"success":true,"lock_info":{"holder":"server-1",...}}

# Try to acquire same lock via Server 2 (should timeout/fail)
curl -X POST http://localhost:8082/api/locks/test-file
# Response: {"success":false,"message":"failed to acquire lock: context deadline exceeded"}

# Update file via Server 1 (should succeed)
curl -X PUT http://localhost:8081/api/files/test-file \
  -H "Content-Type: application/json" \
  -d '{"attributes":{"status":"updated"}}'
# Response: {"success":true,"metadata":{"version":2,...}}

# Release lock via Server 1
curl -X DELETE http://localhost:8081/api/locks/test-file

# Now Server 2 can acquire the lock
curl -X POST http://localhost:8082/api/locks/test-file
# Response: {"success":true,"lock_info":{"holder":"server-2",...}}
```

### 9.3 Race Condition Demo

The UI includes a "Demo" tab that:
1. Creates a demo file (if not exists)
2. Simultaneously attempts to acquire lock from all 3 servers
3. Shows which server won the race
4. Each server updates the file and releases the lock in order

**Expected Behavior:**
- Only ONE server acquires the lock at a time
- Other servers wait their turn
- File version increments correctly (no lost updates)

### 9.4 Querying etcd Directly

```bash
# View all metadata
docker compose exec etcd etcdctl get --prefix /metadata

# View all lock info
docker compose exec etcd etcdctl get --prefix /lockinfo

# Watch for changes in real-time
docker compose exec etcd etcdctl watch --prefix /

# Check cluster health
docker compose exec etcd etcdctl endpoint health
```

---

## Conclusion

This implementation demonstrates key distributed systems concepts:

1. **Distributed Locking**: Using etcd's concurrency primitives to ensure mutual exclusion
2. **Fault Tolerance**: Session-based leases automatically release locks on node failure
3. **Consistency**: All metadata changes go through etcd's consensus mechanism
4. **Race Condition Prevention**: Lock-before-write pattern prevents concurrent modification conflicts

The system can be extended with:
- Multi-node etcd cluster for high availability
- gRPC streaming for real-time updates
- Optimistic locking with version-based conflict detection
- Lease renewal for long-running operations
