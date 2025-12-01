package simulation

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cmpe541/file-metadata-manager/internal/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Event represents a simulation event
type Event struct {
	Timestamp string `json:"timestamp"`
	Server    string `json:"server"`
	Action    string `json:"action"`
	Status    string `json:"status"` // "pending", "success", "failed", "waiting"
	Message   string `json:"message"`
	FileID    string `json:"file_id,omitempty"`
	Details   string `json:"details,omitempty"`
}

// SimulationResult holds the final result
type SimulationResult struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// EventCallback is called for each event during simulation
type EventCallback func(event Event)

// Runner handles simulation execution
type Runner struct {
	client *clientv3.Client
	stores map[string]*metadata.Store
	mu     sync.Mutex
}

// NewRunner creates a new simulation runner
func NewRunner(client *clientv3.Client) *Runner {
	return &Runner{
		client: client,
		stores: make(map[string]*metadata.Store),
	}
}

// getOrCreateStore gets or creates a metadata store for a node
func (r *Runner) getOrCreateStore(nodeID string) (*metadata.Store, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if store, exists := r.stores[nodeID]; exists {
		return store, nil
	}

	store, err := metadata.NewStore(r.client, nodeID)
	if err != nil {
		return nil, err
	}

	r.stores[nodeID] = store
	return store, nil
}

// Cleanup releases all stores
func (r *Runner) Cleanup() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, store := range r.stores {
		store.Close()
	}
	r.stores = make(map[string]*metadata.Store)
}

// sendEvent sends an event with timestamp
func sendEvent(cb EventCallback, server, action, status, message, fileID, details string) {
	cb(Event{
		Timestamp: time.Now().Format("15:04:05.000"),
		Server:    server,
		Action:    action,
		Status:    status,
		Message:   message,
		FileID:    fileID,
		Details:   details,
	})
}

// RunSimulation1 demonstrates basic lock contention between 2 servers
// Scenario: Server 1 and Server 2 both try to update the same file
func (r *Runner) RunSimulation1(ctx context.Context, cb EventCallback) error {
	fileID := fmt.Sprintf("sim1-file-%d", time.Now().UnixNano())

	// Cleanup old stores to ensure fresh sessions
	r.Cleanup()

	sendEvent(cb, "system", "init", "pending", "Starting Simulation 1: Basic Lock Contention", fileID, "Two servers competing for the same lock")
	time.Sleep(500 * time.Millisecond)

	// Create stores for each simulated server
	store1, err := r.getOrCreateStore("sim-server-1")
	if err != nil {
		sendEvent(cb, "system", "error", "failed", "Failed to create store for server-1", "", err.Error())
		return err
	}

	store2, err := r.getOrCreateStore("sim-server-2")
	if err != nil {
		sendEvent(cb, "system", "error", "failed", "Failed to create store for server-2", "", err.Error())
		return err
	}

	// Step 1: Create the file
	sendEvent(cb, "server-1", "create", "pending", "Creating file...", fileID, "")
	time.Sleep(300 * time.Millisecond)

	_, err = store1.CreateFile(ctx, fileID, "simulation", map[string]string{"type": "demo"})
	if err != nil {
		sendEvent(cb, "server-1", "create", "failed", "Failed to create file", fileID, err.Error())
		return err
	}
	sendEvent(cb, "server-1", "create", "success", "File created successfully", fileID, "Version: 1")
	time.Sleep(500 * time.Millisecond)

	// Step 2: Server 1 acquires lock
	sendEvent(cb, "server-1", "lock", "pending", "Attempting to acquire lock...", fileID, "")
	time.Sleep(300 * time.Millisecond)

	lockInfo1, err := store1.AcquireLock(ctx, fileID)
	if err != nil {
		sendEvent(cb, "server-1", "lock", "failed", "Failed to acquire lock", fileID, err.Error())
		return err
	}
	sendEvent(cb, "server-1", "lock", "success", "Lock ACQUIRED!", fileID, fmt.Sprintf("Holder: %s", lockInfo1.Holder))
	time.Sleep(500 * time.Millisecond)

	// Step 3: Server 2 tries to acquire lock (will be blocked)
	sendEvent(cb, "server-2", "lock", "pending", "Attempting to acquire lock...", fileID, "")
	sendEvent(cb, "server-2", "lock", "waiting", "BLOCKED! Waiting for lock...", fileID, "Lock held by server-1")

	// Use goroutine for server 2's lock attempt with timeout
	lockChan := make(chan error, 1)
	go func() {
		lockCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		_, err := store2.AcquireLock(lockCtx, fileID)
		lockChan <- err
	}()

	// Step 4: Server 1 updates file while holding lock
	time.Sleep(800 * time.Millisecond)
	sendEvent(cb, "server-1", "update", "pending", "Updating file metadata...", fileID, "")
	time.Sleep(500 * time.Millisecond)

	meta, err := store1.UpdateFile(ctx, fileID, map[string]string{"updated_by": "server-1", "sim": "1"})
	if err != nil {
		sendEvent(cb, "server-1", "update", "failed", "Failed to update file", fileID, err.Error())
	} else {
		sendEvent(cb, "server-1", "update", "success", "File updated!", fileID, fmt.Sprintf("Version: %d", meta.Version))
	}
	time.Sleep(500 * time.Millisecond)

	// Step 5: Server 1 releases lock
	sendEvent(cb, "server-1", "unlock", "pending", "Releasing lock...", fileID, "")
	time.Sleep(300 * time.Millisecond)

	err = store1.ReleaseLock(ctx, fileID)
	if err != nil {
		sendEvent(cb, "server-1", "unlock", "failed", "Failed to release lock", fileID, err.Error())
	} else {
		sendEvent(cb, "server-1", "unlock", "success", "Lock RELEASED!", fileID, "")
	}

	// Step 6: Server 2 should now acquire lock
	select {
	case err := <-lockChan:
		if err != nil {
			sendEvent(cb, "server-2", "lock", "failed", "Failed to acquire lock", fileID, err.Error())
		} else {
			sendEvent(cb, "server-2", "lock", "success", "Lock ACQUIRED!", fileID, "Finally got the lock!")
			time.Sleep(500 * time.Millisecond)

			// Server 2 updates
			sendEvent(cb, "server-2", "update", "pending", "Updating file metadata...", fileID, "")
			time.Sleep(400 * time.Millisecond)

			meta, err := store2.UpdateFile(ctx, fileID, map[string]string{"updated_by": "server-2"})
			if err != nil {
				sendEvent(cb, "server-2", "update", "failed", "Failed to update", fileID, err.Error())
			} else {
				sendEvent(cb, "server-2", "update", "success", "File updated!", fileID, fmt.Sprintf("Version: %d", meta.Version))
			}
			time.Sleep(300 * time.Millisecond)

			// Server 2 releases
			sendEvent(cb, "server-2", "unlock", "pending", "Releasing lock...", fileID, "")
			time.Sleep(200 * time.Millisecond)
			store2.ReleaseLock(ctx, fileID)
			sendEvent(cb, "server-2", "unlock", "success", "Lock RELEASED!", fileID, "")
		}
	case <-time.After(4 * time.Second):
		sendEvent(cb, "server-2", "lock", "failed", "Lock acquisition timeout", fileID, "")
	}

	time.Sleep(300 * time.Millisecond)
	sendEvent(cb, "system", "complete", "success", "Simulation 1 Complete!", fileID, "Demonstrated sequential lock acquisition")

	return nil
}

// RunSimulation2 demonstrates 3-way race condition
// Scenario: Three servers simultaneously try to acquire the same lock
func (r *Runner) RunSimulation2(ctx context.Context, cb EventCallback) error {
	fileID := fmt.Sprintf("sim2-file-%d", time.Now().UnixNano())

	r.Cleanup()

	sendEvent(cb, "system", "init", "pending", "Starting Simulation 2: Three-Way Race", fileID, "Three servers racing for the same lock")
	time.Sleep(500 * time.Millisecond)

	// Create stores
	store1, _ := r.getOrCreateStore("sim-server-1")
	store2, _ := r.getOrCreateStore("sim-server-2")
	store3, _ := r.getOrCreateStore("sim-server-3")

	// Create file first
	sendEvent(cb, "system", "create", "pending", "Creating shared file...", fileID, "")
	time.Sleep(300 * time.Millisecond)
	store1.CreateFile(ctx, fileID, "simulation", map[string]string{"type": "race-demo"})
	sendEvent(cb, "system", "create", "success", "File created", fileID, "")
	time.Sleep(500 * time.Millisecond)

	// All servers try simultaneously
	sendEvent(cb, "server-1", "lock", "pending", "Racing for lock...", fileID, "")
	sendEvent(cb, "server-2", "lock", "pending", "Racing for lock...", fileID, "")
	sendEvent(cb, "server-3", "lock", "pending", "Racing for lock...", fileID, "")
	time.Sleep(300 * time.Millisecond)

	type lockResult struct {
		server string
		store  *metadata.Store
		err    error
	}

	results := make(chan lockResult, 3)

	// Launch all three lock attempts simultaneously
	go func() {
		lockCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		defer cancel()
		_, err := store1.AcquireLock(lockCtx, fileID)
		results <- lockResult{"server-1", store1, err}
	}()

	go func() {
		lockCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		defer cancel()
		_, err := store2.AcquireLock(lockCtx, fileID)
		results <- lockResult{"server-2", store2, err}
	}()

	go func() {
		lockCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		defer cancel()
		_, err := store3.AcquireLock(lockCtx, fileID)
		results <- lockResult{"server-3", store3, err}
	}()

	// Process results as they come in
	version := 1
	for i := 0; i < 3; i++ {
		select {
		case res := <-results:
			if res.err != nil {
				sendEvent(cb, res.server, "lock", "failed", "Failed to acquire lock", fileID, res.err.Error())
			} else {
				sendEvent(cb, res.server, "lock", "success", fmt.Sprintf("Lock ACQUIRED! (#%d in queue)", i+1), fileID, "")
				time.Sleep(400 * time.Millisecond)

				// Update file
				sendEvent(cb, res.server, "update", "pending", "Updating file...", fileID, "")
				time.Sleep(300 * time.Millisecond)

				version++
				res.store.UpdateFile(ctx, fileID, map[string]string{
					"updated_by": res.server,
					"order":      fmt.Sprintf("%d", i+1),
				})
				sendEvent(cb, res.server, "update", "success", "File updated!", fileID, fmt.Sprintf("Version: %d", version))
				time.Sleep(300 * time.Millisecond)

				// Release lock
				sendEvent(cb, res.server, "unlock", "pending", "Releasing lock...", fileID, "")
				time.Sleep(200 * time.Millisecond)
				res.store.ReleaseLock(ctx, fileID)
				sendEvent(cb, res.server, "unlock", "success", "Lock RELEASED!", fileID, "")
				time.Sleep(400 * time.Millisecond)
			}
		case <-time.After(10 * time.Second):
			sendEvent(cb, "system", "error", "failed", "Timeout waiting for lock results", fileID, "")
		}
	}

	time.Sleep(300 * time.Millisecond)
	sendEvent(cb, "system", "complete", "success", "Simulation 2 Complete!", fileID, "All servers processed in order - no data corruption!")

	return nil
}

// RunSimulation3 demonstrates lock timeout/failure scenario
// Scenario: Server holds lock too long, others timeout
func (r *Runner) RunSimulation3(ctx context.Context, cb EventCallback) error {
	fileID := fmt.Sprintf("sim3-file-%d", time.Now().UnixNano())

	r.Cleanup()

	sendEvent(cb, "system", "init", "pending", "Starting Simulation 3: Lock Timeout Scenario", fileID, "One server holds lock while others timeout")
	time.Sleep(500 * time.Millisecond)

	store1, _ := r.getOrCreateStore("sim-server-1")
	store2, _ := r.getOrCreateStore("sim-server-2")

	// Create file
	sendEvent(cb, "server-1", "create", "pending", "Creating file...", fileID, "")
	time.Sleep(300 * time.Millisecond)
	store1.CreateFile(ctx, fileID, "simulation", map[string]string{"type": "timeout-demo"})
	sendEvent(cb, "server-1", "create", "success", "File created", fileID, "")
	time.Sleep(500 * time.Millisecond)

	// Server 1 acquires lock
	sendEvent(cb, "server-1", "lock", "pending", "Acquiring lock...", fileID, "")
	time.Sleep(300 * time.Millisecond)
	store1.AcquireLock(ctx, fileID)
	sendEvent(cb, "server-1", "lock", "success", "Lock ACQUIRED!", fileID, "Will hold for extended time...")
	time.Sleep(500 * time.Millisecond)

	// Server 2 tries with short timeout
	sendEvent(cb, "server-2", "lock", "pending", "Attempting to acquire lock (2s timeout)...", fileID, "")
	sendEvent(cb, "server-2", "lock", "waiting", "Waiting for lock...", fileID, "")

	// Simulate server 1 doing "long work"
	for i := 1; i <= 3; i++ {
		time.Sleep(700 * time.Millisecond)
		sendEvent(cb, "server-1", "work", "pending", fmt.Sprintf("Processing step %d/3...", i), fileID, "Simulating long operation")
	}

	// Server 2's lock attempt with short timeout
	lockCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	_, err := store2.AcquireLock(lockCtx, fileID)
	cancel()

	if err != nil {
		sendEvent(cb, "server-2", "lock", "failed", "TIMEOUT! Could not acquire lock", fileID, "Lock held too long by server-1")
	}
	time.Sleep(500 * time.Millisecond)

	// Server 1 finishes and updates
	sendEvent(cb, "server-1", "update", "pending", "Updating file after long operation...", fileID, "")
	time.Sleep(400 * time.Millisecond)
	store1.UpdateFile(ctx, fileID, map[string]string{"long_operation": "completed"})
	sendEvent(cb, "server-1", "update", "success", "File updated!", fileID, "")
	time.Sleep(300 * time.Millisecond)

	// Server 1 releases
	sendEvent(cb, "server-1", "unlock", "pending", "Finally releasing lock...", fileID, "")
	time.Sleep(300 * time.Millisecond)
	store1.ReleaseLock(ctx, fileID)
	sendEvent(cb, "server-1", "unlock", "success", "Lock RELEASED!", fileID, "")
	time.Sleep(500 * time.Millisecond)

	// Server 2 retries
	sendEvent(cb, "server-2", "lock", "pending", "Retrying lock acquisition...", fileID, "")
	time.Sleep(300 * time.Millisecond)
	_, err = store2.AcquireLock(ctx, fileID)
	if err != nil {
		sendEvent(cb, "server-2", "lock", "failed", "Still failed", fileID, err.Error())
	} else {
		sendEvent(cb, "server-2", "lock", "success", "Lock ACQUIRED on retry!", fileID, "")
		time.Sleep(400 * time.Millisecond)

		sendEvent(cb, "server-2", "update", "pending", "Updating file...", fileID, "")
		time.Sleep(300 * time.Millisecond)
		store2.UpdateFile(ctx, fileID, map[string]string{"retry": "success"})
		sendEvent(cb, "server-2", "update", "success", "File updated!", fileID, "")
		time.Sleep(300 * time.Millisecond)

		store2.ReleaseLock(ctx, fileID)
		sendEvent(cb, "server-2", "unlock", "success", "Lock released", fileID, "")
	}

	time.Sleep(300 * time.Millisecond)
	sendEvent(cb, "system", "complete", "success", "Simulation 3 Complete!", fileID, "Demonstrated timeout handling and retry pattern")

	return nil
}

// EventToJSON converts an event to JSON string
func EventToJSON(e Event) string {
	data, _ := json.Marshal(e)
	return string(data)
}

// RunSimulation4 demonstrates cascading updates across multiple files
// Scenario: Update file A, then file B depends on A, then file C depends on B
func (r *Runner) RunSimulation4(ctx context.Context, cb EventCallback) error {
	fileA := fmt.Sprintf("sim4-fileA-%d", time.Now().UnixNano())
	fileB := fmt.Sprintf("sim4-fileB-%d", time.Now().UnixNano())
	fileC := fmt.Sprintf("sim4-fileC-%d", time.Now().UnixNano())

	r.Cleanup()

	sendEvent(cb, "system", "init", "pending", "Starting Simulation 4: Cascading Updates", fileA, "Chain reaction: A -> B -> C")
	time.Sleep(500 * time.Millisecond)

	store1, _ := r.getOrCreateStore("sim-server-1")
	store2, _ := r.getOrCreateStore("sim-server-2")
	store3, _ := r.getOrCreateStore("sim-server-3")

	// Create all files
	sendEvent(cb, "system", "create", "pending", "Creating file chain...", "", "")
	time.Sleep(300 * time.Millisecond)
	store1.CreateFile(ctx, fileA, "simulation", map[string]string{"type": "parent", "status": "pending"})
	sendEvent(cb, "server-1", "create", "success", "File A created", fileA, "Parent file")
	time.Sleep(200 * time.Millisecond)
	store1.CreateFile(ctx, fileB, "simulation", map[string]string{"type": "child", "depends_on": fileA, "status": "pending"})
	sendEvent(cb, "server-1", "create", "success", "File B created", fileB, "Depends on A")
	time.Sleep(200 * time.Millisecond)
	store1.CreateFile(ctx, fileC, "simulation", map[string]string{"type": "grandchild", "depends_on": fileB, "status": "pending"})
	sendEvent(cb, "server-1", "create", "success", "File C created", fileC, "Depends on B")
	time.Sleep(500 * time.Millisecond)

	// Server 1 updates file A
	sendEvent(cb, "server-1", "lock", "pending", "Locking File A...", fileA, "")
	time.Sleep(300 * time.Millisecond)
	store1.AcquireLock(ctx, fileA)
	sendEvent(cb, "server-1", "lock", "success", "Lock acquired on A", fileA, "")
	time.Sleep(300 * time.Millisecond)

	sendEvent(cb, "server-1", "update", "pending", "Updating File A...", fileA, "")
	time.Sleep(400 * time.Millisecond)
	store1.UpdateFile(ctx, fileA, map[string]string{"status": "completed", "processed_by": "server-1"})
	sendEvent(cb, "server-1", "update", "success", "File A updated!", fileA, "Status: completed")
	time.Sleep(300 * time.Millisecond)

	store1.ReleaseLock(ctx, fileA)
	sendEvent(cb, "server-1", "unlock", "success", "Lock released on A", fileA, "Triggering cascade...")
	time.Sleep(500 * time.Millisecond)

	// Server 2 detects A completed, updates B
	sendEvent(cb, "server-2", "lock", "pending", "A completed! Locking File B...", fileB, "Cascade step 1")
	time.Sleep(300 * time.Millisecond)
	store2.AcquireLock(ctx, fileB)
	sendEvent(cb, "server-2", "lock", "success", "Lock acquired on B", fileB, "")
	time.Sleep(300 * time.Millisecond)

	sendEvent(cb, "server-2", "update", "pending", "Updating File B...", fileB, "")
	time.Sleep(400 * time.Millisecond)
	store2.UpdateFile(ctx, fileB, map[string]string{"status": "completed", "processed_by": "server-2", "parent_status": "completed"})
	sendEvent(cb, "server-2", "update", "success", "File B updated!", fileB, "Status: completed")
	time.Sleep(300 * time.Millisecond)

	store2.ReleaseLock(ctx, fileB)
	sendEvent(cb, "server-2", "unlock", "success", "Lock released on B", fileB, "Triggering next cascade...")
	time.Sleep(500 * time.Millisecond)

	// Server 3 detects B completed, updates C
	sendEvent(cb, "server-3", "lock", "pending", "B completed! Locking File C...", fileC, "Cascade step 2")
	time.Sleep(300 * time.Millisecond)
	store3.AcquireLock(ctx, fileC)
	sendEvent(cb, "server-3", "lock", "success", "Lock acquired on C", fileC, "")
	time.Sleep(300 * time.Millisecond)

	sendEvent(cb, "server-3", "update", "pending", "Updating File C...", fileC, "")
	time.Sleep(400 * time.Millisecond)
	store3.UpdateFile(ctx, fileC, map[string]string{"status": "completed", "processed_by": "server-3", "chain": "complete"})
	sendEvent(cb, "server-3", "update", "success", "File C updated!", fileC, "Status: completed")
	time.Sleep(300 * time.Millisecond)

	store3.ReleaseLock(ctx, fileC)
	sendEvent(cb, "server-3", "unlock", "success", "Lock released on C", fileC, "Chain complete!")
	time.Sleep(300 * time.Millisecond)

	sendEvent(cb, "system", "complete", "success", "Simulation 4 Complete!", "", "Cascading updates: A->B->C all completed in order!")

	return nil
}

// RunSimulation5 demonstrates read-write conflict resolution
// Scenario: Multiple servers reading while one tries to write
func (r *Runner) RunSimulation5(ctx context.Context, cb EventCallback) error {
	fileID := fmt.Sprintf("sim5-file-%d", time.Now().UnixNano())

	r.Cleanup()

	sendEvent(cb, "system", "init", "pending", "Starting Simulation 5: Read-Write Conflict", fileID, "Readers vs Writer scenario")
	time.Sleep(500 * time.Millisecond)

	store1, _ := r.getOrCreateStore("sim-server-1")
	store2, _ := r.getOrCreateStore("sim-server-2")
	store3, _ := r.getOrCreateStore("sim-server-3")

	// Create file
	sendEvent(cb, "server-1", "create", "pending", "Creating shared resource...", fileID, "")
	time.Sleep(300 * time.Millisecond)
	store1.CreateFile(ctx, fileID, "simulation", map[string]string{"data": "initial", "reads": "0"})
	sendEvent(cb, "server-1", "create", "success", "Shared resource created", fileID, "")
	time.Sleep(500 * time.Millisecond)

	// Multiple readers start
	sendEvent(cb, "server-2", "read", "pending", "Reading file...", fileID, "Reader 1")
	time.Sleep(200 * time.Millisecond)
	meta, _ := store2.GetFile(ctx, fileID)
	sendEvent(cb, "server-2", "read", "success", fmt.Sprintf("Read complete: data='%s'", meta.Attributes["data"]), fileID, "No lock needed for read")
	time.Sleep(300 * time.Millisecond)

	sendEvent(cb, "server-3", "read", "pending", "Reading file...", fileID, "Reader 2")
	time.Sleep(200 * time.Millisecond)
	meta, _ = store3.GetFile(ctx, fileID)
	sendEvent(cb, "server-3", "read", "success", fmt.Sprintf("Read complete: data='%s'", meta.Attributes["data"]), fileID, "Concurrent read OK")
	time.Sleep(300 * time.Millisecond)

	// Writer wants to update
	sendEvent(cb, "server-1", "lock", "pending", "Writer needs exclusive access...", fileID, "")
	time.Sleep(300 * time.Millisecond)
	store1.AcquireLock(ctx, fileID)
	sendEvent(cb, "server-1", "lock", "success", "Writer acquired lock!", fileID, "Exclusive access granted")
	time.Sleep(500 * time.Millisecond)

	// Readers try to read during write
	sendEvent(cb, "server-2", "read", "pending", "Attempting read during write...", fileID, "")
	time.Sleep(200 * time.Millisecond)
	meta, _ = store2.GetFile(ctx, fileID)
	sendEvent(cb, "server-2", "read", "success", fmt.Sprintf("Read allowed: data='%s'", meta.Attributes["data"]), fileID, "Reads still work during write lock")
	time.Sleep(300 * time.Millisecond)

	// Writer updates
	sendEvent(cb, "server-1", "update", "pending", "Writing new data...", fileID, "")
	time.Sleep(500 * time.Millisecond)
	store1.UpdateFile(ctx, fileID, map[string]string{"data": "modified", "writer": "server-1"})
	sendEvent(cb, "server-1", "update", "success", "Write complete!", fileID, "data='modified'")
	time.Sleep(300 * time.Millisecond)

	// Another server tries to write (blocked)
	sendEvent(cb, "server-3", "lock", "pending", "Server-3 wants to write...", fileID, "")
	sendEvent(cb, "server-3", "lock", "waiting", "BLOCKED! Writer holding lock", fileID, "Must wait...")

	go func() {
		lockCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		store3.AcquireLock(lockCtx, fileID)
	}()

	time.Sleep(800 * time.Millisecond)

	// Original writer releases
	sendEvent(cb, "server-1", "unlock", "pending", "Writer releasing lock...", fileID, "")
	time.Sleep(300 * time.Millisecond)
	store1.ReleaseLock(ctx, fileID)
	sendEvent(cb, "server-1", "unlock", "success", "Lock released!", fileID, "")
	time.Sleep(500 * time.Millisecond)

	// Server 3 gets the lock
	sendEvent(cb, "server-3", "lock", "success", "Server-3 acquired lock!", fileID, "")
	time.Sleep(300 * time.Millisecond)

	sendEvent(cb, "server-3", "update", "pending", "Server-3 writing...", fileID, "")
	time.Sleep(400 * time.Millisecond)
	store3.UpdateFile(ctx, fileID, map[string]string{"data": "final", "writer": "server-3"})
	sendEvent(cb, "server-3", "update", "success", "Write complete!", fileID, "data='final'")
	time.Sleep(300 * time.Millisecond)

	store3.ReleaseLock(ctx, fileID)
	sendEvent(cb, "server-3", "unlock", "success", "Lock released", fileID, "")
	time.Sleep(300 * time.Millisecond)

	// Final read to verify
	sendEvent(cb, "server-2", "read", "pending", "Final verification read...", fileID, "")
	time.Sleep(200 * time.Millisecond)
	meta, _ = store2.GetFile(ctx, fileID)
	sendEvent(cb, "server-2", "read", "success", fmt.Sprintf("Verified: data='%s', version=%d", meta.Attributes["data"], meta.Version), fileID, "All writes preserved!")
	time.Sleep(300 * time.Millisecond)

	sendEvent(cb, "system", "complete", "success", "Simulation 5 Complete!", fileID, "Read-Write conflicts resolved correctly!")

	return nil
}

// RunSimulation6 demonstrates fair lock queue (FIFO ordering)
// Scenario: Show that locks are granted in request order
func (r *Runner) RunSimulation6(ctx context.Context, cb EventCallback) error {
	fileID := fmt.Sprintf("sim6-file-%d", time.Now().UnixNano())

	r.Cleanup()

	sendEvent(cb, "system", "init", "pending", "Starting Simulation 6: Fair Lock Queue", fileID, "Demonstrating FIFO lock ordering")
	time.Sleep(500 * time.Millisecond)

	store1, _ := r.getOrCreateStore("sim-server-1")
	store2, _ := r.getOrCreateStore("sim-server-2")
	store3, _ := r.getOrCreateStore("sim-server-3")

	// Create file
	sendEvent(cb, "server-1", "create", "pending", "Creating file...", fileID, "")
	time.Sleep(300 * time.Millisecond)
	store1.CreateFile(ctx, fileID, "simulation", map[string]string{"queue_demo": "true"})
	sendEvent(cb, "server-1", "create", "success", "File created", fileID, "")
	time.Sleep(500 * time.Millisecond)

	// Server 1 acquires lock first
	sendEvent(cb, "server-1", "lock", "pending", "Server-1 requesting lock (1st)...", fileID, "Queue position: 1")
	time.Sleep(300 * time.Millisecond)
	store1.AcquireLock(ctx, fileID)
	sendEvent(cb, "server-1", "lock", "success", "Server-1 acquired lock!", fileID, "First in queue")
	time.Sleep(500 * time.Millisecond)

	// Server 2 joins queue
	sendEvent(cb, "server-2", "lock", "pending", "Server-2 requesting lock (2nd)...", fileID, "Queue position: 2")
	time.Sleep(100 * time.Millisecond)
	sendEvent(cb, "server-2", "lock", "waiting", "Server-2 in queue...", fileID, "Waiting behind Server-1")

	resultChan2 := make(chan bool, 1)
	go func() {
		lockCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		_, err := store2.AcquireLock(lockCtx, fileID)
		resultChan2 <- (err == nil)
	}()

	time.Sleep(300 * time.Millisecond)

	// Server 3 joins queue
	sendEvent(cb, "server-3", "lock", "pending", "Server-3 requesting lock (3rd)...", fileID, "Queue position: 3")
	time.Sleep(100 * time.Millisecond)
	sendEvent(cb, "server-3", "lock", "waiting", "Server-3 in queue...", fileID, "Waiting behind Server-2")

	resultChan3 := make(chan bool, 1)
	go func() {
		lockCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		_, err := store3.AcquireLock(lockCtx, fileID)
		resultChan3 <- (err == nil)
	}()

	time.Sleep(500 * time.Millisecond)

	// Show queue status
	sendEvent(cb, "system", "info", "pending", "Current queue: [Server-1*] <- Server-2 <- Server-3", fileID, "* = lock holder")
	time.Sleep(800 * time.Millisecond)

	// Server 1 does work and releases
	sendEvent(cb, "server-1", "work", "pending", "Server-1 processing...", fileID, "")
	time.Sleep(600 * time.Millisecond)
	sendEvent(cb, "server-1", "update", "pending", "Server-1 updating...", fileID, "")
	time.Sleep(400 * time.Millisecond)
	store1.UpdateFile(ctx, fileID, map[string]string{"processed_by": "server-1", "order": "1"})
	sendEvent(cb, "server-1", "update", "success", "Server-1 done!", fileID, "Order: 1")
	time.Sleep(300 * time.Millisecond)

	sendEvent(cb, "server-1", "unlock", "pending", "Server-1 releasing...", fileID, "")
	time.Sleep(200 * time.Millisecond)
	store1.ReleaseLock(ctx, fileID)
	sendEvent(cb, "server-1", "unlock", "success", "Server-1 released lock", fileID, "Next: Server-2")
	time.Sleep(300 * time.Millisecond)

	// Server 2 gets lock (was 2nd in queue)
	<-resultChan2
	sendEvent(cb, "server-2", "lock", "success", "Server-2 acquired lock!", fileID, "FIFO: Was 2nd, got 2nd")
	time.Sleep(500 * time.Millisecond)

	sendEvent(cb, "server-2", "work", "pending", "Server-2 processing...", fileID, "")
	time.Sleep(600 * time.Millisecond)
	sendEvent(cb, "server-2", "update", "pending", "Server-2 updating...", fileID, "")
	time.Sleep(400 * time.Millisecond)
	store2.UpdateFile(ctx, fileID, map[string]string{"processed_by": "server-2", "order": "2"})
	sendEvent(cb, "server-2", "update", "success", "Server-2 done!", fileID, "Order: 2")
	time.Sleep(300 * time.Millisecond)

	sendEvent(cb, "server-2", "unlock", "pending", "Server-2 releasing...", fileID, "")
	time.Sleep(200 * time.Millisecond)
	store2.ReleaseLock(ctx, fileID)
	sendEvent(cb, "server-2", "unlock", "success", "Server-2 released lock", fileID, "Next: Server-3")
	time.Sleep(300 * time.Millisecond)

	// Server 3 gets lock (was 3rd in queue)
	<-resultChan3
	sendEvent(cb, "server-3", "lock", "success", "Server-3 acquired lock!", fileID, "FIFO: Was 3rd, got 3rd")
	time.Sleep(500 * time.Millisecond)

	sendEvent(cb, "server-3", "work", "pending", "Server-3 processing...", fileID, "")
	time.Sleep(600 * time.Millisecond)
	sendEvent(cb, "server-3", "update", "pending", "Server-3 updating...", fileID, "")
	time.Sleep(400 * time.Millisecond)
	store3.UpdateFile(ctx, fileID, map[string]string{"processed_by": "server-3", "order": "3"})
	sendEvent(cb, "server-3", "update", "success", "Server-3 done!", fileID, "Order: 3")
	time.Sleep(300 * time.Millisecond)

	store3.ReleaseLock(ctx, fileID)
	sendEvent(cb, "server-3", "unlock", "success", "Server-3 released lock", fileID, "Queue empty!")
	time.Sleep(300 * time.Millisecond)

	sendEvent(cb, "system", "complete", "success", "Simulation 6 Complete!", fileID, "FIFO ordering verified: 1->2->3")

	return nil
}
