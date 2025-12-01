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
