package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

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

// Store handles metadata storage and distributed locking
type Store struct {
	client  *clientv3.Client
	session *concurrency.Session
	nodeID  string
	locks   map[string]*concurrency.Mutex
	locksMu sync.RWMutex
}

// NewStore creates a new metadata store
func NewStore(client *clientv3.Client, nodeID string) (*Store, error) {
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

// Close closes the store and releases resources
func (s *Store) Close() error {
	return s.session.Close()
}

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
	resp, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, string(data))).
		Commit()

	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	if !resp.Succeeded {
		return nil, fmt.Errorf("file already exists: %s", fileID)
	}

	return metadata, nil
}

// GetFile retrieves file metadata
func (s *Store) GetFile(ctx context.Context, fileID string) (*FileMetadata, error) {
	resp, err := s.client.Get(ctx, metadataKey(fileID))
	if err != nil {
		return nil, fmt.Errorf("failed to get file: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("file not found: %s", fileID)
	}

	var metadata FileMetadata
	if err := json.Unmarshal(resp.Kvs[0].Value, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &metadata, nil
}

// UpdateFile updates file metadata (requires lock)
func (s *Store) UpdateFile(ctx context.Context, fileID string, attributes map[string]string) (*FileMetadata, error) {
	// Get existing metadata
	metadata, err := s.GetFile(ctx, fileID)
	if err != nil {
		return nil, err
	}

	// Update fields
	metadata.Version++
	metadata.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	metadata.LastModifiedBy = s.nodeID

	// Merge attributes
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

// DeleteFile deletes file metadata
func (s *Store) DeleteFile(ctx context.Context, fileID string) error {
	_, err := s.client.Delete(ctx, metadataKey(fileID))
	if err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}
	return nil
}

// ListFiles lists all files with optional prefix filter
func (s *Store) ListFiles(ctx context.Context, prefix string) ([]*FileMetadata, error) {
	key := "/metadata/files/"
	if prefix != "" {
		key = metadataKey(prefix)
	}

	resp, err := s.client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	files := make([]*FileMetadata, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var metadata FileMetadata
		if err := json.Unmarshal(kv.Value, &metadata); err != nil {
			continue
		}
		files = append(files, &metadata)
	}

	return files, nil
}

// AcquireLock acquires a distributed lock for a file
func (s *Store) AcquireLock(ctx context.Context, fileID string) (*LockInfo, error) {
	s.locksMu.Lock()
	defer s.locksMu.Unlock()

	// Check if we already hold the lock
	if _, exists := s.locks[fileID]; exists {
		return nil, fmt.Errorf("lock already held by this node")
	}

	mutex := concurrency.NewMutex(s.session, lockKey(fileID))

	// Try to acquire with timeout
	lockCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := mutex.Lock(lockCtx); err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	s.locks[fileID] = mutex

	// Store lock info
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

// ReleaseLock releases a distributed lock for a file
func (s *Store) ReleaseLock(ctx context.Context, fileID string) error {
	s.locksMu.Lock()
	defer s.locksMu.Unlock()

	mutex, exists := s.locks[fileID]
	if !exists {
		return fmt.Errorf("lock not held by this node")
	}

	if err := mutex.Unlock(ctx); err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	delete(s.locks, fileID)

	// Update lock info
	lockInfo := &LockInfo{
		FileID:   fileID,
		Holder:   "",
		IsLocked: false,
	}
	data, _ := json.Marshal(lockInfo)
	s.client.Put(ctx, lockInfoKey(fileID), string(data))

	return nil
}

// GetLockStatus returns the current lock status for a file
func (s *Store) GetLockStatus(ctx context.Context, fileID string) (*LockInfo, error) {
	resp, err := s.client.Get(ctx, lockInfoKey(fileID))
	if err != nil {
		return nil, fmt.Errorf("failed to get lock status: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return &LockInfo{
			FileID:   fileID,
			IsLocked: false,
		}, nil
	}

	var lockInfo LockInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &lockInfo); err != nil {
		return nil, fmt.Errorf("failed to unmarshal lock info: %w", err)
	}

	return &lockInfo, nil
}

// IsLockHeld checks if this node holds the lock for a file
func (s *Store) IsLockHeld(fileID string) bool {
	s.locksMu.RLock()
	defer s.locksMu.RUnlock()
	_, exists := s.locks[fileID]
	return exists
}

// GetNodeID returns the node ID
func (s *Store) GetNodeID() string {
	return s.nodeID
}
