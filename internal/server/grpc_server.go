package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/cmpe541/file-metadata-manager/internal/metadata"
	"github.com/cmpe541/file-metadata-manager/internal/simulation"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Server handles both gRPC and HTTP requests
type Server struct {
	store     *metadata.Store
	nodeID    string
	grpcPort  int
	httpPort  int
	client    *clientv3.Client
	simRunner *simulation.Runner
}

// NewServer creates a new server instance
func NewServer(client *clientv3.Client, nodeID string, grpcPort, httpPort int) (*Server, error) {
	store, err := metadata.NewStore(client, nodeID)
	if err != nil {
		return nil, err
	}

	return &Server{
		store:     store,
		nodeID:    nodeID,
		grpcPort:  grpcPort,
		httpPort:  httpPort,
		client:    client,
		simRunner: simulation.NewRunner(client),
	}, nil
}

// Start starts both gRPC and HTTP servers
func (s *Server) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	errChan := make(chan error, 2)

	// Start HTTP server
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.startHTTPServer(ctx); err != nil {
			errChan <- fmt.Errorf("HTTP server error: %w", err)
		}
	}()

	log.Printf("[%s] Servers started - HTTP: %d", s.nodeID, s.httpPort)

	// Wait for context cancellation
	<-ctx.Done()
	return nil
}

// startHTTPServer starts the HTTP REST API server
func (s *Server) startHTTPServer(ctx context.Context) error {
	mux := http.NewServeMux()

	// CORS middleware wrapper
	corsHandler := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}

			h(w, r)
		}
	}

	// API routes
	mux.HandleFunc("/api/files", corsHandler(s.handleFiles))
	mux.HandleFunc("/api/files/", corsHandler(s.handleFile))
	mux.HandleFunc("/api/locks/", corsHandler(s.handleLocks))
	mux.HandleFunc("/api/locks/status/", corsHandler(s.handleLockStatus))
	mux.HandleFunc("/api/node", corsHandler(s.handleNodeInfo))
	mux.HandleFunc("/health", corsHandler(s.handleHealth))

	// Simulation routes (SSE endpoints)
	mux.HandleFunc("/api/simulation/1", s.handleSimulation1)
	mux.HandleFunc("/api/simulation/2", s.handleSimulation2)
	mux.HandleFunc("/api/simulation/3", s.handleSimulation3)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.httpPort),
		Handler: mux,
	}

	// Graceful shutdown
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()

	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return err
	}

	log.Printf("[%s] HTTP server listening on port %d", s.nodeID, s.httpPort)
	return server.Serve(listener)
}

// HTTP Handlers

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy", "node": s.nodeID})
}

func (s *Server) handleNodeInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"node_id":   s.nodeID,
		"status":    "running",
		"grpc_port": s.grpcPort,
		"http_port": s.httpPort,
	})
}

func (s *Server) handleFiles(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	ctx := r.Context()

	switch r.Method {
	case http.MethodGet:
		// List files
		prefix := r.URL.Query().Get("prefix")
		files, err := s.store.ListFiles(ctx, prefix)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"files":   files,
		})

	case http.MethodPost:
		// Create file
		var req struct {
			FileID     string            `json:"file_id"`
			Owner      string            `json:"owner"`
			Attributes map[string]string `json:"attributes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		metadata, err := s.store.CreateFile(ctx, req.FileID, req.Owner, req.Attributes)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		log.Printf("[%s] Created file: %s", s.nodeID, req.FileID)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":  true,
			"metadata": metadata,
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleFile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	ctx := r.Context()

	// Extract file ID from path
	fileID := r.URL.Path[len("/api/files/"):]
	if fileID == "" {
		http.Error(w, "File ID required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		// Get file
		metadata, err := s.store.GetFile(ctx, fileID)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": err.Error(),
			})
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":  true,
			"metadata": metadata,
		})

	case http.MethodPut:
		// Update file (requires lock)
		if !s.store.IsLockHeld(fileID) {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "Lock must be acquired before updating",
			})
			return
		}

		var req struct {
			Attributes map[string]string `json:"attributes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		metadata, err := s.store.UpdateFile(ctx, fileID, req.Attributes)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		log.Printf("[%s] Updated file: %s (version: %d)", s.nodeID, fileID, metadata.Version)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":  true,
			"metadata": metadata,
		})

	case http.MethodDelete:
		// Delete file
		if !s.store.IsLockHeld(fileID) {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "Lock must be acquired before deleting",
			})
			return
		}

		if err := s.store.DeleteFile(ctx, fileID); err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		log.Printf("[%s] Deleted file: %s", s.nodeID, fileID)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "File deleted",
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleLocks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	ctx := r.Context()

	// Extract file ID from path
	fileID := r.URL.Path[len("/api/locks/"):]
	if fileID == "" {
		http.Error(w, "File ID required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPost:
		// Acquire lock
		lockInfo, err := s.store.AcquireLock(ctx, fileID)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		log.Printf("[%s] Acquired lock for: %s", s.nodeID, fileID)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":   true,
			"lock_info": lockInfo,
		})

	case http.MethodDelete:
		// Release lock
		if err := s.store.ReleaseLock(ctx, fileID); err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": err.Error(),
			})
			return
		}

		log.Printf("[%s] Released lock for: %s", s.nodeID, fileID)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Lock released",
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleLockStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	ctx := r.Context()

	// Extract file ID from path
	fileID := r.URL.Path[len("/api/locks/status/"):]
	if fileID == "" {
		http.Error(w, "File ID required", http.StatusBadRequest)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	lockInfo, err := s.store.GetLockStatus(ctx, fileID)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":   true,
		"lock_info": lockInfo,
	})
}

// Close closes the server and releases resources
func (s *Server) Close() error {
	if s.simRunner != nil {
		s.simRunner.Cleanup()
	}
	return s.store.Close()
}

// SSE helper to set up Server-Sent Events response
func setupSSE(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

// handleSimulation1 runs simulation 1 with SSE events
func (s *Server) handleSimulation1(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.WriteHeader(http.StatusOK)
		return
	}

	setupSSE(w)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()

	callback := func(event simulation.Event) {
		data := simulation.EventToJSON(event)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	log.Printf("[%s] Starting Simulation 1", s.nodeID)
	err := s.simRunner.RunSimulation1(ctx, callback)
	if err != nil {
		log.Printf("[%s] Simulation 1 error: %v", s.nodeID, err)
	}

	// Send end event
	fmt.Fprintf(w, "data: {\"action\":\"end\"}\n\n")
	flusher.Flush()
}

// handleSimulation2 runs simulation 2 with SSE events
func (s *Server) handleSimulation2(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.WriteHeader(http.StatusOK)
		return
	}

	setupSSE(w)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()

	callback := func(event simulation.Event) {
		data := simulation.EventToJSON(event)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	log.Printf("[%s] Starting Simulation 2", s.nodeID)
	err := s.simRunner.RunSimulation2(ctx, callback)
	if err != nil {
		log.Printf("[%s] Simulation 2 error: %v", s.nodeID, err)
	}

	fmt.Fprintf(w, "data: {\"action\":\"end\"}\n\n")
	flusher.Flush()
}

// handleSimulation3 runs simulation 3 with SSE events
func (s *Server) handleSimulation3(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.WriteHeader(http.StatusOK)
		return
	}

	setupSSE(w)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()

	callback := func(event simulation.Event) {
		data := simulation.EventToJSON(event)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	log.Printf("[%s] Starting Simulation 3", s.nodeID)
	err := s.simRunner.RunSimulation3(ctx, callback)
	if err != nil {
		log.Printf("[%s] Simulation 3 error: %v", s.nodeID, err)
	}

	fmt.Fprintf(w, "data: {\"action\":\"end\"}\n\n")
	flusher.Flush()
}
