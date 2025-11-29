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
	etcdEndpoints = flag.String("etcd-endpoints", "etcd:2379", "Comma-separated etcd endpoints")
)

func main() {
	flag.Parse()

	// Override with environment variables if set
	if envNodeID := os.Getenv("NODE_ID"); envNodeID != "" {
		*nodeID = envNodeID
	}
	if envEtcd := os.Getenv("ETCD_ENDPOINTS"); envEtcd != "" {
		*etcdEndpoints = envEtcd
	}
	if envGRPCPort := os.Getenv("GRPC_PORT"); envGRPCPort != "" {
		if port, err := strconv.Atoi(envGRPCPort); err == nil {
			*grpcPort = port
		}
	}
	if envHTTPPort := os.Getenv("HTTP_PORT"); envHTTPPort != "" {
		if port, err := strconv.Atoi(envHTTPPort); err == nil {
			*httpPort = port
		}
	}

	log.Printf("[%s] Starting file metadata server", *nodeID)
	log.Printf("[%s] gRPC port: %d, HTTP port: %d", *nodeID, *grpcPort, *httpPort)
	log.Printf("[%s] Connecting to etcd at %s", *nodeID, *etcdEndpoints)

	// Connect to etcd
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{*etcdEndpoints},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("[%s] Failed to connect to etcd: %v", *nodeID, err)
	}
	defer client.Close()

	log.Printf("[%s] Successfully connected to etcd", *nodeID)

	// Create server
	srv, err := server.NewServer(client, *nodeID, *grpcPort, *httpPort)
	if err != nil {
		log.Fatalf("[%s] Failed to create server: %v", *nodeID, err)
	}
	defer srv.Close()

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("[%s] Received signal: %v, shutting down...", *nodeID, sig)
		cancel()
	}()

	// Start server
	if err := srv.Start(ctx); err != nil {
		log.Fatalf("[%s] Server error: %v", *nodeID, err)
	}

	log.Printf("[%s] Server stopped", *nodeID)
}
