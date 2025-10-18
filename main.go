package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"2pcbyz-prajwal/node"
	"2pcbyz-prajwal/pbft"

	"google.golang.org/grpc"
)

var (
	nodeId = flag.String("id", "", "Node ID")
	logs   = flag.Bool("logs", false, "Logging")
	clusterId  = flag.String("cluster-id", "", "Cluster ID")
)

func main() {
	flag.Parse()

	if !*logs {
		log.SetOutput(io.Discard)
	}

	if *nodeId == "" {
		log.Fatalf("Node ID is required")
	}

	if *clusterId == "" {
		log.Fatalf("Cluster ID is required")
	}

	mapJson, err := os.Open("config.json")
	if err != nil {
		log.Fatalf("Error with reading config JSON: %s\n", err)
	}
	defer mapJson.Close()

	byteValue, err := io.ReadAll(mapJson)
	if err != nil {
		log.Fatalf("Failed to read file: %s", err)
	}

	var clusters map[string]node.ClusterStruct
	err = json.Unmarshal(byteValue, &clusters)
	if err != nil {
		log.Fatalf("Failed to unmarshal JSON: %s", err)
	}

	// nodes := clusters[*clusterId].Nodes

	node, err := node.NewNode(*clusterId, *nodeId, clusters, clusters[*clusterId].Start, clusters[*clusterId].End)
	if err != nil {
		log.Fatalf("Error creating node: %v", err)
	}

	lis, err := net.Listen("tcp", node.Port)
	if err != nil {
		log.Fatalf("Failed to listen on port %v: %v", node.Port, err)
	}
	log.Printf("Listening on port: %v\n", node.Port)

	grpcSrv := grpc.NewServer()

	pbft.RegisterPbftServer(grpcSrv, node)
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	node.Start()
}
