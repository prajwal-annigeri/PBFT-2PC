package main

import (
	"2pcbyz-prajwal/gateway/clientrpc"
	"2pcbyz-prajwal/pbft"
	"context"
	"crypto/rsa"
	"slices"
	"strconv"

	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	inputFile = flag.String("input", "../transactions.csv", "Input file path")
	logs      = flag.Bool("logs", false, "Logging")
	port      = ":8080"
)

type Gateway struct {
	clientrpc.UnimplementedClientServer
	NodeMap                   map[string]map[string]string
	ConnMap                   map[string]*grpc.ClientConn
	gRPCClientMap             map[string]pbft.PbftClient
	LastExecSet               sync.Map
	LatencyMap                sync.Map
	ClusterNodeMap            map[string][]string
	SetTransactionsMap        map[int][]*pbft.Transaction
	LiveServersMap            map[int][]string
	MaxSetNum                 int
	MaxExecutedSet            int
	TransactionCountMap       map[int]int
	SeqTransactionMap         sync.Map
	TimestampSeqMap           sync.Map
	highestSeq                int64
	ClusterLiveServersMap     map[int]map[string][]string
	currentSet                int
	ClusterLiveServersMapLock sync.RWMutex
	transactionLatency        sync.Map
	startTimeMap              sync.Map
	ResultsChan               chan ReplyResult
	PublicKeyMap              sync.Map
	PrivateKey                *rsa.PrivateKey
	ViewMap                   sync.Map
	ByzantineServersMap       map[int][]string
	ReplyMap                  map[int64][]ReplyResult
	ReplyMapLock              sync.RWMutex
	ResCountMap               map[int64]map[bool]int
	ResCountMapLock           sync.RWMutex
}

func main() {
	flag.Parse()
	if !*logs {
		log.SetOutput(io.Discard)
	}
	gateway := &Gateway{
		highestSeq:  0,
		currentSet:  0,
		ReplyMap:    make(map[int64][]ReplyResult),
		ResultsChan: make(chan ReplyResult),
		ResCountMap: make(map[int64]map[bool]int),
	}

	if err := gateway.readJson(); err != nil {
		log.Fatalf("Failed to read JSON file: %s\n", err)
	}

	err := gateway.readCsv(*inputFile)
	if err != nil {
		log.Fatalf("Failed to read input file: %s\n", err)
	}

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen on port %v: %v", port, err)
	}
	log.Printf("Listening on port: %v\n", port)

	grpcSrv := grpc.NewServer()

	clientrpc.RegisterClientServer(grpcSrv, gateway)
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	gateway.establishConns()
	gateway.initKeys()
	go gateway.readReplies()

	set := 1
	for {
		printMenu()
		var input string
		fmt.Scan(&input)
		switch input {
		case "1":
			if set <= gateway.MaxSetNum {
				fmt.Printf("Executing Set %d\n", set)
				gateway.currentSet = set
				gateway.LatencyMap.Store(set, time.Duration(0))
				log.Printf("Live servers this round: %v\n", gateway.LiveServersMap[set])
				isLiveSet := gateway.setLiveServers(gateway.LiveServersMap[set])
				if isLiveSet {
					log.Println("Servers have risen")
				}

				isByzantineSet := gateway.setByzantineServers(gateway.ByzantineServersMap[set])
				if isByzantineSet {
					log.Println("Byzantine servers set")
				}

				gateway.executeTransactions(gateway.SetTransactionsMap[set], set)
				gateway.MaxExecutedSet = set
				fmt.Printf("Set %d done\n", set)
				set++
			} else {
				fmt.Println("No more sets")
			}
		case "2":
			var clientId string
			fmt.Print("Enter Client ID: ")
			fmt.Scan(&clientId)
			cluster := gateway.getClusterOfClient(clientId)
			if err != nil {
				fmt.Printf("Error fetching cluster of %s: %v", clientId, err)
			}

			for _, node := range gateway.ClusterNodeMap[cluster] {
				balanceResp, err := gateway.gRPCClientMap[node].GetBalance(context.Background(), &pbft.GetBalanceRequest{Client: clientId})
				if err != nil {
					fmt.Printf("Error getting balance of %s from %s: %v\n", clientId, node, err)
				} else {
					fmt.Printf("%-3s: %d\n", node, balanceResp.Balance)
				}
			}
		case "3":
			var nodes []int64
			for node := range gateway.gRPCClientMap {
				nodeNum, err := strconv.ParseInt(node[1:], 10, 64)
				if err != nil {
					log.Printf("Could not convert %s to int: %v\n", node, err)
				}
				nodes = append(nodes, nodeNum)
			}

			slices.Sort(nodes)
			for _, nodeNum := range nodes {
				node := fmt.Sprintf("S%d", nodeNum)
				datastore, err := gateway.gRPCClientMap[node].GetDatastore(context.Background(), &emptypb.Empty{})
				if err != nil {
					fmt.Printf("Error getting datastore from %s: %v\n", node, err)
				} else {
					fmt.Printf("%s\n-------\n", node)
					for _, data := range datastore.Data {
						fmt.Printf("%d. (%s, %s, %d) %s\n", data.Seq, data.Transaction.From, data.Transaction.To, data.Transaction.Amount, data.Status)
					}
					fmt.Println("")
				}
			}
		case "4":
			gateway.printOverallLatency()
		case "5":
			gateway.printSetWiseLatency()
		case "6":
			gateway.printAllLatencies()
		case "8":
			gateway.printState()
		case "0":
			fmt.Printf("Gracefully shutting down\n")
			gateway.shutdown()
			return
		default:
			fmt.Println("Invalid option")
		}
	}
}
