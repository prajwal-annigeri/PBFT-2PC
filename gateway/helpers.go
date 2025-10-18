package main

import (
	"2pcbyz-prajwal/cryptox"
	"2pcbyz-prajwal/pbft"
	"crypto/rsa"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeStruct struct {
	Id   string `json:"id"`
	Port string `json:"port"`
}

type ClusterStruct struct {
	Nodes []NodeStruct `json:"nodes"`
	Start string       `json:"start"`
	End   string       `json:"end"`
}

func (gateway *Gateway) establishConns() {
	connMap := make(map[string]*grpc.ClientConn)
	clientMap := make(map[string]pbft.PbftClient)
	for cluster, clusterNodes := range gateway.NodeMap {
		gateway.ViewMap.Store(cluster, 1)
		for nodeId, port := range clusterNodes {
			log.Printf("Establishing connection from gateway to %s.%s\n", cluster, nodeId)
			var conn *grpc.ClientConn
			conn, err := grpc.NewClient(port, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf("Failed connecting to %s: %s\n", nodeId, err)
			}

			connMap[nodeId] = conn
			clientMap[nodeId] = pbft.NewPbftClient(conn)
		}
	}

	gateway.ConnMap = connMap
	gateway.gRPCClientMap = clientMap
}

func (gateway *Gateway) shutdown() {
	time.Sleep(2 * time.Second)
	for nodeId, conn := range gateway.ConnMap {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection from Gateway to node %s\n", nodeId)
		}
	}
}

func (gateway *Gateway) readJson() error {
	mapJson, err := os.Open("../config.json")
	if err != nil {
		log.Fatalf("Error with reading config JSON: %s\n", err)
	}
	defer mapJson.Close()

	byteValue, err := io.ReadAll(mapJson)
	if err != nil {
		log.Fatalf("Failed to read file: %s", err)
	}

	var clusters map[string]ClusterStruct
	err = json.Unmarshal(byteValue, &clusters)
	if err != nil {
		log.Fatalf("Failed to unmarshal JSON: %s", err)
	}

	nodeMap := make(map[string]map[string]string)
	clusterNodeMap := make(map[string][]string)
	for clusterId, clusterData := range clusters {
		nodeMap[clusterId] = make(map[string]string)
		for _, node := range clusterData.Nodes {
			nodeMap[clusterId][node.Id] = node.Port
			clusterNodeMap[clusterId] = append(clusterNodeMap[clusterId], node.Id)
		}
	}

	gateway.NodeMap = nodeMap
	gateway.ClusterNodeMap = clusterNodeMap
	return nil
}

func printMenu() {
	fmt.Println("\nMenu:")
	fmt.Println("1. Execute next set")
	fmt.Println("2. Fetch Balance of client on its cluster")
	fmt.Println("3. Fetch Datastore on all servers")
	// fmt.Println("4. Performance")
	// fmt.Println("5. Performance per set")
	// fmt.Println("6. Performance of all transactions")
	// fmt.Println("7. Get cluster of client ID")
	// fmt.Println("8. Print Gateway State")
	// fmt.Println("9. Reshard")
	// fmt.Println("0. Exit")
	fmt.Print("Choose an option: ")
}

func (gateway *Gateway) printState() {
	fmt.Printf("Node Map: %v\n", gateway.NodeMap)
	for i := 1; i <= gateway.MaxSetNum; i++ {
		fmt.Printf("Set %d\n", i)
		fmt.Printf("Live servers: %v\n", gateway.LiveServersMap[i])
		fmt.Printf("Transaction Count: %d\n", gateway.TransactionCountMap[i])
		fmt.Printf("ClusterWise Live Servers: %v\n", gateway.ClusterLiveServersMap[i])
		fmt.Printf("Byzantine Servers: %v\n", gateway.ByzantineServersMap)
	}
}

func (gateway *Gateway) readCsv(filepath string) error {
	maxSetNum := 0
	transactionCountMap := make(map[int]int)
	transactionsMap := make(map[int][]*pbft.Transaction)
	liveServersMap := make(map[int][]string)
	clusterLiveServersMap := make(map[int]map[string][]string)
	byzantineServersMap := make(map[int][]string)
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1

	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	for _, row := range records {
		var liveServerList []string
		var contactServerList []string
		set, err := strconv.Atoi(strings.TrimSpace(row[0]))
		if err != nil {
			// log.Printf("Error reading set: %v\n", err)
		} else {
			maxSetNum = max(int(set), maxSetNum)
			clusterLiveServersMap[set] = make(map[string][]string)
			liveServerList = strings.Split(strings.Trim(row[2], "[]"), ",")
			byzantineServerList := strings.Split(strings.Trim(row[4], "[]"), ",")
			for i := range liveServerList {
				liveServerList[i] = strings.TrimSpace(liveServerList[i])
			}
			liveServersMap[set] = liveServerList

			for _, liveServer := range liveServerList {
				for cluster, nodes := range gateway.ClusterNodeMap {
					if slices.Contains(nodes, liveServer) {
						clusterLiveServersMap[set][cluster] = append(clusterLiveServersMap[set][cluster], liveServer)
						break
					}
				}
			}

			contactServerList = strings.Split(strings.Trim(row[3], "[]"), ",")
			for i := range contactServerList {
				contactServerList[i] = strings.TrimSpace(contactServerList[i])
			}

			for i := range byzantineServerList {
				byzantineServerList[i] = strings.TrimSpace(byzantineServerList[i])
			}
			byzantineServersMap[set] = byzantineServerList
		}

		transactionCountMap[maxSetNum] += 1

		transactionSlice := strings.Split(strings.Trim(row[1], "()"), ",")

		from := strings.TrimSpace(transactionSlice[0])
		to := strings.TrimSpace(transactionSlice[1])
		amount, err := strconv.Atoi(strings.TrimSpace(transactionSlice[2]))
		if err != nil {
			log.Printf("Error converting amount to int: %s\n", err)
		}

		transaction := &pbft.Transaction{
			From:   from,
			To:     to,
			Amount: int64(amount),
		}

		transactionsMap[maxSetNum] = append(transactionsMap[maxSetNum], transaction)
	}
	gateway.TransactionCountMap = transactionCountMap
	gateway.SetTransactionsMap = transactionsMap
	gateway.LiveServersMap = liveServersMap
	gateway.MaxSetNum = maxSetNum
	gateway.ClusterLiveServersMap = clusterLiveServersMap
	gateway.ByzantineServersMap = byzantineServersMap
	return nil
}

type resultStruct struct {
	transaction *pbft.Transaction
	latency     time.Duration
	success     bool
}

func (gateway *Gateway) storeLatency(clientSeq int64, endTime time.Time, result bool) {
	transaction, ok := gateway.SeqTransactionMap.Load(clientSeq)
	if !ok {
		log.Printf("storeLatency(): No transaction for seq %d\n", clientSeq)
		return
	}
	startTime, ok := gateway.startTimeMap.Load(clientSeq)
	if !ok {
		log.Printf("storeLatency(): No start time for clientseq: %d\n", clientSeq)
		return
	}
	_, ok = gateway.transactionLatency.Load(clientSeq)
	if !ok {
		latency := endTime.Sub(startTime.(time.Time))
		gateway.transactionLatency.Store(clientSeq, resultStruct{
			transaction: transaction.(*pbft.Transaction),
			latency:     latency,
			success:     result,
		})
	}
}

func (gateway *Gateway) printAllLatencies() {
	for i := int64(1); i <= gateway.highestSeq; i++ {
		details, ok := gateway.transactionLatency.Load(i)
		if !ok {
			fmt.Printf("No data for seq: %d\n", i)
			continue
		}
		data := details.(resultStruct)
		var res string
		if data.success {
			res = "success"
		} else {
			res = "abort"
		}
		fmt.Printf("%d. (%s, %s, %d) %v %s\n", i, data.transaction.From, data.transaction.To, data.transaction.Amount, data.latency, res)
	}
}

func (gateway *Gateway) printSetWiseLatency() {
	lastSeq := 0
	for i := 1; i <= gateway.MaxExecutedSet; i++ {
		count := gateway.TransactionCountMap[i]
		fmt.Printf("Set %d\n--------\n", i)
		fmt.Printf("Transaction count: %d\n", count)
		var duration time.Duration
		duration = 0
		for j := lastSeq + 1; j <= lastSeq+count; j++ {
			transactionDuration, ok := gateway.transactionLatency.Load(int64(j))
			if ok {
				duration += transactionDuration.(resultStruct).latency
			}
		}
		lastSeq += count
		if duration != 0 {
			fmt.Printf("Latency: %v per transction, Throughput: %v transactions/second\n", duration/time.Duration(count), roundToTwoDecimals(float64(count)/duration.Seconds()))
		}

		fmt.Println()
	}
}

func roundToTwoDecimals(val float64) float64 {
	return math.Round(val*100) / 100
}

func (gateway *Gateway) printOverallLatency() {
	lastSeq := 0
	totalCount := 0
	var duration time.Duration
	for i := 1; i <= gateway.MaxExecutedSet; i++ {
		count := gateway.TransactionCountMap[i]
		totalCount += count
		for j := lastSeq + 1; j <= lastSeq+count; j++ {
			transactionDuration, ok := gateway.transactionLatency.Load(int64(j))
			if ok {
				duration += transactionDuration.(resultStruct).latency
			}
		}
		lastSeq += count
	}
	fmt.Printf("Transaction count: %d\n", totalCount)
	if duration != 0 {
		fmt.Printf("Latency: %v per transaction, Throughput: %v transactions/second\n", duration/time.Duration(totalCount), roundToTwoDecimals(float64(totalCount)/duration.Seconds()))
	}
}

func (gateway *Gateway) getPublicKey(nodeId string) (*rsa.PublicKey, error) {
	pubKey, ok := gateway.PublicKeyMap.Load(nodeId)
	if !ok {
		return nil, fmt.Errorf("public key not found for %s", nodeId)
	}

	return pubKey.(*rsa.PublicKey), nil
}

func (gateway *Gateway) initKeys() {
	err := os.MkdirAll("../keys", os.ModePerm)
	if err != nil {
		log.Fatalf("Failed to make directory: %v", err)
	}

	privateKey, publicKey := cryptox.GenerateRSAKeys()
	cryptox.SaveRSAPublicKeyToFile(publicKey, "../keys/client_pub.pem")

	gateway.PrivateKey = privateKey

	go gateway.readPublicKeys()
}

func (gateway *Gateway) readPublicKeys() {
	start := time.Now()
	dirPath := "../keys"
	for time.Since(start) < 30*time.Second {
		err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}
			gateway.readPublicKeyIntoMap(path)

			return nil
		})

		if err != nil {
			log.Fatalf("Error reading directory: %v", err)
		}
		time.Sleep(1 * time.Second)
	}
	log.Println("Stopped reading keys")
}

func (gateway *Gateway) readPublicKeyIntoMap(path string) {
	pubKey, err := cryptox.ReadRSAPublicKeyFromFile(path)
	if err != nil {
		log.Printf("Error reading public key: %s\n", err)
		return
	}
	splitPathSlice := strings.Split(path, "/")
	nodeId := strings.Split(splitPathSlice[len(splitPathSlice)-1], "_")[0]
	gateway.PublicKeyMap.Store(nodeId, pubKey)
}

func (gateway *Gateway) getClusterOfClient(client string) string {
	clientInt, _ := strconv.ParseInt(client, 10, 64)
	return fmt.Sprintf("C%d", (clientInt / 1000) + 1)
}

func (gateway *Gateway) getViewLeader(cluster string) string {
	clusterView, _ := gateway.ViewMap.Load(cluster)
	clusterInt, _ := strconv.ParseInt(cluster[1:], 10, 64)
	serverNum := ((clusterInt - 1) * 4) + (((int64(clusterView.(int)) - 1) % 4) + 1)
	return fmt.Sprintf("S%d", serverNum)
}