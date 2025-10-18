package node

import (
	"2pcbyz-prajwal/config"
	// "2pcbyz-prajwal/cryptox"
	"2pcbyz-prajwal/db"
	"2pcbyz-prajwal/gateway/clientrpc"
	"2pcbyz-prajwal/pbft"
	"crypto/rsa"
	"sync"

	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
)

type Node struct {
	pbft.UnimplementedPbftServer
	ClusterId           string
	Id                  string `json:"id"`
	Port                string `json:"port"`
	NodeMap             map[string]map[string]string
	ConnMap             map[string]map[string]*grpc.ClientConn
	ClientMap           map[string]map[string]pbft.PbftClient
	Datastore           map[int64]DBUnit
	DatastoreLock       sync.RWMutex
	DatastoreMax        int64
	PrivateKey          *rsa.PrivateKey
	PublicKeyMap        sync.Map
	View                int64
	Seq                 int64
	SeqLock             sync.Mutex
	TransactionLog      map[int64]TransactionLog
	TransactionLogLock  sync.RWMutex
	PrePrepares         map[int64]*pbft.PrePrepareRequest
	PrePreparesLock     sync.RWMutex
	PrepareCertChans    map[int64]chan *pbft.PrepareRequest
	PrepareCerts        map[int64][]*pbft.PrepareRequest
	PrepareCertsLock    sync.RWMutex
	CommitCertChans     map[int64]chan *pbft.CommitRequest
	CommitCerts         map[int64][]*pbft.CommitRequest
	CheckpointCerts     map[int64]CheckpointData
	CheckpointCertsLock sync.RWMutex
	CommitCertsLock     sync.RWMutex
	RequestLog          map[int64]RequestData
	RequestLogLock      sync.RWMutex
	ClientgrpcClient    clientrpc.ClientClient
	LockLock            sync.Mutex
	Live                bool
	Byzantine           bool
	LastStable          int64
	LastStableBalances  map[string]int64
	TotalTime           time.Duration
	TransactionCount    int
	PerfStrings         []string
	ViewChanging        bool
	NewViewNum          int64
	NewViewCerts        []*pbft.NewViewRequest
	ExecutionCount      int64
	Tmr                 *Timer
	ViewChangeCerts     map[int64][]*pbft.SignedViewChangeRequest
	ViewChangeCertsLock sync.RWMutex
	MaxSeq              int64
	ExecutorSeq         int64
	NewViewsSent        sync.Map
	LowWater            int64
	HighWater           int64
	DB                  *db.Database
	DBCloseFunc         func() error
	StartId             string
	EndId               string
	TimeKeeper          time.Time
	LeaderMap           sync.Map
	PCCommitsMap        sync.Map
}

type CheckpointData struct {
	Stable   bool
	Digest   string
	Balances map[string]int64
	Certs    []*pbft.SignedCheckpointRequest
	Waiting  []*pbft.SignedCheckpointRequest
}

type RequestData struct {
	Seq      int64
	Executed bool
	Result   bool
}

type State string

const (
	PrePrepared State = "PP"
	Prepared    State = "P"
	Committed   State = "C"
	Executed    State = "E"
)

type TransactionLog struct {
	Transaction *pbft.Transaction
	View        int64
	State       int64
	Digest      string
	Timestamp   int64
	ClientSeq   int64
}

type ClusterStruct struct {
	Nodes []*Node `json:"nodes"`
	Start string  `json:"start"`
	End   string  `json:"end"`
}

func NewNode(clusterId string, nodeId string, clusters map[string]ClusterStruct, start string, end string) (*Node, error) {
	dbName := "./dbs/" + clusterId + "_" + nodeId + ".db"
	db, dbCloseFunc, err := db.InitDatabase(dbName)
	if err != nil {
		return nil, fmt.Errorf("InitDatabase(%s): %w", dbName, err)
	}

	nodeMap := make(map[string]map[string]string)
	for cluster, clusterData := range clusters {
		nodeMap[cluster] = make(map[string]string)
		for _, node := range clusterData.Nodes {
			nodeMap[cluster][node.Id] = node.Port
		}
	}

	connMap := make(map[string]map[string]*grpc.ClientConn)
	lastStableBalances := make(map[string]int64)
	clientMap := make(map[string]map[string]pbft.PbftClient)
	transactionLog := make(map[int64]TransactionLog)
	prepareCertsChans := make(map[int64]chan *pbft.PrepareRequest)
	prepareCerts := make(map[int64][]*pbft.PrepareRequest)
	checkpointCerts := make(map[int64]CheckpointData)
	commitCertsChans := make(map[int64]chan *pbft.CommitRequest)
	commitCerts := make(map[int64][]*pbft.CommitRequest)
	requestLog := make(map[int64]RequestData)
	prePrepares := make(map[int64]*pbft.PrePrepareRequest)
	viewChangeCerts := make(map[int64][]*pbft.SignedViewChangeRequest)

	node := &Node{
		ClusterId:          clusterId,
		Id:                 nodeId,
		Port:               nodeMap[clusterId][nodeId],
		NodeMap:            nodeMap,
		ConnMap:            connMap,
		View:               1,
		Seq:                1,
		Live:               true,
		Byzantine:          false,
		ClientMap:          clientMap,
		TransactionLog:     transactionLog,
		PrepareCertChans:   prepareCertsChans,
		PrepareCerts:       prepareCerts,
		CommitCertChans:    commitCertsChans,
		CommitCerts:        commitCerts,
		RequestLog:         requestLog,
		CheckpointCerts:    checkpointCerts,
		PrePrepares:        prePrepares,
		LastStable:         0,
		ViewChangeCerts:    viewChangeCerts,
		ExecutorSeq:        1,
		LastStableBalances: lastStableBalances,
		LowWater:           1,
		HighWater:          10,
		DB:                 db,
		DBCloseFunc:        dbCloseFunc,
		StartId:            start,
		EndId:              end,
		TimeKeeper:         time.Now(),
		Datastore:          make(map[int64]DBUnit),
		DatastoreMax:       0,
	}
	return node, nil
}

func (node *Node) Start() {
	node.establishConns()
	node.initKeys()
	node.resetBalances()
	node.DB.InitiateBalances(node.StartId, node.EndId)
	node.Tmr = NewTimer(2500 * time.Second)
	// node.StartPBFTRunner()
	go node.executor()
	go node.startMonitoringTimer()
	// go node.timePrinter()
	for {
		printMenu()
		var input string
		fmt.Scan(&input)

		switch input {
		case "2":
			node.printNewViews()
		case "3":
			// var seq int64
			// fmt.Printf("Enter sequence number: ")
			// fmt.Scanf("%d", &seq)
			// node.printPrepareCerts(seq)
			// node.printCommitCerts(seq)
			node.printAllCerts()
		case "4":
			node.printLog()
		case "7":
			node.printCheckpoints()
		case "0", "exit", ";":
			log.Println("Closing connections")
			node.closeConnections()
			log.Println("Exiting...")
			return
		default:
			fmt.Println("Invalid option. Please choose again.")
		}
	}
}

func (node *Node) writeLog(seq int64, transaction *pbft.Transaction, view int64, digest string, timestamp int64) {
	node.MaxSeq = max(node.MaxSeq, seq)
	node.TransactionLogLock.Lock()
	defer node.TransactionLogLock.Unlock()
	node.TransactionLog[seq] = TransactionLog{
		Transaction: transaction,
		State:       config.States["pre-prepared"],
		View:        view,
		Digest:      digest,
		Timestamp:   timestamp,
		ClientSeq:   transaction.ClientSeq,
	}

	// _, ok := node.TransactionLog[seq]
	// if !ok {
	// 	node.TransactionLog[seq] = TransactionLog{
	// 		Transaction: transaction,
	// 		State:       config.States["pre-prepared"],
	// 		View:        view,
	// 		Digest:      digest,
	// 		Timestamp:   timestamp,
	// 	}
	// 	node.MaxSeq = max(node.MaxSeq, seq)
	// }
}

func (node *Node) savePrePrepare(seq int64, prePrepare *pbft.PrePrepareRequest) {
	node.PrePreparesLock.Lock()
	defer node.PrePreparesLock.Unlock()
	node.PrePrepares[seq] = prePrepare
}

func (node *Node) checkPrePrepare(seq int64) bool {
	node.PrePreparesLock.RLock()
	defer node.PrePreparesLock.RUnlock()
	_, ok := node.PrePrepares[seq]
	return ok
}

func (node *Node) executor() {
	for {
		if datastoreLog, ok := node.GetDatastoreLog(node.ExecutorSeq); ok {
			res := node.executeDatastoreLog(datastoreLog)
			log.Printf("Executed seq: %d %t\n", datastoreLog.Seq, res)
			if datastoreLog.Status == "" || datastoreLog.Status == "C" {
				go node.replyToClient(datastoreLog.timestamp, res, datastoreLog.Transaction.ClientSeq)
				go node.updateLog(datastoreLog.Seq, "executed")
				// go node.saveExecutionResult(datastoreLog.timestamp, true, node.ExecutorSeq)
			} else if datastoreLog.Status == "A" {
				go node.replyToClient(datastoreLog.timestamp, false, datastoreLog.Transaction.ClientSeq)
				go node.updateLog(datastoreLog.Seq, "executed")
				// go node.saveExecutionResult(datastoreLog.timestamp, true, node.ExecutorSeq)
			}
			node.ExecutorSeq++
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
	// for {
	// 	logEntry, ok := node.getTransactionLog(node.ExecutorSeq)
	// 	if ok {
	// 		if logEntry.Digest == "" {
	// 			node.ExecutorSeq++
	// 		} else if logEntry.State == config.States["committed"] {
	// res := node.executeLog(node.ExecutorSeq, logEntry)
	// 			node.ExecutorSeq += 1
	// 			if logEntry.Transaction.ClusterFrom == logEntry.Transaction.ClusterTo {
	// 				log.Printf("HERE\n")
	// 				go node.replyToClient(logEntry.Timestamp, res, logEntry.ClientSeq)
	// 			}
	// 			log.Printf("RequestCount: %d, ExecutionCount: %d\n", node.RequestCount, node.ExecutionCount)
	// 			if node.RequestCount > node.ExecutionCount {
	// 				log.Printf("RequestCount: %d, ExecutionCount: %d\n", node.RequestCount, node.ExecutionCount)
	// 				node.Tmr.StartOrReset()
	// 			} else {
	// 				log.Printf("Stopping timer")
	// 				node.Tmr.Stop()
	// 			}
	// 		} else if logEntry.State == config.States["executed"] {
	// 			node.ExecutorSeq++
	// 		}
	// 	}
	// }
}

func (node *Node) executeDatastoreLog(dbUnit DBUnit) bool {
	transaction := dbUnit.Transaction
	if dbUnit.Status == "" || dbUnit.Status == "P" {
		log.Printf("Executing seq: %d\n", dbUnit.Seq)
		node.DB.UpdateBalances(transaction)
	} else if dbUnit.Status == "A" {
		log.Printf("Reversing seq: %d\n", dbUnit.Seq)
		node.undoWAL(dbUnit.ClientSeq)
		// node.DB.UpdateBalances(&pbft.Transaction{From: transaction.To, To: transaction.From, Amount: transaction.Amount})
	}
	if dbUnit.Status != "P" {
		node.DB.ReleaseLocksDB([]string{transaction.From, transaction.To})
	}
	node.ExecutionCount++
	return true
}

// func (node *Node) executeLog(seq int64, transactionLog TransactionLog) bool {
// 	log.Printf("Executing seq: %d\n", seq)
// 	if transactionLog.Digest == "" {
// 		log.Printf("Seq: %d, null digest\n", seq)
// 		node.ExecutionCount += 1
// 		return true
// 	}
// 	transaction := transactionLog.Transaction
// 	node.DB.UpdateBalances(transaction)
// 	if transaction.ClusterFrom == transaction.ClusterTo {
// 		go node.updateLog(seq, "executed")
// 		// go node.saveExecutionResult(transactionLog, true, node.ExecutorSeq)
// 	} else {
// 		go node.updateLog(seq, "pexecuted")
// 	}
// 	node.DB.ReleaseLocksDB([]string{transaction.From, transaction.To})
// 	node.ExecutionCount += 1
// 	return true
// }

// func (node *Node) doCheckpointing(seq int64, balancesCopy map[string]int64) {
// 	// balancesCopy := copyMap(node.Balances)
// 	digest, err := cryptox.MD5Hash(balancesCopy)
// 	if err != nil {
// 		log.Printf("Failed to create checkpoint digest: %v\n", err)
// 		return
// 	}
// 	log.Printf("Sending checkpoints for seq %d\n", seq)

// 	checkpointReq := &pbft.Checkpoint{
// 		Sequence: seq,
// 		Digest:   digest,
// 		NodeId:   node.Id,
// 	}
// 	signature, err := cryptox.SignMessage(node.PrivateKey, checkpointReq)
// 	if err != nil {
// 		log.Printf("Failed to sign checkpoint message: %v\n", err)
// 		return
// 	}

// 	signedReq := &pbft.SignedCheckpointRequest{
// 		Checkpoint: checkpointReq,
// 		Signature:  signature,
// 	}

// 	node.CheckpointCertsLock.Lock()

// 	waiting := node.CheckpointCerts[seq].Waiting
// 	var certs []*pbft.SignedCheckpointRequest
// 	certs = append(certs, signedReq)
// 	node.CheckpointCerts[seq] = CheckpointData{
// 		Digest:   digest,
// 		Stable:   false,
// 		Balances: balancesCopy,
// 		Certs:    certs,
// 	}
// 	node.CheckpointCertsLock.Unlock()

// 	go node.sendCheckpoints(signedReq)

// 	for _, waitingReq := range waiting {
// 		log.Printf("Processing waiting request: %v\n", waitingReq.Checkpoint)
// 		err = node.validateCheckpoint(waitingReq)
// 		if err != nil {
// 			log.Printf("Waiting checkpoint req validation failed: %v\n", err)
// 		}
// 	}
// }

func (node *Node) updateLog(seq int64, status string) {
	node.TransactionLogLock.Lock()
	defer node.TransactionLogLock.Unlock()

	transactionLog, ok := node.TransactionLog[seq]
	if ok && transactionLog.State < config.States[status] {
		log.Printf("Updating log of seq %d to status %s\n", seq, status)
		updatedLog := TransactionLog{
			Transaction: transactionLog.Transaction,
			View:        transactionLog.View,
			State:       config.States[status],
			Digest:      transactionLog.Digest,
			Timestamp:   transactionLog.Timestamp,
			ClientSeq:   transactionLog.ClientSeq,
		}
		node.TransactionLog[seq] = updatedLog
	}
}
