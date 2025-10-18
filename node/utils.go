package node

import (
	"2pcbyz-prajwal/config"
	"2pcbyz-prajwal/cryptox"
	"2pcbyz-prajwal/gateway/clientrpc"
	"2pcbyz-prajwal/pbft"
	"crypto/rsa"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (node *Node) establishConns() error {

	node.LeaderMap.Store("C1", "S1")
	node.LeaderMap.Store("C2", "S5")
	node.LeaderMap.Store("C3", "S9")

	for cluster, nodes := range node.NodeMap {
		node.ClientMap[cluster] = make(map[string]pbft.PbftClient)
		node.ConnMap[cluster] = make(map[string]*grpc.ClientConn)
		for id, port := range nodes {
			if node.Id != id {
				log.Printf("Establishing connection from %s to %s\n", node.Id, id)
				var conn *grpc.ClientConn
				conn, err := grpc.NewClient(port, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					return err
				}
				client := pbft.NewPbftClient(conn)
				node.ClientMap[cluster][id] = client
				node.ConnMap[cluster][id] = conn
			}
		}
	}

	conn, err := grpc.NewClient(":8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	client := clientrpc.NewClientClient(conn)
	node.ClientgrpcClient = client
	return nil
}

func (node *Node) closeConnections() {
	for _, nodes := range node.ConnMap {
		for id, conn := range nodes {
			conn.Close()
			log.Printf("Closing connection from %s to %s\n", node.Id, id)
		}
	}
}

func printMenu() {
	fmt.Println("\nMenu:")
	fmt.Println("2. Print New View messages")
	fmt.Println("3. Print Prepares and Commits")
	fmt.Println("4. Print Transaction Log")
	fmt.Println("5. Performance")
	fmt.Println("6. Performance transaction wise")
	fmt.Println("7. Print checkpoints")
	fmt.Println("0. Exit")
	fmt.Print("Choose an option: ")
}

func (node *Node) readPublicKeys() {
	start := time.Now()
	dirPath := "./keys"
	for time.Since(start) < 30*time.Second {
		err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			node.readPublicKeyIntoMap(path)

			return nil
		})

		if err != nil {
			log.Fatalf("Error reading directory: %v", err)
		}
		time.Sleep(1 * time.Second)
	}
}

func (node *Node) readPublicKeyIntoMap(path string) {
	pubKey, err := cryptox.ReadRSAPublicKeyFromFile(path)
	if err != nil {
		log.Printf("Error reading public key: %s\n", err)
		return
	}

	splitPathSlice := strings.Split(path, "/")
	nodeId := strings.Split(splitPathSlice[len(splitPathSlice)-1], "_")[0]
	node.PublicKeyMap.Store(nodeId, pubKey)
}

func (node *Node) initKeys() {
	privateKey, publicKey := cryptox.GenerateRSAKeys()
	node.PrivateKey = privateKey
	node.savePublicKey(publicKey)
	go node.readPublicKeys()
}

func (node *Node) savePublicKey(pubKey *rsa.PublicKey) {
	err := os.MkdirAll("keys", os.ModePerm)
	if err != nil {
		log.Fatalf("Failed to make directory: %v", err)
	}
	cryptox.SaveRSAPublicKeyToFile(pubKey, fmt.Sprintf("keys/%s_pub.pem", node.Id))
}

func (node *Node) getViewLeader() string {
	clusterInt, _ := strconv.ParseInt(node.ClusterId[1:], 10, 64)
	serverNum := ((clusterInt - 1) * 4) + (((node.View - 1) % 4) + 1)
	return fmt.Sprintf("S%d", serverNum)
}

func (node *Node) printLog() {
	node.TransactionLogLock.RLock()
	defer node.TransactionLogLock.RUnlock()
	var sortedSeqs []int64
	for seq := range node.TransactionLog {
		sortedSeqs = append(sortedSeqs, seq)
	}
	sort.Slice(sortedSeqs, func(i, j int) bool {
		return sortedSeqs[i] < sortedSeqs[j]
	})
	for _, seq := range sortedSeqs {
		tLog := node.TransactionLog[seq]
		fmt.Printf("Seq: %d %v State: %s View: %d Timestamp: %d\n", seq, tLog.Transaction, config.IntToStates[tLog.State], tLog.View, tLog.Timestamp)
	}
}

func (node *Node) verifyPrepareRequest(req *pbft.PrepareRequest) error {
	pubKey, ok := node.PublicKeyMap.Load(req.Prepare.NodeId)
	if !ok {
		return fmt.Errorf("no public key for %s", req.Prepare.NodeId)
	}

	err := cryptox.VerifySignature(pubKey.(*rsa.PublicKey), req.Prepare, req.Signature)
	if err != nil {
		return errors.New("signature verification failed")
	}

	if node.TransactionLog[req.Prepare.Sequence].Digest != req.Prepare.Digest {
		log.Printf("Digest from node %s not matching seq: %d in prepare: %v, in log %v\n", req.Prepare.NodeId, req.Prepare.Sequence, req.Prepare.Digest, node.TransactionLog[req.Prepare.Sequence].Digest)
		return errors.New("digest does not match")
	}

	return nil
}

func (node *Node) verifyCommitRequest(req *pbft.CommitRequest) error {
	pubKey, ok := node.PublicKeyMap.Load(req.Commit.NodeId)
	if !ok {
		return fmt.Errorf("no public key for %s", req.Commit.NodeId)
	}

	err := cryptox.VerifySignature(pubKey.(*rsa.PublicKey), req.Commit, req.Signature)
	if err != nil {
		return errors.New("signature verification failed")
	}

	if node.TransactionLog[req.Commit.Sequence].Digest != req.Commit.Digest {
		return errors.New("digest does not match")
	}

	return nil
}

func (node *Node) getPublicKey(nodeId string) (*rsa.PublicKey, error) {
	pubKey, ok := node.PublicKeyMap.Load(nodeId)
	if !ok {
		return nil, fmt.Errorf("public key not found for %s", nodeId)
	}

	return pubKey.(*rsa.PublicKey), nil
}

func (node *Node) getTransactionLog(seq int64) (TransactionLog, bool) {
	node.TransactionLogLock.RLock()
	defer node.TransactionLogLock.RUnlock()

	tLog, ok := node.TransactionLog[seq]
	if !ok {
		return tLog, false
	}
	return tLog, true
}

func (node *Node) resetBalances() {
	node.DB.InitiateBalances(node.StartId, node.EndId)
}

func (node *Node) printPrepareCerts(seq int64) {
	if len(node.PrepareCerts[seq]) == 0 {
		fmt.Printf("No prepares for %d\n", seq)
		return
	}
	fmt.Printf("Prepares received for sequence number %d\n", seq)
	// if len(node.PrepareCerts[seq]) >= 3*config.F {
	// 	fmt.Printf("Optimistic phase reduction\n")
	// 	fmt.Println()
	// 	return
	// }
	for i, cert := range node.PrepareCerts[seq] {
		fmt.Printf("%d: View: %d Sequence: %d From: %s Digest: %s\n", i+1, cert.Prepare.View, cert.Prepare.Sequence, cert.Prepare.NodeId, string(cert.Prepare.Digest))
	}
	fmt.Println()
}

func (node *Node) printCommitCerts(seq int64) {
	node.CommitCertsLock.RLock()
	defer node.CommitCertsLock.RUnlock()
	fmt.Printf("Commits received for sequence number: %d\n", seq)
	if len(node.PrepareCerts[seq]) >= 3*config.F {
		for i, cert := range node.PrepareCerts[seq] {
			if i == 0 {
				fmt.Printf("%d: View: %d Sequence: %d From: %s Digest: %s\n", i+1, cert.Prepare.View, cert.Prepare.Sequence, node.getViewLeader(), string(cert.Prepare.Digest))
			}
			fmt.Printf("%d: View: %d Sequence: %d From: %s Digest: %s\n", i+2, cert.Prepare.View, cert.Prepare.Sequence, cert.Prepare.NodeId, string(cert.Prepare.Digest))
		}
		fmt.Println()
		return
	}
	if len(node.CommitCerts[seq]) == 0 {
		fmt.Printf("No commits for %d\n", seq)
		fmt.Println()
		return
	}

	for i, cert := range node.CommitCerts[seq] {
		fmt.Printf("%d: View: %d Sequence: %d From: %s Digest: %s\n", i+1, cert.Commit.View, cert.Commit.Sequence, cert.Commit.NodeId, cert.Commit.Digest)
	}
	fmt.Println()
}

func (node *Node) printPrePrepare(seq int64) {
	if preP, ok := node.PrePrepares[seq]; ok {
		fmt.Printf("Pre-Prepare: ")
		fmt.Printf("Seq: %d View: %d Digest: %s\n", preP.PrePrepare.Sequence, preP.PrePrepare.View, preP.PrePrepare.Digest)
	} else {
		fmt.Printf("No pre-prepare for seq: %d\n", seq)
	}
}

func (node *Node) printAllCerts() {
	var sortedSeqs []int64
	for seq := range node.PrePrepares {
		sortedSeqs = append(sortedSeqs, seq)
	}
	sort.Slice(sortedSeqs, func(i, j int) bool {
		return sortedSeqs[i] < sortedSeqs[j]
	})

	for _, i := range sortedSeqs {
		node.printPrePrepare(i)
		node.printPrepareCerts(i)
		node.printCommitCerts(i)
	}
}

func (node *Node) printNewViews() {
	for _, newView := range node.NewViewCerts {
		fmt.Printf("New view message for view: %d\n", newView.View)
		fmt.Println("View Change certs")
		for i, cert := range newView.ViewChangeCerts {
			fmt.Printf("%d. From: %s Checkpoint certs: %v\n", i+1, cert.ViewChangeRequest.NodeId, cert.ViewChangeRequest.CheckpointCerts)
		}
		fmt.Println("Prepared logs")
		for i, prep := range newView.Prepared {
			fmt.Printf("%d. Pre-prepare: %v\n Client message: %v\n", i+1, prep.PrePrepare, prep.ClientReq.ClientRequest)
		}
		fmt.Printf("Latest stable checkpoint: %d\n", newView.LatestStableCheckpoint)
	}
}

func (node *Node) printCheckpoints() {
	for seq, checkpointData := range node.CheckpointCerts {
		if seq == 0 {
			continue
		}
		fmt.Printf("Seq: %d, Stable: %t\n", seq, checkpointData.Stable)
		fmt.Printf("Checkpoint certificates:\n")
		for num, cert := range checkpointData.Certs {
			fmt.Printf("%d. %v\n", num+1, cert.Checkpoint)
		}
		// log.Printf("Waiting certs for seq: %d\n", seq)
		// for num, cert := range checkpointData.Waiting {
		// 	log.Printf("%d. %v\n", num, cert.Checkpoint)
		// }
	}
}

// func copyMap(original map[string]int64) map[string]int64 {
// 	copiedMap := make(map[string]int64)

// 	for key, value := range original {
// 		copiedMap[key] = value
// 	}

// 	return copiedMap
// }

func (node *Node) validateCheckpoint(signedReq *pbft.SignedCheckpointRequest) error {
	nodeId := signedReq.Checkpoint.NodeId
	pubKey, err := node.getPublicKey(nodeId)
	if err != nil {
		log.Printf("Public key not found for %s\n", nodeId)
		return fmt.Errorf("public key not found for %s: %v", nodeId, err)
	}
	err = cryptox.VerifySignature(pubKey, signedReq.Checkpoint, signedReq.Signature)
	if err != nil {
		log.Printf("Failed to verify signature on checkpoint req from %s\n", nodeId)
		return fmt.Errorf("signature verification failed: %v", err)
	}

	node.CheckpointCertsLock.Lock()
	if checkpointData, ok := node.CheckpointCerts[signedReq.Checkpoint.Sequence]; ok {
		thisDigest := checkpointData.Digest
		if thisDigest == signedReq.Checkpoint.Digest {
			checkpointData.Certs = append(checkpointData.Certs, signedReq)
			if len(checkpointData.Certs) == 2*config.F+1 {
				checkpointData.Stable = true
				if signedReq.Checkpoint.Sequence > node.LastStable {
					node.LastStable = signedReq.Checkpoint.Sequence
					node.LastStableBalances = checkpointData.Balances
					node.SeqLock.Lock()
					node.Seq = max(node.Seq, node.LastStable+1)
					node.SeqLock.Unlock()
				}
				node.LowWater = signedReq.Checkpoint.Sequence + 1
				node.HighWater = node.LowWater + 50
				log.Printf("Stable checkpoint sequence %d\n", signedReq.Checkpoint.Sequence)
				go node.collectGarbage(signedReq.Checkpoint.Sequence)
			}
			node.CheckpointCerts[signedReq.Checkpoint.Sequence] = checkpointData
		} else if thisDigest == "" {
			log.Printf("Digest empty, adding request to waiting list")
			checkpointData := node.CheckpointCerts[signedReq.Checkpoint.Sequence]
			waiting := append(checkpointData.Waiting, signedReq)
			checkpointData.Waiting = waiting
			node.CheckpointCerts[signedReq.Checkpoint.Sequence] = checkpointData

			// if len(waiting) == 2*config.F + 1 {
			// 	checkpointData.Stable = true
			// 	if signedReq.Checkpoint.Sequence > node.LastStable {
			// 		node.LastStable = signedReq.Checkpoint.Sequence
			// 		node.LastStableBalances = checkpointData.Balances
			// 		node.SeqLock.Lock()
			// 		node.Seq = max(node.Seq, node.LastStable + 1)
			// 		node.SeqLock.Unlock()
			// 	}
			// 	log.Printf("Stable checkpoint sequence %d\n", signedReq.Checkpoint.Sequence)
			// }
		}
	} else {
		var waiting []*pbft.SignedCheckpointRequest
		waiting = append(waiting, signedReq)
		checkpointData := CheckpointData{
			Digest:  "",
			Stable:  false,
			Waiting: waiting,
		}
		node.CheckpointCerts[signedReq.Checkpoint.Sequence] = checkpointData
	}
	node.CheckpointCertsLock.Unlock()

	return nil
}

func convertBalancesToProto(balances map[string]int64) *pbft.BalanceStructList {
	var balanceProtoSlice []*pbft.BalanceStruct
	var sortedKeys []string
	for node := range balances {
		sortedKeys = append(sortedKeys, node)
	}

	sort.Strings(sortedKeys)

	for _, node := range sortedKeys {
		balanceProtoSlice = append(balanceProtoSlice, &pbft.BalanceStruct{Client: node, Amount: balances[node]})
	}

	return &pbft.BalanceStructList{
		BalanceMap: balanceProtoSlice,
	}
}

func (node *Node) convertProtoBalancesToMap(protobalances *pbft.BalanceStructList) map[string]int64 {
	balances := make(map[string]int64)
	for _, balance := range protobalances.BalanceMap {
		balances[balance.Client] = balance.Amount
	}
	return balances
}

func (node *Node) collectGarbage(seq int64) {
	log.Printf("Cleaning up messages before seq: %d\n", seq)
	node.PrepareCertsLock.Lock()
	for i := int64(1); i <= seq; i++ {
		delete(node.PrepareCerts, i)
	}
	node.PrepareCertsLock.Unlock()

	node.CommitCertsLock.Lock()
	for i := int64(1); i <= seq; i++ {
		delete(node.CommitCerts, i)
	}
	node.CommitCertsLock.Unlock()

	node.PrePreparesLock.Lock()
	for i := int64(1); i <= seq; i++ {
		delete(node.PrePrepares, i)
	}
	node.PrePreparesLock.Unlock()
}

func (node *Node) saveTransaction(seq int64, status string) {
	logEntry, _ := node.getTransactionLog(seq)
	_, err := node.DB.SaveTransactionToDB(seq, logEntry.Transaction, status)
	if err != nil {
		log.Printf("Erros saving %d. (%s, %s, %d) %s to datastore\n", seq, logEntry.Transaction.From, logEntry.Transaction.To, logEntry.Transaction.Amount, status)
	}
}

type DBUnit struct {
	Seq         int64
	Transaction *pbft.Transaction
	Status      string
	timestamp   int64
	ClientSeq   int64
}

func (node *Node) saveTransactionToDatastore(seq int64, status string, clientSeq int64) {
	node.DatastoreLock.Lock()
	defer node.DatastoreLock.Unlock()
	logEntry, _ := node.getTransactionLog(seq)
	node.Datastore[seq] = DBUnit{
		Seq:         seq,
		Transaction: logEntry.Transaction,
		Status:      status,
		timestamp:   logEntry.Timestamp,
		ClientSeq:   clientSeq,
	}
	node.DatastoreMax = max(node.DatastoreMax, seq)
}

func (node *Node) GetDatastoreLog(seq int64) (DBUnit, bool) {
	node.DatastoreLock.RLock()
	defer node.DatastoreLock.RUnlock()
	datastoreLog, ok := node.Datastore[seq]
	return datastoreLog, ok
}

func (node *Node) GetAllDatastore() []DBUnit {
	node.DatastoreLock.RLock()
	defer node.DatastoreLock.RUnlock()
	var blocks []DBUnit
	for i := int64(1); i <= node.DatastoreMax; i++ {
		if data, ok := node.Datastore[i]; ok {
			blocks = append(blocks, data)
		} else {
			// break
		}
	}
	return blocks
}

func (node *Node) undoWAL(clientSeq int64) error {
	log.Printf("Undoing WAL")
	wal, err := node.DB.ReadWALEntry(clientSeq)
	if err != nil {
		log.Printf("Error fetching WAL for clientseq %d\n", clientSeq)
		node.DB.PrintAllWAL()
		return err
	}
	for _, logEntry := range wal.Logs {
		log.Printf("Undoing transaction (%s, %s, %d)\n", logEntry.Transaction.From, logEntry.Transaction.To, logEntry.Transaction.Amount)
		node.DB.UpdateBalances(&pbft.Transaction{
			From:   logEntry.Transaction.To,
			To:     logEntry.Transaction.From,
			Amount: logEntry.Transaction.Amount,
		})
	}
	return nil
}
