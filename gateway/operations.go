package main

import (
	"2pcbyz-prajwal/config"
	"2pcbyz-prajwal/pbft"
	"context"
	"log"
	"slices"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (gateway *Gateway) setLiveServers(liveNodes []string) bool {
	var aliveNodes []string
	var deadNodes []string
	for _, nodes := range gateway.NodeMap {
		for node := range nodes {
			if !slices.Contains(liveNodes, node) {
				log.Printf("Setting unalive server %s\n", node)
				deadNodes = append(deadNodes, node)
			} else {
				log.Printf("Setting alive server %s\n", node)
				aliveNodes = append(aliveNodes, node)
			}
		}
	}
	for _, node := range deadNodes {
		client := gateway.gRPCClientMap[node]
		_, err := client.SetStatus(context.Background(), wrapperspb.Bool(false))
		if err != nil {
			log.Printf("Error setting unalive node %s\n", node)
		}
	}
	for _, node := range aliveNodes {
		client := gateway.gRPCClientMap[node]
		client.SetStatus(context.TODO(), wrapperspb.Bool(true))
	}
	return true
}

func (gateway *Gateway) setByzantineServers(byzantineNodes []string) bool {
	log.Printf("Byzantine this set: %v\n", byzantineNodes)
	var goodNodes []string
	for _, nodes := range gateway.NodeMap {
		for node := range nodes {
			if !slices.Contains(byzantineNodes, node) {
				log.Printf("Setting non-byzantine server %s\n", node)
				goodNodes = append(goodNodes, node)
			}
		}		
	}
	for _, node := range goodNodes {
		client := gateway.gRPCClientMap[node]
		_, err := client.SetByzantine(context.Background(), wrapperspb.Bool(false))
		if err != nil {
			log.Printf("Error setting non-byzantine node %s\n", node)
		}
	}
	log.Printf("Byzantine: %v\n", byzantineNodes)
	log.Printf("ByzantineLen: %v\n", len(byzantineNodes))
	if len(byzantineNodes) > 0 {
		for num, node := range byzantineNodes {
			if node == "" {
				continue
			}
			log.Printf("Byz node %d: %v\n", num, node)
			client := gateway.gRPCClientMap[node]
			client.SetByzantine(context.TODO(), wrapperspb.Bool(true))
		}
	}
	return true
}

func (gateway *Gateway) executeTransactions(transactions []*pbft.Transaction, setNum int) {
	log.Printf("Executing set %d\n", setNum)
	time.Sleep(1 * time.Second)
	for _, transaction := range transactions {		
		time.Sleep(50 * time.Millisecond)
		gateway.highestSeq += 1
		gateway.SeqTransactionMap.Store(gateway.highestSeq, transaction)
		transaction.ClientSeq = gateway.highestSeq
		clusterFrom := gateway.getClusterOfClient(transaction.From)
		clusterTo := gateway.getClusterOfClient(transaction.To)
		transaction.ClusterFrom = clusterFrom
		transaction.ClusterTo = clusterTo
		timestamp := time.Now().UnixMilli()
		signedClientReq := gateway.getSignedClientReq(transaction, timestamp)
		gateway.TimestampSeqMap.Store(timestamp, transaction.ClientSeq)
		go gateway.sendTransaction(signedClientReq)
	}
	log.Printf("Set %d done\n", setNum)
}

func (gateway *Gateway) sendTransaction(signedClientReq *pbft.SignedClientRequest) {
	retry := true
	
	transaction := signedClientReq.ClientRequest.Transaction
	clientSeq := signedClientReq.ClientRequest.Transaction.ClientSeq

	for ;retry; {
		targetServer := gateway.getViewLeader(signedClientReq.ClientRequest.Transaction.ClusterFrom)

		gateway.ReplyMapLock.Lock()
		gateway.ReplyMap[clientSeq] = []ReplyResult {}
		gateway.ReplyMapLock.Unlock()

		gateway.ResCountMapLock.Lock()
		gateway.ResCountMap[signedClientReq.ClientRequest.Timestamp.Seconds] = map[bool]int {
			true: 0,
			false: 0,
		}
		gateway.ResCountMapLock.Unlock()


		log.Printf("Sending transaction %d. (%s, %s, %d) %s to %s with timestamp %v\n", clientSeq, transaction.From, transaction.To, transaction.Amount, transaction.ClusterFrom, targetServer, signedClientReq.ClientRequest.Timestamp)
		gateway.startTimeMap.Store(signedClientReq.ClientRequest.Transaction.ClientSeq, time.Now())
		_, err := gateway.gRPCClientMap[targetServer].ExecuteTransaction(context.Background(), signedClientReq)
		if err != nil {
			log.Printf("Error sending ExecuteTransaction of clientSeq: %d: %v\n", clientSeq, err)
		}

		timer := time.NewTimer(1 * time.Second)
		defer timer.Stop()
		doneReading := false
		for {
			if doneReading {
				break
			}
			select {
			case <-timer.C:
				log.Printf("Timer done for receiving replies for %d\n", clientSeq)
				doneReading = true
				retry = true
			default:
				gateway.ResCountMapLock.RLock()
				done := false
				success := false
				if gateway.ResCountMap[signedClientReq.ClientRequest.Timestamp.Seconds][true] >= config.F + 1 {
					done = true
					success = true
				} else if gateway.ResCountMap[signedClientReq.ClientRequest.Timestamp.Seconds][false] >= config.F + 1 {
					done = true
					success = false
				}
				gateway.ResCountMapLock.RUnlock()

				if done {
					if success {
						go gateway.storeLatency(clientSeq, time.Now(), true)
						log.Printf("Received f+1 success replies for transaction %d. (%s, %s, %d)\n", clientSeq, transaction.From, transaction.To, transaction.Amount)
					} else {
						go gateway.storeLatency(clientSeq, time.Now(), false)
						log.Printf("Received f+1 failure for transaction %d. (%s, %s, %d)\n", clientSeq, transaction.From, transaction.To, transaction.Amount)
					}
					doneReading = true
					retry = false
				} else {
					time.Sleep(20 * time.Millisecond)
				}
			}
		}

	}	
}

func (gateway *Gateway) readReplies() {
	for {
		select {
		case reply := <-gateway.ResultsChan:
			gateway.ReplyMapLock.Lock()
			gateway.ReplyMap[reply.Timestamp] = append(gateway.ReplyMap[reply.Timestamp], reply)
			gateway.ReplyMapLock.Unlock()
			gateway.ResCountMapLock.Lock()
			gateway.ResCountMap[reply.Timestamp][reply.Result] += 1
			gateway.ResCountMapLock.Unlock()
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}
