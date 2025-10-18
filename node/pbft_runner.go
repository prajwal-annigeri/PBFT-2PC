package node

import (
	"2pcbyz-prajwal/pbft"
	"context"
	"log"
)

func (node *Node) runIntrashard(clientReq *pbft.SignedClientRequest, oldSeq int64) {
	transaction := clientReq.ClientRequest.Transaction
	_, err := node.runPBFT(clientReq, oldSeq, "")
	if err != nil {
		log.Printf("PBFT run failed for %d. (%s, %s, %d)\n", transaction.ClientSeq, transaction.From, transaction.To, transaction.Amount)
	} else {
		log.Printf("PBFT run done for %d. (%s, %s, %d)\n", transaction.ClientSeq, transaction.From, transaction.To, transaction.Amount)
	}
}

func (node *Node) runCrossShard(clientReq *pbft.SignedClientRequest, oldSeq int64) {
	transaction := clientReq.ClientRequest.Transaction
	preprepareReq, err := node.runPBFT(clientReq, oldSeq, "P")
	if err == nil {
		clusterTo := clientReq.ClientRequest.Transaction.ClusterTo
		participantLeader, _ := node.LeaderMap.Load(clusterTo)
		node.CommitCertsLock.RLock()
		commitCerts := node.CommitCerts[preprepareReq.PrePrepare.Sequence]
		node.CommitCertsLock.RUnlock()
		PCPrepareReq := &pbft.PCPrepareReq{
			SignedClientReq: clientReq,
			Commits:         commitCerts,
			NodeId:          node.Id,
		}
		log.Printf("Sending PCPrepare for clientseq %d with %d commits\n", transaction.ClientSeq, len(commitCerts))
		_, err := node.ClientMap[clientReq.ClientRequest.Transaction.ClusterTo][participantLeader.(string)].PCPrepare(context.Background(), PCPrepareReq)

		if err != nil {
			log.Printf("Error 2pc prepare ack for seq: %d: %v\n", transaction.ClientSeq, err)
			node.runPBFT(clientReq, 0, "A")
		} else {
			log.Printf("Running Commit PBFT for seq: %d\n", transaction.ClientSeq)
			_, err := node.runPBFT(clientReq, 0, "C")
			if err != nil {
				log.Printf("Don't think this will happen: %v\n", err)
			} else {
				go node.replyToClient(clientReq.ClientRequest.Timestamp.Seconds, true, transaction.ClientSeq)
				go node.DB.EraseWal(transaction.ClientSeq)
				_, err := node.ClientMap[clientReq.ClientRequest.Transaction.ClusterTo][participantLeader.(string)].PCCommit(context.Background(), PCPrepareReq)
				if err != nil {
					for {
						if err != nil {
							log.Printf("Resending commit!!!!!!!!")
							_, err = node.ClientMap[clientReq.ClientRequest.Transaction.ClusterTo][participantLeader.(string)].PCCommit(context.Background(), PCPrepareReq)
						} else {
							break
						}
						
					}
					
				}
			}
		}

	}
}
