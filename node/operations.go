package node

import (
	"2pcbyz-prajwal/config"
	"2pcbyz-prajwal/cryptox"
	"2pcbyz-prajwal/db"
	"2pcbyz-prajwal/gateway/clientrpc"
	"2pcbyz-prajwal/pbft"
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func (node *Node) runPBFT(signedClientReq *pbft.SignedClientRequest, oldSeq int64, status string) (*pbft.PrePrepareRequest, error) {
	digest, err := cryptox.MD5Hash(signedClientReq)
	if err != nil {
		return &pbft.PrePrepareRequest{}, errors.New("could not create digest")
	}

	if status == "P" || status == "" {
		for {
			node.LockLock.Lock()
			err = node.DB.CheckAndGetLocks([]string{signedClientReq.ClientRequest.Transaction.From, signedClientReq.ClientRequest.Transaction.To}, false, signedClientReq.ClientRequest.Transaction.ClientSeq)
			node.LockLock.Unlock()
			if err != nil {
				log.Printf("Could not secure locks: %v\n", err)
				log.Printf("Retrying")
			} else {
				log.Printf("Secured locks for seq: %d\n", signedClientReq.ClientRequest.Transaction.ClientSeq)
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if signedClientReq.ClientRequest.Transaction.ClusterFrom == node.ClusterId {
			balance, err := node.DB.GetBalance(signedClientReq.ClientRequest.Transaction.From)
			if err != nil {
				node.DB.ReleaseLocksDB([]string{signedClientReq.ClientRequest.Transaction.From, signedClientReq.ClientRequest.Transaction.To})
				return &pbft.PrePrepareRequest{}, err
			} else if balance < signedClientReq.ClientRequest.Transaction.Amount {
				node.DB.ReleaseLocksDB([]string{signedClientReq.ClientRequest.Transaction.From, signedClientReq.ClientRequest.Transaction.To})
				return &pbft.PrePrepareRequest{}, fmt.Errorf("seq: %d: %s has balance %d. transaction amount is %d", signedClientReq.ClientRequest.Transaction.ClientSeq, signedClientReq.ClientRequest.Transaction.From, balance, signedClientReq.ClientRequest.Transaction.Amount)
			}
		}
	}

	var prePrepare *pbft.PrePrepare
	var preprepareReq *pbft.PrePrepareRequest
	if oldSeq == 0 {
		node.SeqLock.Lock()
		prePrepare = &pbft.PrePrepare{
			View:     node.View,
			Sequence: node.Seq,
			Digest:   digest,
			Status:   status,
			// Wal:      node.DB.GetWalCopy(),
			// Locks:    node.DB.GetAllLocks(),
		}
		log.Printf("Assigning seq %d to %v %s\n", node.Seq, signedClientReq.ClientRequest.Transaction, status)
		node.Seq = node.Seq + 1
		node.SeqLock.Unlock()

		signature, _ := cryptox.SignMessage(node.PrivateKey, prePrepare)
		preprepareReq = &pbft.PrePrepareRequest{
			PrePrepare: prePrepare,
			Signature:  signature,
			ClientReq:  signedClientReq,
		}
	} else {
		node.PrePreparesLock.Lock()
		preprepareReq = node.PrePrepares[oldSeq]
		node.PrePreparesLock.Unlock()
		prePrepare = preprepareReq.PrePrepare
	}
	log.Printf("Pre-prepare for seq %d: %v\n", prePrepare.Sequence, prePrepare)

	node.logClientRequest(signedClientReq, prePrepare.Sequence)
	node.writeLog(prePrepare.Sequence, preprepareReq.ClientReq.ClientRequest.Transaction, node.View, preprepareReq.PrePrepare.Digest, preprepareReq.ClientReq.ClientRequest.Timestamp.Seconds)
	node.savePrePrepare(prePrepare.Sequence, preprepareReq)
	if _, ok := node.PrepareCertChans[prePrepare.Sequence]; !ok {
		node.PrepareCertChans[prePrepare.Sequence] = make(chan *pbft.PrepareRequest)
	}

	if _, ok := node.CommitCertChans[prePrepare.Sequence]; !ok {
		node.CommitCertChans[prePrepare.Sequence] = make(chan *pbft.CommitRequest)
	}

	for nodeId, client := range node.ClientMap[node.ClusterId] {
		if nodeId != node.Id {
			log.Printf("Sending pre-prepare to %s for seq %d %v\n", nodeId, prePrepare.Sequence, signedClientReq.ClientRequest.Transaction)
			go client.PrePrepare(context.Background(), preprepareReq)
		}
	}
	err = node.collectPrepares(prePrepare.Sequence)
	if err != nil {
		log.Printf("Error collecting prepares: %v\n", err)
		node.DB.ReleaseLocksDB([]string{signedClientReq.ClientRequest.Transaction.From, signedClientReq.ClientRequest.Transaction.To})
		return nil, err
	}
	prepared := node.validatePrepares(prePrepare.Sequence, node.PrepareCerts[prePrepare.Sequence])
	if prepared {
		log.Printf("Prepare done for seq: %d\n", prePrepare.Sequence)

		go node.sendCommitToSelf(prePrepare.Sequence)
		res := node.collectCommits(prePrepare.Sequence, status, signedClientReq.ClientRequest.Transaction.ClientSeq, signedClientReq.ClientRequest.Transaction)
		if res {
			return preprepareReq, nil
		} else {
			node.DB.ReleaseLocksDB([]string{signedClientReq.ClientRequest.Transaction.From, signedClientReq.ClientRequest.Transaction.To})
			return preprepareReq, fmt.Errorf("seq %d pbft run failed not committed", signedClientReq.ClientRequest.Transaction.ClientSeq)
		}
	} else {
		node.DB.ReleaseLocksDB([]string{signedClientReq.ClientRequest.Transaction.From, signedClientReq.ClientRequest.Transaction.To})
		return preprepareReq, fmt.Errorf("seq %d pbft run failed not prepared", signedClientReq.ClientRequest.Transaction.ClientSeq)
	}
}

func (node *Node) sendCommitToSelf(seq int64) {
	log.Printf("Sending commit to self\n")
	commit := &pbft.Commit{
		Sequence: seq,
		View:     node.View,
		Digest:   node.TransactionLog[seq].Digest,
		NodeId:   node.Id,
	}
	signature, err := cryptox.SignMessage(node.PrivateKey, commit)
	if err != nil {
		log.Printf("Failed to sign")
	}

	commitReq := &pbft.CommitRequest{
		Commit:    commit,
		Signature: signature,
	}
	time.Sleep(50 * time.Millisecond)
	_, err = node.Commit(context.Background(), commitReq)
	if err != nil {
		log.Printf("error response for commit: %v\n", err)
	}
}

func (node *Node) collectPrepares(seq int64) error {
	timer := time.NewTimer(400 * time.Millisecond)
	defer timer.Stop()
	doneReading := false
	var certs []*pbft.PrepareRequest
	for {
		if doneReading {
			break
		}
		select {
		case prepare := <-node.PrepareCertChans[seq]:
			certs = append(certs, prepare)
			if len(certs) == 2*config.F {
				afterMajorityTimer := time.NewTimer(50 * time.Millisecond)
				for {
					if doneReading {
						break
					}
					select {
					case prepare := <-node.PrepareCertChans[seq]:
						certs = append(certs, prepare)
					case <-afterMajorityTimer.C:
						log.Printf("Prepare waited 50ms after majority. Time: %d\n", time.Since(node.TimeKeeper).Milliseconds())
						doneReading = true
					}
				}

			}
		case <-timer.C:
			log.Printf("Done collecting prepares for seq: %d\n", seq)
			doneReading = true
		}
	}
	node.PrepareCertsLock.Lock()
	defer node.PrepareCertsLock.Unlock()
	node.PrepareCerts[seq] = certs
	return nil
}

func (node *Node) collectCommits(seq int64, status string, clientSeq int64, transaction *pbft.Transaction) bool {
	timer := time.NewTimer(400 * time.Millisecond)
	defer timer.Stop()
	doneReading := false
	var certs []*pbft.CommitRequest
	for {
		if doneReading {
			break
		}
		select {
		case commit := <-node.CommitCertChans[seq]:
			certs = append(certs, commit)
			if len(certs) == 2*config.F+1 {
				afterMajorityTimer := time.NewTimer(50 * time.Millisecond)
				for {
					if doneReading {
						break
					}
					select {
					case commit := <-node.CommitCertChans[seq]:
						certs = append(certs, commit)
					case <-afterMajorityTimer.C:
						log.Printf("Commit waited 50ms after majority. Time: %d\n", time.Since(node.TimeKeeper).Milliseconds())
						doneReading = true
					}
				}

			}
		case <-timer.C:
			log.Printf("Done collecting commits for seq: %d\n", seq)
			doneReading = true
		}
	}
	node.PrepareCertsLock.Lock()
	defer node.PrepareCertsLock.Unlock()
	node.CommitCerts[seq] = certs //append(node.CommitCerts[seq], certs...)
	return node.validateCommits(seq, certs, status, clientSeq, transaction)
}

func (node *Node) sendPrepareReq(seq int64, preprepareReq *pbft.PrePrepareRequest, leader, status string) error {
	// if node.Byzantine {
	// 	log.Printf("Byzantine. Not sending prepare")
	// 	return errors.New("byzantine, not sending prepare")
	// }

	var prepare *pbft.Prepare

	if !node.Byzantine {
		prepare = &pbft.Prepare{
			View:     preprepareReq.PrePrepare.View,
			Sequence: preprepareReq.PrePrepare.Sequence,
			Digest:   preprepareReq.PrePrepare.Digest,
			NodeId:   node.Id,
		}
		log.Printf("Sending prepare View: %d Seq: %d Digest: %v to %s\n", prepare.View, prepare.Sequence, preprepareReq.PrePrepare.Digest, leader)
	} else {
		prepare = &pbft.Prepare{
			View:     preprepareReq.PrePrepare.View,
			Sequence: preprepareReq.PrePrepare.Sequence,
			Digest:   "",
			NodeId:   node.Id,
		}
	}
	signature, err := cryptox.SignMessage(node.PrivateKey, prepare)
	if err != nil {
		node.DB.ReleaseLocksDB([]string{preprepareReq.ClientReq.ClientRequest.Transaction.From, preprepareReq.ClientReq.ClientRequest.Transaction.To})
		return err
	}
	prepareReq := &pbft.PrepareRequest{
		Prepare:   prepare,
		Signature: signature,
	}

	prepareCerts, err := node.ClientMap[node.ClusterId][leader].Prepare(context.Background(), prepareReq)
	if err != nil {
		node.DB.ReleaseLocksDB([]string{preprepareReq.ClientReq.ClientRequest.Transaction.From, preprepareReq.ClientReq.ClientRequest.Transaction.To})
		log.Printf("Error receiving prepare certs: %v\n", err)
		return err
	}

	node.PrepareCertsLock.Lock()
	node.PrepareCerts[seq] = prepareCerts.PrepareRequests
	node.PrepareCertsLock.Unlock()
	prepared := node.validatePrepares(seq, prepareCerts.PrepareRequests)
	if prepared {
		node.sendCommitReq(seq, status, preprepareReq.ClientReq.ClientRequest.Transaction.ClientSeq, preprepareReq.ClientReq.ClientRequest.Transaction)
	}
	return nil
}

func (node *Node) sendCommitReq(seq int64, status string, clientSeq int64, transaction *pbft.Transaction) error {
	transactionLog, ok := node.getTransactionLog(seq)
	if !ok {
		return fmt.Errorf("no transaction at seq: %d", seq)
	}
	leader := node.getViewLeader()
	var commit *pbft.Commit
	if !node.Byzantine {
		commit = &pbft.Commit{
			View:     node.View,
			Sequence: seq,
			Digest:   transactionLog.Digest,
			NodeId:   node.Id,
		}
		log.Printf("Sending commit View: %d Seq: %d to %s\n", commit.View, commit.Sequence, leader)
	} else {
		commit = &pbft.Commit{
			View:     node.View,
			Sequence: seq,
			Digest:   "",
			NodeId:   node.Id,
		}
	}

	signature, err := cryptox.SignMessage(node.PrivateKey, commit)
	if err != nil {
		return err
	}
	commitReq := &pbft.CommitRequest{
		Commit:    commit,
		Signature: signature,
	}

	commitCerts, err := node.ClientMap[node.ClusterId][leader].Commit(context.Background(), commitReq)
	if err != nil {
		log.Printf("Error receiving commit certs: %v\n", err)
		return err
	}

	node.CommitCertsLock.Lock()
	node.CommitCerts[seq] = commitCerts.CommitRequests
	node.CommitCertsLock.Unlock()

	committed := node.validateCommits(seq, commitCerts.CommitRequests, status, clientSeq, transaction)
	if committed && !node.Byzantine {
		log.Printf("Committed seq: %d\n", seq)
	}
	return nil
}

func (node *Node) validatePrepares(seq int64, prepares []*pbft.PrepareRequest) bool {
	certs := node.getValidPrepareCerts(seq, prepares)
	log.Printf("Received %d prepare certs for seq: %d\n", len(certs), seq)
	if len(certs) >= (2 * config.F) {
		if !node.Byzantine {
			node.updateLog(seq, "prepared")
		}
		node.PrepareCertsLock.Lock()
		node.PrepareCerts[seq] = certs
		node.PrepareCertsLock.Unlock()
		return true
	}
	return false
}

func (node *Node) validateCommits(seq int64, commits []*pbft.CommitRequest, status string, clientSeq int64, transaction *pbft.Transaction) bool {
	certs := node.getValidCommitCerts(seq, commits)
	log.Printf("Received %d commit certs for seq: %d\n", len(certs), seq)
	if len(certs) >= (2*config.F + 1) {
		if !node.Byzantine {
			node.updateLog(seq, "committed")
			if status == "P" {
				node.DB.AppendToWALEntry(clientSeq, []db.WALLog{
					{
						Operation:   "transaction",
						Transaction: transaction,
					},
				})
			}
			node.saveTransactionToDatastore(seq, status, clientSeq)
		}
		node.CommitCertsLock.Lock()
		node.CommitCerts[seq] = certs
		node.CommitCertsLock.Unlock()

		go node.saveTransaction(seq, status)
		return true
	}
	return false
}

func (node *Node) getValidPrepareCerts(seq int64, prepares []*pbft.PrepareRequest) []*pbft.PrepareRequest {
	var certs []*pbft.PrepareRequest
	transactionLog, ok := node.getTransactionLog(seq)
	if !ok {
		log.Printf("No transaction int log at seq: %d\n", seq)
		return certs
	}
	for i, signedPrepare := range prepares {
		pubKey, err := node.getPublicKey(signedPrepare.Prepare.NodeId)
		if err != nil {
			log.Printf("%v\n", err)
			continue
		}
		if err := cryptox.VerifySignature(pubKey, signedPrepare.Prepare, signedPrepare.Signature); err != nil {
			log.Printf("%d: invalid signature: %v\n", i, err)
			continue
		}

		if transactionLog.Digest != signedPrepare.Prepare.Digest {
			log.Printf("%d: digest or view does not match with pre-prepare\n", i)
			continue
		}
		certs = append(certs, signedPrepare)
	}

	return certs
}

func (node *Node) getValidCommitCerts(seq int64, commits []*pbft.CommitRequest) []*pbft.CommitRequest {
	var certs []*pbft.CommitRequest
	transactionLog, ok := node.getTransactionLog(seq)
	if !ok {
		log.Printf("No transaction in log at seq %d\n", seq)
		return certs
	}
	for i, signedCommit := range commits {
		pubKey, err := node.getPublicKey(signedCommit.Commit.NodeId)
		if err != nil {
			log.Printf("%v\n", err)
			continue
		}
		if err := cryptox.VerifySignature(pubKey, signedCommit.Commit, signedCommit.Signature); err != nil {
			log.Printf("%d: invalid signature: %v\n", i, err)
			continue
		}

		if transactionLog.State < config.States["prepared"] {
			log.Printf("Sequence %d in log is not in prepared (or later) stage\n", seq)
			continue
		}
		if transactionLog.Digest != signedCommit.Commit.Digest {
			log.Printf("%d: digest or view does not match with prepared\n", i)
			continue
		}
		certs = append(certs, signedCommit)
	}

	return certs
}

func (node *Node) logClientRequest(signedClientReq *pbft.SignedClientRequest, seq int64) {
	log.Printf("Logging client req %d\n", signedClientReq.ClientRequest.Timestamp.Seconds)
	node.RequestLogLock.Lock()
	defer node.RequestLogLock.Unlock()

	node.RequestLog[signedClientReq.ClientRequest.Timestamp.Seconds] = RequestData{
		Seq:      seq,
		Executed: false,
		Result:   false,
	}
}

func (node *Node) replyToClient(timestamp int64, result bool, clientSeq int64) {
	log.Printf("Replying to client for %d %t\n", timestamp, result)
	reply := &clientrpc.Reply{
		View: node.View,
		Timestamp: &timestamppb.Timestamp{
			Seconds: timestamp,
		},
		Server:    node.Id,
		Result:    result,
		Clientseq: clientSeq,
	}

	signature, err := cryptox.SignMessage(node.PrivateKey, reply)
	if err != nil {
		log.Printf("could not sign reply with timestamp %v\n", timestamp)
		return
	}

	signedReply := &clientrpc.SignedReply{
		Reply:     reply,
		Signature: signature,
	}

	node.ClientgrpcClient.Reply(context.Background(), signedReply)
}

// func (node *Node) sendCheckpoints(signedReq *pbft.SignedCheckpointRequest) {
// 	for nodeId, grpcClient := range node.ClientMap {
// 		log.Printf("Sending checkpoint seq %d to %s\n", signedReq.Checkpoint.Sequence, nodeId)
// 		go grpcClient.Checkpoint(context.Background(), signedReq)
// 	}

// 	// node.CheckpointCertsLock.Lock()
// 	// if checkpointData, ok := node.CheckpointCerts[signedReq.Checkpoint.Sequence]; ok {
// 	// 	node.CheckpointCerts[signedReq.Checkpoint.Sequence] = CheckpointData{
// 	// 		Stable: checkpointData.Stable,
// 	// 		Digest: checkpointData.Digest,
// 	// 	}
// 	// }
// 	// node.CheckpointCertsLock.Unlock()
// }

func (node *Node) sendViewChange(view int64) {
	if !node.Live {
		return
	}

	if view <= node.View {
		return
	}

	if node.ViewChanging && node.NewViewNum >= view {
		log.Printf("View change already sent for view %d\n", view)
		return
	}
	node.Tmr.Stop()
	node.ViewChanging = true
	node.NewViewNum = view

	var preparedMessages []*pbft.PreparedMessages

	for i := node.LastStable + 1; i <= node.MaxSeq; i++ {
		if preparedReq, ok := node.TransactionLog[i]; ok && preparedReq.State >= 2 {
			preparedMessage := &pbft.PreparedMessages{
				PrePrepare: node.PrePrepares[i],
				Certs:      node.PrepareCerts[i],
			}
			preparedMessages = append(preparedMessages, preparedMessage)
		}
	}

	viewChangeReq := &pbft.ViewChangeRequest{
		View:                 view,
		LastStableCheckpoint: node.LastStable,
		CheckpointCerts:      node.CheckpointCerts[node.LastStable].Certs,
		Prepared:             preparedMessages,
		NodeId:               node.Id,
		State:                convertBalancesToProto(node.CheckpointCerts[node.LastStable].Balances),
	}

	signature, _ := cryptox.SignMessage(node.PrivateKey, viewChangeReq)

	signedReq := &pbft.SignedViewChangeRequest{
		ViewChangeRequest: viewChangeReq,
		Signature:         signature,
	}

	for nodeId, client := range node.ClientMap[node.ClusterId] {
		log.Printf("Sending view change %d to %s\n", view, nodeId)
		go client.ViewChange(context.Background(), signedReq)
	}

	node.ViewChangeCertsLock.Lock()
	node.ViewChangeCerts[view] = append(node.ViewChangeCerts[view], signedReq)
	// log.Printf("Appened myself: new length %d\n", len(node.ViewChangeCerts[view]))
	node.ViewChangeCertsLock.Unlock()
}

func (node *Node) sendNewView(view int64, certs []*pbft.SignedViewChangeRequest) {

	if !node.Live {
		return
	}

	if node.Byzantine {
		log.Printf("Byzantine. Not sending new view")
		return
	}
	if _, ok := node.NewViewsSent.Load(view); ok {
		return
	}

	node.View = view

	node.NewViewsSent.Store(view, true)
	latestStableCheckpoint := int64(0)
	state := convertBalancesToProto(node.LastStableBalances)
	var checkpointCerts []*pbft.SignedCheckpointRequest
	for _, viewChangeReq := range certs {
		if viewChangeReq.ViewChangeRequest.LastStableCheckpoint > int64(latestStableCheckpoint) {
			latestStableCheckpoint = viewChangeReq.ViewChangeRequest.LastStableCheckpoint
			state = viewChangeReq.ViewChangeRequest.State
			checkpointCerts = viewChangeReq.ViewChangeRequest.CheckpointCerts
		}
	}

	prePrepares := make(map[int64]*pbft.PrePrepareRequest)
	var prepSeqs []int64
	for _, viewChangeReq := range certs {
		for _, prepared := range viewChangeReq.ViewChangeRequest.Prepared {
			if _, ok := prePrepares[prepared.PrePrepare.PrePrepare.Sequence]; !ok {
				prePrepares[prepared.PrePrepare.PrePrepare.Sequence] = prepared.PrePrepare
				prepSeqs = append(prepSeqs, prepared.PrePrepare.PrePrepare.Sequence)
			}
		}
	}
	sort.Slice(prepSeqs, func(i, j int) bool {
		return prepSeqs[i] < prepSeqs[j]
	})

	var prePreparesList []*pbft.PrePrepareRequest
	if len(prepSeqs) > 0 {
		for i := prepSeqs[0]; i <= prepSeqs[len(prepSeqs)-1]; i++ {
			if prePrepare, ok := prePrepares[i]; ok {
				prep := &pbft.PrePrepare{
					Sequence: i,
					View:     view,
					Digest:   prePrepare.PrePrepare.Digest,
				}
				signature, _ := cryptox.SignMessage(node.PrivateKey, prep)
				prepReq := &pbft.PrePrepareRequest{
					PrePrepare: prep,
					Signature:  signature,
				}
				prePreparesList = append(prePreparesList, prepReq)
			} else {
				prep := &pbft.PrePrepare{
					Sequence: i,
					View:     view,
					Digest:   "",
				}
				signature, _ := cryptox.SignMessage(node.PrivateKey, prep)
				prepReq := &pbft.PrePrepareRequest{
					PrePrepare: prep,
					Signature:  signature,
				}
				prePreparesList = append(prePreparesList, prepReq)
			}
		}
	}

	// log.Printf("States for sequence: %d %v\n", latestStableCheckpoint, state)
	newView := &pbft.NewViewRequest{
		View:                   view,
		ViewChangeCerts:        certs,
		Prepared:               prePreparesList,
		State:                  state,
		LatestStableCheckpoint: latestStableCheckpoint,
		CheckpointCerts:        checkpointCerts,
	}

	signature, err := cryptox.SignMessage(node.PrivateKey, newView)
	if err != nil {
		log.Printf("Failed to sign new view message: %v\n", err)
	}

	signedNewView := &pbft.SignedNewViewRequest{
		NewView:   newView,
		Signature: signature,
	}

	for nodeId, client := range node.ClientMap[node.ClusterId] {
		log.Printf("Sending new view to %s\n", nodeId)
		go client.NewView(context.Background(), signedNewView)
	}

	if len(signedNewView.NewView.Prepared) > 0 {
		node.Seq = signedNewView.NewView.Prepared[len(signedNewView.NewView.Prepared)-1].PrePrepare.Sequence + 1
	} else {
		node.Seq = signedNewView.NewView.LatestStableCheckpoint + 1
	}
	node.ExecutorSeq = signedNewView.NewView.LatestStableCheckpoint + 1

	node.ViewChanging = false
	if len(prepSeqs)-1 >= 0 {
		node.Seq = prepSeqs[len(prepSeqs)-1] + 1
	}
	node.Tmr.Stop()
}

func (node *Node) validateViewChangeCerts(certs []*pbft.SignedViewChangeRequest) error {
	// var latestState *pbft.BalanceStructList
	// latestState := convertBalancesToProto(node.LastStableBalances)
	latestStableCP := node.LastStable
	valid := 0
	for _, cert := range certs {
		pubKey, _ := node.getPublicKey(cert.ViewChangeRequest.NodeId)
		err := cryptox.VerifySignature(pubKey, cert.ViewChangeRequest, cert.Signature)
		if err != nil {
			log.Printf("Could not verify signature of view change request\n")
			continue
		}

		if cert.ViewChangeRequest.LastStableCheckpoint > 0 {
			err = node.validateCheckpointCerts(cert.ViewChangeRequest.LastStableCheckpoint, cert.ViewChangeRequest.CheckpointCerts, node.convertProtoBalancesToMap(cert.ViewChangeRequest.State))
			if err != nil {
				log.Printf("View change checkpoint certs verification failed: %v\n", err)
				return fmt.Errorf("view change checkpoint certs verification failed: %v", err)
			}
		}

		if cert.ViewChangeRequest.LastStableCheckpoint > node.LastStable {
			latestStableCP = cert.ViewChangeRequest.LastStableCheckpoint
			// latestState = cert.ViewChangeRequest.State
		}
		valid += 1
	}

	if valid >= 2*config.F+1 {
		if latestStableCP >= node.LastStable {
			// node.BalancesLock.Lock()
			// node.Balances = node.convertProtoBalancesToMap(latestState)
			// node.LastStable = latestStableCP
			// // for nodeId, balance := range latestState {
			// // 	node.Balances[nodeId] = balance
			// // }
			// node.BalancesLock.Unlock()
		}
		return nil
	}
	return errors.New("failed to validate view change messages")
}

func (node *Node) validateCheckpointCerts(seq int64, signedReqs []*pbft.SignedCheckpointRequest, state map[string]int64) error {
	if seq == 0 {
		return nil
	}
	var checkpointCerts []*pbft.SignedCheckpointRequest
	digest, _ := cryptox.MD5Hash(state)
	for _, signedReq := range signedReqs {
		pubKey, _ := node.getPublicKey(signedReq.Checkpoint.NodeId)
		err := cryptox.VerifySignature(pubKey, signedReq.Checkpoint, signedReq.Signature)
		if err != nil {
			log.Printf("Signature of checkpoint cert does not match")
		} else if digest != signedReq.Checkpoint.Digest {
			log.Printf("Digest of checkpoint in new view does not match")
		} else {
			checkpointCerts = append(checkpointCerts, signedReq)
		}
	}

	if len(checkpointCerts) >= 2*config.F+1 {
		node.CheckpointCertsLock.Lock()
		node.CheckpointCerts[seq] = CheckpointData{
			Stable:   true,
			Digest:   checkpointCerts[0].Checkpoint.Digest,
			Balances: state,
			Certs:    checkpointCerts,
		}
		return nil
	} else {
		return errors.New("checkpoint certs validation failed")
	}
}
