package node

import (
	"2pcbyz-prajwal/config"
	"2pcbyz-prajwal/cryptox"
	"2pcbyz-prajwal/pbft"
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/golang/protobuf/ptypes/wrappers"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (node *Node) ExecuteTransaction(ctx context.Context, signedClientReq *pbft.SignedClientRequest) (*emptypb.Empty, error) {
	if !node.Live {
		log.Printf("I'm not alive. Received client request")
		return &emptypb.Empty{}, nil
	}

	if node.ViewChanging {
		log.Printf("I'm changing view. Received ExecuteTransaction")
		go node.Tmr.StartIfNotRunning()
		return &emptypb.Empty{}, nil
	}

	log.Printf("Received request %s\n", signedClientReq.ClientRequest)
	pubKey, ok := node.PublicKeyMap.Load("client")
	if !ok {
		log.Printf("")
		return &emptypb.Empty{}, errors.New("no public key found")
	}

	err := cryptox.VerifySignature(pubKey.(*rsa.PublicKey), signedClientReq.ClientRequest, signedClientReq.Signature)
	if err != nil {
		return &emptypb.Empty{}, errors.New("could not verify signature")
	}

	node.RequestLogLock.RLock()
	requestData, ok := node.RequestLog[signedClientReq.ClientRequest.Timestamp.Seconds]
	node.RequestLogLock.RUnlock()
	if ok && requestData.Executed {
		log.Printf("Already have computed result for timestamp %v\n", signedClientReq.ClientRequest.Timestamp.Seconds)
		node.replyToClient(signedClientReq.ClientRequest.Timestamp.Seconds, requestData.Result, signedClientReq.ClientRequest.Transaction.ClientSeq)
		return &emptypb.Empty{}, nil
	} else {
		go node.Tmr.StartIfNotRunning()

		leader := node.getViewLeader()
		if leader != node.Id {
			log.Printf("View %d, forwarding to %s\n", node.View, leader)
			go node.ClientMap[node.ClusterId][leader].ExecuteTransaction(ctx, signedClientReq)
			return &emptypb.Empty{}, nil
		}
		seq := int64(0)
		if ok {
			log.Printf("Old request Seq: %d\n", requestData.Seq)
			seq = requestData.Seq
		}

		transaction := signedClientReq.ClientRequest.Transaction
		if transaction.ClusterFrom == transaction.ClusterTo {
			node.runIntrashard(signedClientReq, seq)
		} else {
			node.runCrossShard(signedClientReq, seq)
		}
	}

	return &emptypb.Empty{}, nil
}

func (node *Node) SetStatus(ctx context.Context, live *wrappers.BoolValue) (*wrappers.BoolValue, error) {
	node.Live = live.GetValue()
	log.Printf("Alive: %t\n", node.Live)
	return &wrappers.BoolValue{Value: node.Live}, nil
}

func (node *Node) SetByzantine(ctx context.Context, byzantine *wrappers.BoolValue) (*wrappers.BoolValue, error) {
	node.Byzantine = byzantine.GetValue()
	log.Printf("Byzantine: %t\n", node.Byzantine)
	return &wrappers.BoolValue{Value: node.Byzantine}, nil
}

func (node *Node) PrePrepare(ctx context.Context, preprepareReq *pbft.PrePrepareRequest) (*emptypb.Empty, error) {

	if node.ViewChanging {
		log.Printf("I'm changing view. Received pre-prepare")
		go node.Tmr.StartIfNotRunning()
		return &emptypb.Empty{}, nil
	}

	if !node.Live {
		log.Printf("I'm not alive. Received pre-prepare")
		return &emptypb.Empty{}, nil
	}
	// Checking if same view
	seq := preprepareReq.PrePrepare.Sequence
	if node.View != preprepareReq.PrePrepare.View {
		log.Printf("PrePrepare View does not match")
		return &emptypb.Empty{}, errors.New("view does not match")
	}

	if preprepareReq.PrePrepare.Sequence != 1 {
		if !node.checkPrePrepare(preprepareReq.PrePrepare.Sequence - 1) {
			log.Printf("Haven't received all pre-prepares. Rejecting seq: %d\n", preprepareReq.PrePrepare.Sequence)
			return &emptypb.Empty{}, errors.New("haven't received all previous preprepares")
		}
	}

	// Checking if tLog has same view, sequence with different digest
	node.TransactionLogLock.RLock()
	tLog, ok := node.TransactionLog[seq]
	node.TransactionLogLock.RUnlock()

	if ok {
		if tLog.View == preprepareReq.PrePrepare.View && tLog.Digest != preprepareReq.PrePrepare.Digest {
			log.Printf("PrePrepare different digest in log for same view. Seq: %d\n", seq)
			return &emptypb.Empty{}, errors.New("different digest in log for same view")
		}
	}

	// Checking digest matches client message
	computedDigest, err := cryptox.MD5Hash(preprepareReq.ClientReq)
	if err != nil {
		log.Printf("PrePrepare Digest computation failed: %v\n", err)
		return &emptypb.Empty{}, fmt.Errorf("digest computation failed: %v", err)
	}
	if computedDigest != preprepareReq.PrePrepare.Digest {
		log.Printf("PrePrepare digest does not match the message: %s\n", err)
		return &emptypb.Empty{}, errors.New("digest does not match the message")
	}

	// Checking client signature
	pubKey, ok := node.PublicKeyMap.Load("client")
	if !ok {
		log.Printf("No public key found for client\n")
		return &emptypb.Empty{}, fmt.Errorf("no public key found for client")
	}
	if err := cryptox.VerifySignature(pubKey.(*rsa.PublicKey), preprepareReq.ClientReq.ClientRequest, preprepareReq.ClientReq.Signature); err != nil {
		log.Printf("PrePrepare Failed to verify client signature: %v", err)
		return &emptypb.Empty{}, errors.New("failed to verify client signature")
	}

	// Checking leader signature
	leader := node.getViewLeader()
	pubKey, _ = node.PublicKeyMap.Load(leader)
	if err = cryptox.VerifySignature(pubKey.(*rsa.PublicKey), preprepareReq.PrePrepare, preprepareReq.Signature); err != nil {
		return &emptypb.Empty{}, errors.New("failed to verify pre-prepare signature")
	}
	go node.Tmr.StartIfNotRunning()
	node.RequestLogLock.RLock()
	_, ok = node.RequestLog[preprepareReq.ClientReq.ClientRequest.Timestamp.Seconds]
	node.RequestLogLock.RUnlock()

	if !ok {
		node.logClientRequest(preprepareReq.ClientReq, preprepareReq.PrePrepare.Sequence)
	}
	transaction := preprepareReq.ClientReq.ClientRequest.Transaction
	log.Printf("PrePrepare received View: %d Seq: %d Transaction: %v Timestamp: %d\n", preprepareReq.PrePrepare.View, preprepareReq.PrePrepare.Sequence, preprepareReq.ClientReq.ClientRequest.Transaction, preprepareReq.ClientReq.ClientRequest.Timestamp.Seconds)

	if preprepareReq.PrePrepare.Status == "" || preprepareReq.PrePrepare.Status == "P" {
		for {
			node.LockLock.Lock()
			err = node.DB.CheckAndGetLocks([]string{transaction.From, transaction.To}, node.Byzantine, preprepareReq.ClientReq.ClientRequest.Transaction.ClientSeq)
			node.LockLock.Unlock()
			if err != nil {
				log.Printf("Could not secure locks: %v\n", err)
				log.Printf("Retrying")
			} else {
				log.Printf("Secured locks for seq: %d\n", transaction.ClientSeq)
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	node.writeLog(seq, preprepareReq.ClientReq.ClientRequest.Transaction, node.View, preprepareReq.PrePrepare.Digest, preprepareReq.ClientReq.ClientRequest.Timestamp.Seconds)
	node.savePrePrepare(seq, preprepareReq)
	go node.sendPrepareReq(seq, preprepareReq, leader, preprepareReq.PrePrepare.Status)
	return &emptypb.Empty{}, nil
}

func (node *Node) Prepare(ctx context.Context, prepareReq *pbft.PrepareRequest) (*pbft.PrepareResponse, error) {
	if !node.Live {
		log.Printf("I'm not alive. Received prepare")
		return &pbft.PrepareResponse{}, errors.New("i'm not alive")
	}

	if node.ViewChanging {
		log.Printf("I'm changing view. received prepare")
		return &pbft.PrepareResponse{}, nil
	}

	if node.Byzantine {
		log.Printf("Byzantine. I won't collect prepares")
		return &pbft.PrepareResponse{}, errors.New("i'm byzantine")
	}
	err := node.verifyPrepareRequest(prepareReq)
	prepareValid := true
	if err != nil {
		prepareValid = false
	}

	if prepareValid {
		log.Printf("Prepare received from %s. View: %d Seq: %d Digest: %s\n", prepareReq.Prepare.NodeId, prepareReq.Prepare.View, prepareReq.Prepare.Sequence, string(prepareReq.Prepare.Digest))
		node.PrepareCertChans[prepareReq.Prepare.Sequence] <- prepareReq
	}
	log.Printf("Waiting for prepares to send to %s\n", prepareReq.Prepare.NodeId)
	// time.Sleep(400 * time.Millisecond)
	prepWaitTimer := time.NewTimer(250 * time.Millisecond)
	defer prepWaitTimer.Stop()
	doneReading := false
	var prepareCerts []*pbft.PrepareRequest
	for {
		if doneReading {
			break
		}
		select {
		case <-prepWaitTimer.C:
			log.Printf("Prepare collection timer done Seq: %d Sending to node: %s\n", prepareReq.Prepare.Sequence, prepareReq.Prepare.NodeId)
			doneReading = true
		default:
			node.PrepareCertsLock.RLock()
			prepareCerts = node.PrepareCerts[prepareReq.Prepare.Sequence]
			node.PrepareCertsLock.RUnlock()
			if len(prepareCerts) >= 2*config.F {
				doneReading = true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(prepareCerts) >= 2*config.F {
		resp := &pbft.PrepareResponse{
			PrepareRequests: prepareCerts,
		}
		log.Printf("Sending valid prepare response to %s\n", prepareReq.Prepare.NodeId)
		return resp, nil
	}
	log.Printf("Sending not-prepared prepare response to %s\n", prepareReq.Prepare.NodeId)
	return &pbft.PrepareResponse{}, errors.New("not prepared")
}

func (node *Node) Commit(ctx context.Context, commitReq *pbft.CommitRequest) (*pbft.CommitResponse, error) {
	if !node.Live {
		log.Printf("I'm not alive. Received commit")
		return &pbft.CommitResponse{}, errors.New("i'm not alive")
	}

	if node.ViewChanging {
		log.Printf("I'm changing view. Received commit")
		return &pbft.CommitResponse{}, nil
	}

	err := node.verifyCommitRequest(commitReq)
	commitValid := true
	if err != nil {
		commitValid = false
		// return &pbft.CommitResponse{}, err
	}
	if commitValid {
		log.Printf("Commit received from %s. View: %d Seq: %d\n", commitReq.Commit.NodeId, commitReq.Commit.View, commitReq.Commit.Sequence)
		node.CommitCertChans[commitReq.Commit.Sequence] <- commitReq
	}
	log.Printf("Waiting for commits to send to %s\n", commitReq.Commit.NodeId)
	// log.Printf("%d ms\n", time.Since(node.TimeKeeper))
	commtiWaitTimer := time.NewTimer(400 * time.Millisecond)
	defer commtiWaitTimer.Stop()
	doneReading := false
	var commitCerts []*pbft.CommitRequest
	for {
		if doneReading {
			break
		}
		select {
		case <-commtiWaitTimer.C:
			log.Printf("Commit collection timer done Seq: %d Sending to node: %s\n", commitReq.Commit.Sequence, commitReq.Commit.NodeId)
			doneReading = true
		default:
			node.CommitCertsLock.RLock()
			commitCerts = node.CommitCerts[commitReq.Commit.Sequence]
			node.CommitCertsLock.RUnlock()
			if len(commitCerts) >= 2*config.F+1 {
				doneReading = true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(commitCerts) > 2*config.F {
		resp := &pbft.CommitResponse{
			CommitRequests: commitCerts,
		}
		log.Printf("Sending valid commit response to %s\n", commitReq.Commit.NodeId)
		return resp, nil
	}
	log.Printf("Sending not committed to %s\n", commitReq.Commit.NodeId)
	return &pbft.CommitResponse{}, errors.New("not committed")
}

func (node *Node) Checkpoint(ctx context.Context, signedRequest *pbft.SignedCheckpointRequest) (*emptypb.Empty, error) {
	if !node.Live {
		log.Printf("Not live. Received checkpoint request from %s\n", signedRequest.Checkpoint.NodeId)
		return &emptypb.Empty{}, errors.New("not live")
	}
	log.Printf("Checkpoint received from %s\n", signedRequest.Checkpoint.NodeId)
	err := node.validateCheckpoint(signedRequest)
	if err != nil {
		return &emptypb.Empty{}, err
	}
	return &emptypb.Empty{}, nil
}

func (node *Node) ViewChange(ctx context.Context, signedReq *pbft.SignedViewChangeRequest) (*emptypb.Empty, error) {
	pubKey, _ := node.getPublicKey(signedReq.ViewChangeRequest.NodeId)
	err := cryptox.VerifySignature(pubKey, signedReq.ViewChangeRequest, signedReq.Signature)
	if err != nil {
		log.Printf("Signature verification failed: %v\n", err)
		return &emptypb.Empty{}, err
	}
	node.ViewChangeCertsLock.Lock()
	node.ViewChangeCerts[signedReq.ViewChangeRequest.View] = append(node.ViewChangeCerts[signedReq.ViewChangeRequest.View], signedReq)
	log.Printf("Received view change: %d from %s\n", signedReq.ViewChangeRequest.View, signedReq.ViewChangeRequest.NodeId)
	viewChangeCerts := node.ViewChangeCerts[signedReq.ViewChangeRequest.View]
	node.ViewChangeCertsLock.Unlock()
	if node.View < signedReq.ViewChangeRequest.View && len(viewChangeCerts) == config.F+1 {
		log.Printf("Joining view change to %d\n", signedReq.ViewChangeRequest.View)
		// node.Tmr.StartOrReset()
		node.sendViewChange(signedReq.ViewChangeRequest.View)
	}

	log.Printf("Current view change reqs for view %d: %d\n", signedReq.ViewChangeRequest.View, len(viewChangeCerts))
	if signedReq.ViewChangeRequest.View > node.View && len(viewChangeCerts) >= (2*config.F+1) && node.getViewLeader() == node.Id {
		// log.Printf("Send new view haha")
		go node.sendNewView(signedReq.ViewChangeRequest.View, viewChangeCerts)
	}
	// log.Printf("After if")
	return &emptypb.Empty{}, nil
}

func (node *Node) NewView(ctx context.Context, signedNewView *pbft.SignedNewViewRequest) (*emptypb.Empty, error) {
	log.Printf("Received new view: %v with %d pre-prepares\n", signedNewView.NewView.View, len(signedNewView.NewView.Prepared))
	mapState := node.convertProtoBalancesToMap(signedNewView.NewView.State)
	node.Tmr.Stop()
	newLeader := node.getViewLeader()
	pubKey, err := node.getPublicKey(newLeader)
	if err != nil {
		log.Printf("Failed to get public key of new leader: %v", err)
		return &emptypb.Empty{}, fmt.Errorf("failed to get public key of new leader: %v", err)
	}
	err = cryptox.VerifySignature(pubKey, signedNewView.NewView, signedNewView.Signature)
	if err != nil {
		log.Printf("Failed to verify signature of new view from %s: %v", newLeader, err)
		return &emptypb.Empty{}, fmt.Errorf("failed to verify signature: %v", err)
	}

	err = node.validateViewChangeCerts(signedNewView.NewView.ViewChangeCerts)
	if err != nil {
		log.Printf("Failed to validate view change certs in new view: %v\n", err)
		return &emptypb.Empty{}, err
	}

	err = node.validateCheckpointCerts(signedNewView.NewView.LatestStableCheckpoint, signedNewView.NewView.CheckpointCerts, mapState)
	if err != nil {
		log.Printf("NewView() Stable checkpoint validation failed: %v\n", err)
		return &emptypb.Empty{}, nil
	}

	node.ViewChanging = false
	// node.BalancesLock.Lock()
	// node.Balances = mapState
	// node.BalancesLock.Unlock()

	node.View = signedNewView.NewView.View
	for _, prePrepare := range signedNewView.NewView.Prepared {
		node.Seq = max(node.Seq, prePrepare.PrePrepare.Sequence+1)
		// go node.runNewViewPbft(prePrepare)
	}

	node.NewViewCerts = append(node.NewViewCerts, signedNewView.NewView)
	node.LastStable = signedNewView.NewView.LatestStableCheckpoint
	if len(signedNewView.NewView.Prepared) > 0 {
		node.Seq = signedNewView.NewView.Prepared[len(signedNewView.NewView.Prepared)-1].PrePrepare.Sequence + 1
	} else {
		node.Seq = signedNewView.NewView.LatestStableCheckpoint + 1
	}

	node.ExecutorSeq = signedNewView.NewView.LatestStableCheckpoint + 1

	return &emptypb.Empty{}, nil
}

func (node *Node) GetBalance(ctx context.Context, req *pbft.GetBalanceRequest) (*pbft.GetBalanceResponse, error) {
	balance, err := node.DB.GetBalance(req.Client)
	if err != nil {
		return &pbft.GetBalanceResponse{Balance: 0}, err
	}
	return &pbft.GetBalanceResponse{Balance: balance}, nil
}

func (node *Node) GetStatus(ctx context.Context, req *pbft.GetStatusRequest) (*pbft.DataResponse, error) {
	seq := req.Seq
	if transactionLog, ok := node.TransactionLog[seq]; ok {
		return &pbft.DataResponse{
			Data: config.IntToStates[transactionLog.State],
		}, nil
	}
	return &pbft.DataResponse{}, fmt.Errorf("no transaction at seq %d in node %s", seq, node.Id)
}

func (node *Node) GetTransactionLog(context.Context, *emptypb.Empty) (*pbft.DataResponse, error) {
	node.TransactionLogLock.RLock()
	defer node.TransactionLogLock.RUnlock()
	var sortedSeqs []int64
	for seq := range node.TransactionLog {
		sortedSeqs = append(sortedSeqs, seq)
	}
	sort.Slice(sortedSeqs, func(i, j int) bool {
		return sortedSeqs[i] < sortedSeqs[j]
	})
	res := ""
	for _, seq := range sortedSeqs {
		tLog := node.TransactionLog[seq]
		res += fmt.Sprintf("Seq: %d (%s, %s, %d) State: %s View: %d Timestamp: %d\n", seq, tLog.Transaction.From, tLog.Transaction.To, tLog.Transaction.Amount, config.IntToStates[tLog.State], tLog.View, tLog.Timestamp)
	}
	return &pbft.DataResponse{Data: res}, nil
}

func (node *Node) PCPrepare(ctx context.Context, PCPrepareReq *pbft.PCPrepareReq) (*pbft.PCReply, error) {
	log.Printf("Received PCPrepare clientseq: %d with %d commits\n", PCPrepareReq.SignedClientReq.ClientRequest.Transaction.ClientSeq, len(PCPrepareReq.Commits))
	err := node.validatePCPCommits(PCPrepareReq)
	if err != nil {
		log.Printf("Failed to validate commits in PCPreepare: %v\n", err)
		return &pbft.PCReply{}, err
	}
	_, err = node.runPBFT(PCPrepareReq.SignedClientReq, 0, "P")
	if err == nil {
		return &pbft.PCReply{
			Success: true,
			Cluster: node.ClusterId,
			NodeId:  node.Id,
		}, nil
	} else {
		log.Printf("PCPrepare error: %v\n", err)
		go node.waitForPCCommit(PCPrepareReq.SignedClientReq)
		return &pbft.PCReply{}, err
	}
}

func (node *Node) waitForPCCommit(signedClientReq *pbft.SignedClientRequest) {
	time.Sleep(1 * time.Second)
	_, ok := node.PCCommitsMap.Load(signedClientReq.ClientRequest.Transaction.ClientSeq)
	if !ok {
		node.runPBFT(signedClientReq, 0, "A")
	}
}

func (node *Node) PCCommit(ctx context.Context, PCCommitReq *pbft.PCPrepareReq) (*pbft.PCReply, error) {
	log.Printf("Received PCCommit clientseq: %d\n", PCCommitReq.SignedClientReq.ClientRequest.Transaction.ClientSeq)
	node.PCCommitsMap.Store(PCCommitReq.SignedClientReq.ClientRequest.Transaction.ClientSeq, true)
	_, err := node.runPBFT(PCCommitReq.SignedClientReq, 0, "C")
	if err == nil {
		return &pbft.PCReply{
			Success: true,
			Cluster: node.ClusterId,
			NodeId:  node.Id,
		}, nil
	} else {
		log.Printf("PCCommit error: %v\n", err)
		return &pbft.PCReply{}, err
	}
}

func (node *Node) GetDatastore(ctx context.Context, empty *emptypb.Empty) (*pbft.Datastore, error) {
	blocks := node.GetAllDatastore()
	var datablocks []*pbft.DatastoreData
	for _, dbUnit := range blocks {
		datablocks = append(datablocks, &pbft.DatastoreData{
			Seq:         dbUnit.Seq,
			Transaction: dbUnit.Transaction,
			Status:      dbUnit.Status,
		})
	}
	return &pbft.Datastore{
		Data: datablocks,
	}, nil
}

func (node *Node) validatePCPCommits(PCPrepareReq *pbft.PCPrepareReq) error {
	clientPubKey, err := node.getPublicKey("client")
	if err != nil {
		log.Printf("Unable to get client public key\n")
		return errors.New("unable to get client public key")
	}
	err = cryptox.VerifySignature(clientPubKey, PCPrepareReq.SignedClientReq.ClientRequest, PCPrepareReq.SignedClientReq.Signature)
	if err != nil {
		log.Printf("Unable to verify client signature\n")
		return errors.New("unable to verify client signature")
	}
	digest, err := cryptox.MD5Hash(PCPrepareReq.SignedClientReq)
	if err != nil {
		log.Printf("Unable to create digest after PCPrepare seq: %d %v\n", PCPrepareReq.SignedClientReq.ClientRequest.Transaction.ClientSeq, err)
		return fmt.Errorf("unable to create digest after pcprepare seq: %d %v", PCPrepareReq.SignedClientReq.ClientRequest.Transaction.ClientSeq, err)
	}
	cnt := 0
	log.Printf("Generated digest: %v\n", digest)
	for _, commit := range PCPrepareReq.Commits {
		log.Printf("Checking commit, digestin commit: %v\n", commit.Commit.Digest)
		if digest == commit.Commit.Digest {
			pubKey, err := node.getPublicKey(commit.Commit.NodeId)
			if err != nil {
				log.Printf("Failed to get public key of %s\n", commit.Commit.NodeId)
				continue
			}
			if err := cryptox.VerifySignature(pubKey, commit.Commit, commit.Signature); err != nil {
				log.Printf("Signature invalid from %s\n", commit.Commit.NodeId)
				continue
			}
			cnt++
		}
	}
	if cnt >= 2 * config.F + 1 {
		return nil
	}
	return fmt.Errorf("insufficient commits in pcprepare: %d", cnt)
}
