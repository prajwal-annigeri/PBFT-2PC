package main

import (
	"2pcbyz-prajwal/cryptox"
	"2pcbyz-prajwal/gateway/clientrpc"
	"context"
	"errors"
	"log"

	"google.golang.org/protobuf/types/known/emptypb"
)

type ReplyResult struct {
	Timestamp int64
	Result    bool
	Server    string
	View      int64
	ClientSeq int64
}

func (gateway *Gateway) Reply(ctx context.Context, reply *clientrpc.SignedReply) (*emptypb.Empty, error) {
	log.Printf("Received reply: %v\n", reply.Reply)
	pubKey, err := gateway.getPublicKey(reply.Reply.Server)
	if err != nil {
		log.Printf("Failed to fetch public key of %s\n", reply.Reply.Server)
		return &emptypb.Empty{}, errors.New("no public key found")
	}

	err = cryptox.VerifySignature(pubKey, reply.Reply, reply.Signature)
	if err != nil {
		log.Printf("Failed to verify signature of reply timestamp: %d from server %s\n", reply.Reply.Timestamp.Seconds, reply.Reply.Server)
		return &emptypb.Empty{}, errors.New("signature verification failed")
	}

	// gateway.View = max(gateway.View, reply.Reply.View)

	res := ReplyResult{
		Timestamp: reply.Reply.Timestamp.Seconds,
		Result:    reply.Reply.Result,
		Server:    reply.Reply.Server,
		View:      reply.Reply.View,
		ClientSeq: reply.Reply.Clientseq,
	}
	gateway.ResultsChan <- res
	return &emptypb.Empty{}, nil
}
