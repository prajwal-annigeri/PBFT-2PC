package main

import (
	"2pcbyz-prajwal/cryptox"
	"2pcbyz-prajwal/pbft"
	"log"

	"google.golang.org/protobuf/types/known/timestamppb"
)


func (gateway *Gateway) getSignedClientReq(transaction *pbft.Transaction, timestamp int64) *pbft.SignedClientRequest {
	clientReq := &pbft.ClientRequest{
		Transaction: transaction,
		Timestamp: &timestamppb.Timestamp{
			Seconds: timestamp,
		},
	}

	signature, err := cryptox.SignMessage(gateway.PrivateKey, clientReq)
	if err != nil {
		log.Printf("Failed to sign message: %v\n", err)
	}

	signedClientReq := &pbft.SignedClientRequest{
		ClientRequest: clientReq,
		Signature:     signature,
	}

	return signedClientReq
}