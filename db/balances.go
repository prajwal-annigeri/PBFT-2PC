package db

import (
	"2pcbyz-prajwal/pbft"
	"2pcbyz-prajwal/config"
	"fmt"
	"log"
	"strconv"

	bolt "go.etcd.io/bbolt"
)

func (d *Database) GetBalance(client string) (int64, error) {
	var balance []byte
	d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(balancesBucket))
		balance = b.Get([]byte(client))
		return nil
	})
	if balance == nil {
		return 0, fmt.Errorf("GetBalance(): %s is not in this shard", client)
	}
	balanceInt, _ := strconv.ParseInt(string(balance), 10, 64)
	return balanceInt, nil
}

func (d *Database) PutBalance(client string, balance int64) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(balancesBucket)
		return b.Put([]byte(client), []byte(strconv.FormatInt(balance, 10)))
	})
}

func (d *Database) UpdateBalances(transaction *pbft.Transaction) error {
	balanceFrom, err := d.GetBalance(transaction.From)
	if err != nil {
		log.Printf("%s\n", err.Error())
	} else {
		d.PutBalance(transaction.From, balanceFrom-transaction.Amount)
	}

	balanceTo, err := d.GetBalance(transaction.To)
	if err != nil {
		log.Printf("%s\n", err.Error())
	} else {
		d.PutBalance(transaction.To, balanceTo+transaction.Amount)
	}
	return nil
}

func (d *Database) InitiateBalances(startId, endId string) error {

	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(balancesBucket)
		startIdInt, _ := strconv.ParseInt(startId, 10, 64)
		endIdInt, _ := strconv.ParseInt(endId, 10, 64)
		for i := startIdInt; i <= endIdInt; i++ {
			err := b.Put([]byte(strconv.FormatInt(i, 10)), []byte(strconv.FormatInt(config.InitialBalance, 10)))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *Database) PrintAllBalances() {
	d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(balancesBucket)

		b.ForEach(func(k, v []byte) error {
			log.Printf("%s: %s\n", string(k), string(v))
			return nil
		})

		return nil
	})
}